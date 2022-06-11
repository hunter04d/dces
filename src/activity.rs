use std::thread::sleep;
use std::time::Duration;
use wasmtime::Caller;

use crate::cli::HostAddr;
use crate::client::{ClientRebuild, ClientStartOptions};
use crate::msg::types::Node;
use crate::{Client, Server};

pub struct Activity {
    log_index: u64,
    com: Communicator,
}

pub enum Communicator {
    None,
    Client(Client),
    Server(Server),
}

struct ClientRestart {
    this_node: Node,
    new_server: HostAddr,
}

impl Activity {
    pub fn new(com: Communicator) -> Self {
        Self { log_index: 0, com }
    }

    pub fn execute_or_sync(mut this: Caller<'_, Activity>, f: impl FnOnce()) {
        let activity = this.data_mut();
        loop {
            let rebuild;
            match &mut activity.com {
                Communicator::Client(client) => {
                    rebuild = client.should_rebuild();
                    if let ClientRebuild::None = rebuild {
                        match client.get_log_value(activity.log_index) {
                            None => {
                                client.request_log_value(activity.log_index);
                                sleep(Duration::from_secs(2));
                            }
                            Some(_value) => {
                                break;
                            }
                        };
                    }
                }
                Communicator::Server(server) => {
                    if let Some(_value) = server.get_log_value(activity.log_index) {
                    } else {
                        f();
                        server.report_result(activity.log_index);
                    }
                    break;
                }
                Communicator::None => {
                    unreachable!("Communicator should be set!!!")
                }
            }

            match rebuild {
                ClientRebuild::None => {}
                ClientRebuild::Server => {
                    let old_com = std::mem::replace(&mut activity.com, Communicator::None);
                    let (node, log) = match old_com {
                        Communicator::Client(c) => {
                            let node = c.this_node();
                            println!("setting node as primary: \"{}\"", node.name);
                            let log = c.shutdown();
                            (node, log)
                        }
                        Communicator::Server(_) | Communicator::None => unreachable!(),
                    };
                    let server = Server::start(&node, log);
                    activity.com = Communicator::Server(server);
                }
                ClientRebuild::Client(new_host) => {
                    println!("Repointing client to new host: \"{}\"", new_host);
                    let old_com = std::mem::replace(&mut activity.com, Communicator::None);
                    let (node, log) = match old_com {
                        Communicator::Client(c) => {
                            let node = c.this_node();
                            let log = c.shutdown();
                            (node, log)
                        }
                        Communicator::Server(_) | Communicator::None => unreachable!(),
                    };
                    let client = Client::start(
                        &ClientRestart {
                            this_node: node,
                            new_server: new_host,
                        },
                        Some(log),
                    );
                    activity.com = Communicator::Client(client);
                }
            }
        }
        activity.log_index += 1;
    }
}

impl ClientStartOptions for ClientRestart {
    fn server_addr(&self) -> HostAddr {
        self.new_server.clone()
    }

    fn node_name(&self) -> String {
        self.this_node.name.clone()
    }

    fn client_rebuilding_addr(&self) -> HostAddr {
        self.this_node.rebuilding_addr.clone()
    }
}
