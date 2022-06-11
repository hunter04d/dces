use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam::channel;

use crate::cli::HostAddr;
use crate::msg::types::{LogValue, Node};
use crate::msg::{ConnectReqMsg, KeepAliveMsg, Msg, SyncReqMsg};
use crate::proto::{ProtoReader, ProtoWriter};
use crate::replication_log::ReplicationLog;

pub struct Client {
    shutdown: Vec<channel::Sender<()>>,
    join_handles: Vec<JoinHandle<()>>,
    action_sender: Arc<channel::Sender<ClientAction>>,
    this_node: Node,
    nodes: Arc<Mutex<Vec<Node>>>,
    server_unavailable: Arc<Mutex<bool>>,
    log: Arc<Mutex<ReplicationLog>>,
}

#[derive(Debug)]
pub enum ClientAction {
    KeepAlive,
    SyncRequest(u64),
}

#[derive(Debug)]
pub enum ClientRebuild {
    None,
    Server,
    Client(HostAddr),
}

pub trait ClientStartOptions {
    fn server_addr(&self) -> HostAddr;

    fn node_name(&self) -> String;

    fn client_rebuilding_addr(&self) -> HostAddr;
}

impl Client {
    pub fn start(config: &impl ClientStartOptions, log: Option<ReplicationLog>) -> Self {
        let socket = loop {
            match TcpStream::connect(&config.server_addr()) {
                Ok(stream) => {
                    break stream;
                }
                Err(e) => {
                    println!("Error connecting to primary node {:?}", e);
                }
            }
        };
        let mut writer = ProtoWriter::new(&socket);
        let mut reader = ProtoReader::new(&socket);

        let req = ConnectReqMsg {
            node_name: config.node_name(),
            rebuilding_addr: config.client_rebuilding_addr().to_string(),
        };

        writer.write(&Msg::ConnectReq(req)).unwrap();

        let (initial_nodes, initial_log) = match reader.read::<Msg>().unwrap() {
            Msg::ConnectResp(resp) => {
                println!(
                    "Connection successful\n. Current nodes: {:?}\nCurrent Log len: {}",
                    resp.nodes, resp.current_log
                );
                (
                    resp.nodes,
                    ReplicationLog::populated_with_empty(resp.current_log as usize),
                )
            }
            _ => {
                panic!("Invalid message")
            }
        };

        let nodes = Arc::new(Mutex::new(initial_nodes));
        let log = Arc::new(Mutex::new(log.unwrap_or(initial_log)));

        let server_unavailable = Arc::new(Mutex::new(false));

        let (action_sender, action_recv) = channel::bounded(16);

        let action_sender = Arc::new(action_sender);
        let action_sender_keep_alive = Arc::clone(&action_sender);

        let (keep_alive_sender, keep_alive_recv) = channel::bounded(1);
        let keep_alive_join_handle = thread::spawn(move || loop {
            match keep_alive_recv.try_recv() {
                Ok(_) | Err(channel::TryRecvError::Disconnected) => {
                    break;
                }
                Err(channel::TryRecvError::Empty) => {}
            }
            thread::sleep(Duration::from_secs(2));
            match action_sender_keep_alive.send(ClientAction::KeepAlive) {
                Ok(()) => {}
                Err(_) => {
                    println!("Server failed. No longer sending keep alive. rebuild imminent");
                    break;
                }
            }
        });

        let reader_socket = Arc::new(socket);
        let writer_socket = Arc::clone(&reader_socket);

        let (reader_sender, reader_recv) = channel::bounded(1);
        let reader_nodes = Arc::downgrade(&nodes);

        let log_ref = Arc::downgrade(&log);
        let read_join_handle = thread::spawn(move || {
            let mut reader = ProtoReader::new(&*reader_socket);
            loop {
                match reader_recv.try_recv() {
                    Ok(_) | Err(channel::TryRecvError::Disconnected) => {
                        break;
                    }
                    Err(channel::TryRecvError::Empty) => {}
                }

                match reader.read::<Msg>() {
                    Err(e) => {
                        println!("Error reading {:?}", e);
                        break;
                    }
                    Ok(msg) => match msg {
                        Msg::ClusterPing(new_nodes) => {
                            match reader_nodes.upgrade() {
                                Some(a) => {
                                    let mut lock = a.lock().unwrap();
                                    *lock = new_nodes.nodes;
                                }
                                None => {}
                            }
                            // println!("Ping recv");
                        }
                        Msg::Sync(sync) => {
                            if let Some(a) = log_ref.upgrade() {
                                let mut lock = a.lock().unwrap();
                                lock.set(sync.index as usize, LogValue::empty());
                            }
                        }
                        _ => {
                            panic!("Invalid msg: {:?}", msg.kind())
                        }
                    },
                }
            }
        });

        let server_unavailable_write = Arc::clone(&server_unavailable);
        let write_join_handle = thread::spawn(move || {
            let mut writer = ProtoWriter::new(&*writer_socket);
            loop {
                match action_recv.recv() {
                    Err(_) => {
                        break;
                    }
                    Ok(action) => match action {
                        ClientAction::KeepAlive => {
                            match writer.write(&Msg::KeepAlive(KeepAliveMsg)) {
                                Ok(_) => {
                                    // println!("Keep alive sent");
                                }
                                Err(_) => {
                                    println!("Keep alive error, starting cluster rebuild");
                                    let mut lock = server_unavailable_write.lock().unwrap();
                                    *lock = true;
                                    break;
                                }
                            }
                        }
                        ClientAction::SyncRequest(log_index) => {
                            match writer.write(&Msg::SyncReq(SyncReqMsg(log_index))) {
                                Ok(_) => {}
                                Err(_) => {
                                    println!("Keep alive error, starting cluster rebuild");
                                    let mut lock = server_unavailable_write.lock().unwrap();
                                    *lock = true;
                                    break;
                                }
                            }
                        }
                    },
                }
            }
        });

        Client {
            shutdown: vec![keep_alive_sender, reader_sender],
            join_handles: vec![read_join_handle, write_join_handle, keep_alive_join_handle],
            action_sender,
            nodes,
            this_node: Node {
                name: config.node_name(),
                rebuilding_addr: config.client_rebuilding_addr(),
            },
            log,
            server_unavailable,
        }
    }

    pub fn this_node(&self) -> Node {
        self.this_node.clone()
    }

    pub fn should_rebuild(&self) -> ClientRebuild {
        let server_unavailable = {
            let lock = self.server_unavailable.lock().unwrap();
            *lock
        };
        if !server_unavailable {
            ClientRebuild::None
        } else {
            let lock = self.nodes.lock().unwrap();
            match lock.first() {
                Some(n) if n.name == self.this_node.name => ClientRebuild::Server,
                Some(n) => ClientRebuild::Client(n.rebuilding_addr.clone()),
                None => ClientRebuild::Server,
            }
        }
    }

    pub fn get_log_value(&self, index: u64) -> Option<LogValue> {
        let lock = self.log.lock().unwrap();
        lock.get(index as usize).cloned()
    }

    pub fn request_log_value(&self, index: u64) {
        let _ = self.action_sender.send(ClientAction::SyncRequest(index));
    }

    #[allow(dead_code)]
    pub fn shutdown(self) -> ReplicationLog {
        drop(self.action_sender);
        for s in self.shutdown {
            let _ = s.send(());
        }
        for h in self.join_handles {
            let _ = h.join();
        }
        let log_mutex =
            Arc::try_unwrap(self.log).expect("Only one strong reference to Replication log exists");
        log_mutex.into_inner().expect("Lock is not poisoned")
    }
}
