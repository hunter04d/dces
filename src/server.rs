use std::net::{TcpListener, TcpStream};
use std::{io, thread};

use std::sync::{Arc, Mutex, Weak};
use std::thread::sleep;
use std::time::Duration;

use crossbeam::channel;

use crate::cli::HostAddr;
use crate::msg::types::Node;
use crate::msg::{ClusterPingMsg, ConnectRespMsg, Msg, SyncMsg};
use crate::proto::{ProtoReader, ProtoWriter};
use crate::replication_log::ReplicationLog;
use crate::LogValue;

#[derive(Debug)]
pub enum ServerAction {
    Ping(Vec<Node>),
    SyncResp { index: u64 },
}

pub trait ServerStartOptions {
    fn addr(&self) -> HostAddr;
    fn node_name(&self) -> String;
}

pub struct Server {
    main_send: channel::Sender<()>,
    ping_send: channel::Sender<()>,
    join_handles: Vec<thread::JoinHandle<()>>,
    active_nodes: Arc<Mutex<Vec<ActiveClientNode>>>,
    log: Arc<Mutex<ReplicationLog>>,
}

struct ActiveClientNode {
    node: Node,
    sender: channel::Sender<ServerAction>,
}

struct NewClient {
    socket: TcpStream,
    nodes: Arc<Mutex<Vec<ActiveClientNode>>>,
    log_ref: Weak<Mutex<ReplicationLog>>,
}

impl Server {
    pub fn start(config: &impl ServerStartOptions, log: ReplicationLog) -> Server {
        let (main_send, main_recv) = channel::bounded(1);

        let addr = config.addr();
        let nodes = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::new(Mutex::new(log));

        let send_nodes = Arc::clone(&nodes);

        let accept_log = Arc::clone(&log);
        let handle = thread::spawn(move || {
            let listener = TcpListener::bind(&addr).unwrap();
            listener
                .set_nonblocking(true)
                .expect("Cannot set non-blocking");

            loop {
                match main_recv.try_recv() {
                    Ok(_) | Err(channel::TryRecvError::Disconnected) => {
                        println!("Terminating.");
                        break;
                    }
                    Err(channel::TryRecvError::Empty) => {}
                }

                match listener.accept() {
                    Ok((socket, _)) => {
                        let new = NewClient {
                            nodes: Arc::clone(&send_nodes),
                            socket,
                            log_ref: Arc::downgrade(&accept_log),
                        };
                        Self::handle_client(new).unwrap();
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        // wait until network socket is ready, typically implemented
                        // via platform-specific APIs such as epoll or IOCP
                        sleep(Duration::from_secs(5));
                        continue;
                    }
                    Err(_) => {
                        panic!("Issue with socket listener")
                    }
                }
            }
        });

        let (ping_send, ping_recv) = channel::bounded(1);
        let ping_nodes = Arc::clone(&nodes);
        let ping_handle = thread::spawn(move || loop {
            match ping_recv.try_recv() {
                Ok(_) | Err(channel::TryRecvError::Disconnected) => {
                    println!("Terminating ping thread");
                    break;
                }
                Err(channel::TryRecvError::Empty) => {}
            }
            sleep(Duration::from_secs(2));

            {
                let lock = ping_nodes.lock().unwrap();
                let current_nodes: Vec<_> = lock.iter().map(|a| a.node.clone()).collect();

                for n in lock.iter() {
                    n.sender
                        .send(ServerAction::Ping(current_nodes.clone()))
                        .unwrap();
                }

                // println!("Ping send");
            }
        });

        Server {
            main_send,
            ping_send,
            join_handles: vec![handle, ping_handle],
            active_nodes: nodes,
            log,
        }
    }

    pub fn report_result(&self, log_index: u64) {
        {
            let mut lock = self.log.lock().unwrap();
            lock.set(log_index as usize, LogValue::empty());
        }
        let lock = self.active_nodes.lock().unwrap();
        for el in lock.iter() {
            let _ = el.sender.send(ServerAction::SyncResp { index: log_index });
        }
    }

    pub fn get_log_value(&self, index: u64) -> Option<LogValue> {
        let lock = self.log.lock().unwrap();
        lock.get(index as usize).cloned()
    }

    fn handle_client(new: NewClient) -> io::Result<()> {
        let NewClient {
            socket,
            nodes: current_nodes,
            log_ref,
        } = new;

        socket.set_nonblocking(false).unwrap();
        let mut reader = ProtoReader::<_>::new(&socket);
        let first_msg = reader.read::<Msg>()?;

        let new_node = match first_msg {
            Msg::ConnectReq(req) => Node {
                name: req.node_name,
                rebuilding_addr: req.rebuilding_addr.parse().expect("Host address is valid"),
            },
            _ => {
                panic!("Invalid message")
            }
        };
        println!("Got Connect request for {:?}", new_node.name);
        let new_node_name_read = new_node.name.clone();
        let new_node_name_write = new_node.name.clone();

        let (node_sender, node_recv) = channel::bounded(16);
        let resp_nodes = {
            let mut lock = current_nodes.lock().unwrap();

            lock.push(ActiveClientNode {
                node: new_node,
                sender: node_sender.clone(),
            });
            lock.iter().map(|a| a.node.clone()).collect()
        };

        let log_count = log_ref
            .upgrade()
            .map(|a| a.lock().unwrap().len())
            .unwrap_or(0) as u64;
        let resp = ConnectRespMsg {
            nodes: resp_nodes,
            current_log: log_count,
        };

        let mut writer = ProtoWriter::new(&socket);

        writer.write(&Msg::ConnectResp(resp))?;

        let write_socket = Arc::new(socket);
        let read_socket = Arc::clone(&write_socket);
        let _read_thread = thread::spawn(move || {
            let mut reader = ProtoReader::<_>::new(&*read_socket);
            loop {
                match reader.read::<Msg>() {
                    Ok(msg) => match msg {
                        Msg::KeepAlive(_) => {
                            // println!("Keep alive from {:?}", new_node_name_read);
                        }
                        Msg::SyncReq(req) => {
                            let has_value = if let Some(a) = log_ref.upgrade() {
                                let lock = a.lock().unwrap();
                                lock.get(req.0 as usize).is_some()
                            } else {
                                false
                            };
                            if has_value {
                                node_sender
                                    .send(ServerAction::SyncResp { index: req.0 })
                                    .unwrap();
                            }
                        }
                        _ => {
                            panic!("Invalid msg {:?}", msg.kind());
                        }
                    },
                    Err(e) => {
                        println!("Error receiving msg: {:?}", e);
                        break;
                    }
                }
            }

            println!("Removing client connection for {:?}...", new_node_name_read);
            {
                let mut lock = current_nodes.lock().unwrap();
                lock.retain(|a| a.node.name != new_node_name_read);
            }
        });

        let _write_thread = thread::spawn(move || {
            let mut writer = ProtoWriter::new(&*write_socket);

            loop {
                match node_recv.recv() {
                    Err(_) => {
                        break;
                    }
                    Ok(action) => {
                        let write_result = match action {
                            ServerAction::Ping(nodes) => {
                                writer.write(&Msg::ClusterPing(ClusterPingMsg { nodes }))
                            }
                            ServerAction::SyncResp { index, .. } => {
                                writer.write(&Msg::Sync(SyncMsg { index }))
                            }
                        };
                        match write_result {
                            Ok(_) => {}
                            Err(e) => {
                                println!("Write error of {:?}: {:?}", new_node_name_write, e)
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    #[allow(dead_code)]
    pub fn shutdown(self) -> ReplicationLog {
        drop(self.active_nodes);
        self.main_send.send(()).unwrap();
        self.ping_send.send(()).unwrap();
        for x in self.join_handles {
            let _ = x.join();
        }

        let log_mutex =
            Arc::try_unwrap(self.log).expect("Only one strong reference to Replication log exists");
        log_mutex.into_inner().expect("Lock is not poisoned")
    }
}
