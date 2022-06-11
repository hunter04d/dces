#![allow(dead_code)]

use std::io;
use std::io::{Read, Result, Write};

use crate::proto::{model::Size4Vec, ProtoReader, ProtoType, ProtoWriter};

use self::types::Node;

pub mod types;

#[derive(Debug)]
pub struct ConnectReqMsg {
    pub node_name: String,
    pub rebuilding_addr: String,
}

#[derive(Debug)]
pub struct ConnectRespMsg {
    pub nodes: Vec<Node>,
    pub current_log: u64,
}

#[derive(Debug)]
pub struct ClusterPingMsg {
    pub nodes: Vec<Node>,
}

#[derive(Debug)]
pub struct KeepAliveMsg;

#[derive(Debug)]
pub struct SyncMsg {
    pub index: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct SyncReqMsg(pub u64);

#[non_exhaustive]
pub enum Msg {
    ConnectReq(ConnectReqMsg),
    ConnectResp(ConnectRespMsg),
    ClusterPing(ClusterPingMsg),
    KeepAlive(KeepAliveMsg),
    Sync(SyncMsg),
    SyncReq(SyncReqMsg),
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum MsgKind {
    ConnectReq = 0x01,
    ConnectResp = 0x02,
    ClusterPing = 0x03,
    KeepAlive = 0x04,
    Sync = 0x05,
    SyncReq = 0x06,
}

impl MsgKind {
    pub fn from_u8(data: u8) -> Option<Self> {
        match data {
            0x01 => Some(MsgKind::ConnectReq),
            0x02 => Some(MsgKind::ConnectResp),
            0x03 => Some(MsgKind::ClusterPing),
            0x04 => Some(MsgKind::KeepAlive),
            0x05 => Some(MsgKind::Sync),
            0x06 => Some(MsgKind::SyncReq),
            _ => None,
        }
    }
}

impl Msg {
    pub fn kind(&self) -> MsgKind {
        match self {
            Msg::ConnectReq(_) => MsgKind::ConnectReq,
            Msg::ConnectResp(_) => MsgKind::ConnectResp,
            Msg::ClusterPing(_) => MsgKind::ClusterPing,
            Msg::KeepAlive(_) => MsgKind::KeepAlive,
            Msg::Sync(_) => MsgKind::Sync,
            Msg::SyncReq(_) => MsgKind::SyncReq,
        }
    }
}

impl ProtoType for Msg {
    fn read<T: Read>(reader: &mut ProtoReader<T>) -> Result<Self>
    where
        Self: Sized,
    {
        let id = reader.read_byte()?;
        let kind = MsgKind::from_u8(id);
        if let Some(kind) = kind {
            let msg = match kind {
                MsgKind::ConnectReq => {
                    let node_name = reader.read_size4_string()?;
                    let rebuilding_addr = reader.read_size4_string()?;
                    Msg::ConnectReq(ConnectReqMsg {
                        node_name,
                        rebuilding_addr,
                    })
                }
                MsgKind::ConnectResp => Msg::ConnectResp(ConnectRespMsg {
                    nodes: reader.read_as::<Size4Vec<Node>>()?,
                    current_log: reader.read_size8()?,
                }),
                MsgKind::ClusterPing => Msg::ClusterPing(ClusterPingMsg {
                    nodes: reader.read_as::<Size4Vec<Node>>()?,
                }),
                MsgKind::KeepAlive => Msg::KeepAlive(KeepAliveMsg),
                MsgKind::Sync => Msg::Sync(SyncMsg {
                    index: reader.read_size8()?,
                }),
                MsgKind::SyncReq => Msg::SyncReq(SyncReqMsg(reader.read_size8()?)),
            };
            Ok(msg)
        } else {
            let e = io::Error::new(io::ErrorKind::InvalidData, "Invalid message");
            Err(e)
        }
    }

    fn write<T: Write>(&self, writer: &mut ProtoWriter<T>) -> Result<()>
    where
        Self: Sized,
    {
        let id = self.kind() as u8;
        writer.write_byte(id)?;
        match self {
            Msg::ConnectReq(req) => {
                writer.write_size4_string(&req.node_name)?;
                writer.write_size4_string(&req.rebuilding_addr)?;
            }
            Msg::ConnectResp(resp) => {
                writer.write_as::<Size4Vec<_>>(&resp.nodes)?;
                writer.write_size8(resp.current_log)?;
            }
            Msg::ClusterPing(ping) => {
                writer.write_as::<Size4Vec<_>>(&ping.nodes)?;
            }
            Msg::KeepAlive(_) => {}
            Msg::Sync(sync) => {
                writer.write_size8(sync.index)?;
            }
            Msg::SyncReq(sync_req) => {
                writer.write_size8(sync_req.0)?;
            }
        }
        Ok(())
    }
}
