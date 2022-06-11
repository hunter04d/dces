use std::io::{self, ErrorKind, Read, Result, Write};

use crate::cli::HostAddr;

use crate::proto::{ProtoReader, ProtoType, ProtoWriter};
use crate::server::ServerStartOptions;

#[derive(Clone, Debug)]
pub struct Node {
    pub name: String,
    pub rebuilding_addr: HostAddr,
}

impl ProtoType for Node {
    fn read<T: Read>(reader: &mut ProtoReader<T>) -> Result<Self>
    where
        Self: Sized,
    {
        let name = reader.read_size4_string()?;
        let addr = reader
            .read_size4_string()?
            .parse::<HostAddr>()
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, Box::new(e)))?;
        Ok(Self {
            name,
            rebuilding_addr: addr,
        })
    }

    fn write<T: Write>(&self, writer: &mut ProtoWriter<T>) -> Result<()>
    where
        Self: Sized,
    {
        writer.write_size4_string(&self.name)?;
        let addr_string = format!("{}", self.rebuilding_addr);
        writer.write_size4_string(&addr_string)?;
        Ok(())
    }
}

impl ServerStartOptions for Node {
    fn addr(&self) -> HostAddr {
        self.rebuilding_addr.clone()
    }

    fn node_name(&self) -> String {
        self.name.clone()
    }
}

#[derive(Debug, Clone)]
pub enum ExternSym {
    Caller { name: String },
    Linker { module_name: String, name: String },
}

#[derive(Debug, Clone)]
pub enum Val {
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    V128(u128),
    FuncRef(ExternSym),
    ExternRef,
}

#[derive(Debug, Clone)]
pub struct MemoryChange {
    offset: u32,
    value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct TableChange {
    offset: u32,
    values: Vec<Val>,
}

#[derive(Debug, Clone)]
pub struct GlobalChange {
    new_value: Val,
}

#[derive(Debug, Clone)]
pub enum SymChange {
    Memory(MemoryChange),
    Table(TableChange),
    Global(GlobalChange),
}

#[derive(Debug, Clone)]
pub struct Change {
    symbol: ExternSym,
    new_value: SymChange,
}

#[derive(Debug, Clone)]
pub enum ExternalTrap {
    I32(i32),
    String(String),
}

#[derive(Debug, Clone)]
pub enum ExternalResult {
    Ok(Vec<Val>),
    Trap(ExternalTrap),
}

#[derive(Debug, Clone)]
pub struct LogValue {
    result: ExternalResult,
    changes: Vec<Change>,
}

impl LogValue {
    pub fn empty() -> LogValue {
        LogValue {
            result: ExternalResult::Ok(Vec::new()),
            changes: Vec::new(),
        }
    }
}

impl ProtoType for ExternSym {
    fn read<T: Read>(reader: &mut ProtoReader<T>) -> Result<Self>
    where
        Self: Sized,
    {
        let discriminant = reader.read_byte()?;
        match discriminant {
            0 => {
                let name = reader.read_size4_string()?;
                Ok(ExternSym::Caller { name })
            }
            _ => {
                let module_name = reader.read_size4_string()?;
                let name = reader.read_size4_string()?;
                Ok(ExternSym::Linker { module_name, name })
            }
        }
    }

    fn write<T: Write>(&self, writer: &mut ProtoWriter<T>) -> Result<()>
    where
        Self: Sized,
    {
        match self {
            ExternSym::Caller { name } => {
                writer.write_byte(0x00)?;
                writer.write_size4_string(name)?;
            }
            ExternSym::Linker { module_name, name } => {
                writer.write_byte(0x01)?;
                writer.write_size4_string(module_name)?;
                writer.write_size4_string(name)?;
            }
        }

        Ok(())
    }
}
