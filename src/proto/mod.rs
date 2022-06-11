#![allow(dead_code)]

use std::fmt::Arguments;
use std::io::{self, Read, Result, Write};
use std::mem;

pub mod model;

pub struct ProtoWriter<TWrite: Write> {
    writer: TWrite,
}

pub struct ProtoReader<TRead: Read> {
    reader: TRead,
}

pub trait ProtoType {
    fn read<T: Read>(reader: &mut ProtoReader<T>) -> Result<Self>
    where
        Self: Sized;
    fn write<T: Write>(&self, writer: &mut ProtoWriter<T>) -> Result<()>
    where
        Self: Sized;
}

pub trait ProtoAsProxy {
    type AsType;

    fn read<TRead: Read>(reader: &mut ProtoReader<TRead>) -> Result<Self::AsType>;

    fn write<TWrite: Write>(writer: &mut ProtoWriter<TWrite>, data: &Self::AsType) -> Result<()>;
}

impl<TRead: Read> ProtoReader<TRead> {
    pub fn new(reader: TRead) -> Self {
        Self { reader }
    }

    pub fn read_into_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.reader.read_exact(buf)
    }

    pub fn read_size4(&mut self) -> Result<u32> {
        let mut buf = [0u8; mem::size_of::<u32>()];
        self.reader.read_exact(&mut buf)?;
        Ok(u32::from_be_bytes(buf))
    }

    pub fn read_size8(&mut self) -> Result<u64> {
        let mut buf = [0u8; mem::size_of::<u64>()];
        self.reader.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }

    pub fn read_byte(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    pub fn read_i32(&mut self) -> Result<i32> {
        let mut buf = [0u8; mem::size_of::<i32>()];
        self.reader.read_exact(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }

    pub fn read_size4_string(&mut self) -> Result<String> {
        let size = self.read_size4()?;
        let mut buf = vec![0; size as usize];
        self.reader.read_exact(&mut buf)?;
        String::from_utf8(buf)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "String was not valid"))
    }

    pub fn read_size4_bytes(&mut self) -> Result<Vec<u8>> {
        let size = self.read_size4()?;
        let mut buf = vec![0; size as usize];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn read<T: ProtoType>(&mut self) -> Result<T> {
        T::read(self)
    }

    pub fn read_as<T: ProtoAsProxy>(&mut self) -> Result<T::AsType> {
        T::read(self)
    }
}

impl<TWrite: Write> ProtoWriter<TWrite> {
    pub fn new(writer: TWrite) -> Self {
        Self { writer }
    }

    pub fn write_from_all(&mut self, buf: &[u8]) -> Result<()> {
        self.writer.write_all(buf)
    }

    pub fn write_fmt(&mut self, fmt: Arguments<'_>) -> Result<()> {
        self.writer.write_fmt(fmt)
    }

    pub fn write_byte(&mut self, data: u8) -> Result<()> {
        let buf = [data];
        self.writer.write_all(&buf)
    }

    pub fn write_size4(&mut self, data: u32) -> Result<()> {
        let buf = data.to_be_bytes();
        self.writer.write_all(&buf)
    }

    pub fn write_size8(&mut self, data: u64) -> Result<()> {
        let buf = data.to_be_bytes();
        self.writer.write_all(&buf)
    }

    pub fn write_i32(&mut self, data: i32) -> Result<()> {
        let buf = data.to_be_bytes();
        self.writer.write_all(&buf)
    }

    pub fn write_size4_string(&mut self, data: &str) -> Result<()> {
        let len = data.len();
        if len > (u32::MAX as usize) {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "String was too large for the protocol",
            ))
        } else {
            self.write_size4(len as u32)?;
            let buf = data.as_bytes();
            self.writer.write_all(buf)?;
            Ok(())
        }
    }

    pub fn write_size4_bytes(&mut self, data: &[u8]) -> Result<()> {
        let len = data.len();
        if len > (u32::MAX as usize) {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Bytes were too large for the protocol",
            ))
        } else {
            self.write_size4(len as u32)?;
            self.writer.write_all(data)?;
            Ok(())
        }
    }

    pub fn write<T: ProtoType>(&mut self, data: &T) -> Result<()> {
        data.write(self)
    }

    pub fn write_as<T: ProtoAsProxy>(&mut self, data: &T::AsType) -> Result<()> {
        T::write(self, data)
    }
}
