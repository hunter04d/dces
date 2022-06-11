use std::{
    io::{self, Read, Result, Write},
    marker::PhantomData,
};

use super::{ProtoAsProxy, ProtoReader, ProtoType, ProtoWriter};

pub struct Size4Vec<T> {
    data: PhantomData<*const T>,
}

impl<T: ProtoType> ProtoAsProxy for Size4Vec<T> {
    type AsType = Vec<T>;

    fn read<TRead: Read>(reader: &mut ProtoReader<TRead>) -> Result<Self::AsType> {
        let size = reader.read_size4()?;
        let mut result = Vec::with_capacity(size as usize);
        for _ in 0..size {
            result.push(<T as ProtoType>::read(reader)?);
        }
        Ok(result)
    }

    fn write<TWrite: Write>(writer: &mut ProtoWriter<TWrite>, data: &Self::AsType) -> Result<()> {
        let len = data.len();
        if len > (u32::MAX as usize) {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Vec(T) was too large for the protocol",
            ))
        } else {
            writer.write_size4(len as u32)?;
            for item in data.iter() {
                <T as ProtoType>::write(item, writer)?;
            }
            Ok(())
        }
    }
}
