use tokio_core::io::EasyBuf;
use byteorder::{ByteOrder, BigEndian};

use super::*;

error_chain!{
    errors {
        InvalidAscii
        Incomplete
    }

    foreign_links {
        DecodeErr(::codec::primitives::decode::Error);
    }
}

type OutputBuffer = EasyBuf;

pub fn ascii(data: EasyBuf) -> Result<Ascii<OutputBuffer>> {
    for b in data.as_slice() {
        if *b > 127 as u8 {
            return Err(ErrorKind::InvalidAscii.into());
        }
    }

    Ok(Ascii { bytes: data })
}

pub fn blob(data: EasyBuf) -> Result<Blob<OutputBuffer>> {
    Ok(Blob { bytes: data })
}

pub fn bigint(data: EasyBuf) -> Result<Bigint> {
    if data.len() != 8 {
        return Err(ErrorKind::Incomplete.into());
    }
    let long = BigEndian::read_i64(data.as_slice());
    Ok(Bigint { inner: long })
}

pub fn boolean(data: EasyBuf) -> Result<Boolean> {
    if data.len() != 1 {
        return Err(ErrorKind::Incomplete.into());
    }

    let b = data.as_slice()[0];
    Ok(Boolean { inner: b != 0x00 })
}

pub fn list<T: CqlSerializable>(data: EasyBuf) -> Result<List<T>> {
    let (data, n) = ::codec::primitives::decode::int(data)?;
    let mut v = Vec::new();

    println!("data = {:?}", data);

    let mut d = data;
    for _ in 0..n {
        let (data, bytes) = ::codec::primitives::decode::bytes(d)?;
        v.push(T::deserialize(bytes.buffer().unwrap())?); // TODO: cover NULL values here
        d = data
    }

    Ok(List { inner: v })
}
