use tokio_core::io::EasyBuf;
use byteorder::{ByteOrder, BigEndian};

use super::*;

error_chain!{
    errors {
        InvalidAscii
        Incomplete
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

pub fn bigint(data: EasyBuf) -> Result<Bigint> {
    if data.len() != 8 {
        return Err(ErrorKind::Incomplete.into());
    }
    let long = BigEndian::read_i64(data.as_slice());
    Ok(Bigint { inner: long })
}
