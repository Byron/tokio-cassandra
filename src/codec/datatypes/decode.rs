use tokio_core::io::EasyBuf;
use codec::primitives::CqlBytes;

use super::*;

error_chain!{
    errors {
        InvalidAscii
    }
}

pub fn ascii(data: EasyBuf) -> Result<Ascii> {
    for b in data.as_slice() {
        if *b > 127 as u8 {
            return Err(ErrorKind::InvalidAscii.into());
        }
    }

    Ok(Ascii { bytes: data })
}
