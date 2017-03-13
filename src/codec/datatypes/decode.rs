use tokio_core::io::EasyBuf;

use super::*;

error_chain!{
    errors {
        InvalidAscii
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
