use codec::primitives::CqlBytes;
use tokio_core::io::EasyBuf;

use super::*;

pub fn ascii(data: Ascii) -> CqlBytes<EasyBuf> {
    CqlBytes::from(data.bytes)
}
