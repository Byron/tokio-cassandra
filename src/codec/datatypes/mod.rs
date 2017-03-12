use codec::primitives::{CqlFrom, CqlBytes};
use tokio_core::io::EasyBuf;

pub mod encode;
pub mod decode;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Ascii {
    bytes: EasyBuf,
}

#[cfg(test)]
mod test_encode_decode {
    use super::*;

    #[test]
    fn ascii() {
        let to_encode = Ascii { bytes: vec![0x00, 0x23].into() };
        let encoded = encode::ascii(to_encode.clone());
        let decoded = decode::ascii(encoded.buffer().unwrap());
        assert_eq!(to_encode, decoded.unwrap());
    }

    #[test]
    fn ascii_failing() {
        let to_encode = Ascii { bytes: vec![0x00, 0x80].into() };
        let encoded = encode::ascii(to_encode.clone());
        let decoded = decode::ascii(encoded.buffer().unwrap());
        assert!(decoded.is_err());
    }
}
