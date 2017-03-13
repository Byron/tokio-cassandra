
pub mod encode;
pub mod decode;

pub trait Buffer: AsRef<[u8]> {}
impl<T: AsRef<[u8]>> Buffer for T {}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Ascii<T>
    where T: Buffer
{
    bytes: T,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Bigint {
    inner: i64,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Blob {}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Boolean {}

#[cfg(test)]
mod test_encode_decode {
    use super::*;

    #[test]
    fn ascii() {
        let to_encode = Ascii { bytes: vec![0x00, 0x23].into() };

        let mut encoded = Vec::new();
        encode::ascii(to_encode.clone(), &mut encoded);

        let decoded = decode::ascii(encoded.into());
        assert_eq!(to_encode, decoded.unwrap());
    }

    #[test]
    fn ascii_failing() {
        let to_encode = Ascii { bytes: vec![0x00, 0x80] };
        let mut encoded = Vec::new();
        encode::ascii(to_encode.clone(), &mut encoded);
        let decoded = decode::ascii(encoded.into());
        assert!(decoded.is_err());
    }
}
