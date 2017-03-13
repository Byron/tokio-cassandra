use tokio_core::io::EasyBuf;

pub mod encode;
pub mod decode;

pub trait Buffer: AsRef<[u8]> {}
impl<T: AsRef<[u8]>> Buffer for T {}

type InputBuffer = Vec<u8>;

pub trait CqlSerializable
    where Self: Sized
{
    fn deserialize(data: EasyBuf) -> decode::Result<Self>;
    fn serialize(self, &mut InputBuffer);
}

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
pub struct Blob<T>
    where T: Buffer
{
    bytes: T,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Boolean {
    inner: bool,
}

impl CqlSerializable for Boolean {
    fn serialize(self, buf: &mut InputBuffer) {
        buf.extend(&::codec::primitives::encode::int(1)[..]); // TODO: better encode bytes
        encode::boolean(self, buf)
    }

    fn deserialize(data: EasyBuf) -> decode::Result<Self> {
        decode::boolean(data)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct List<T: CqlSerializable> {
    inner: Vec<T>,
}

#[cfg(test)]
mod test_encode_decode {
    use super::*;
    use tokio_core::io::EasyBuf;

    fn assert_decode_encode<T>(to_encode: T,
                               encfn: fn(T, &mut Vec<u8>),
                               decfn: fn(EasyBuf) -> Result<T, decode::Error>)
        where T: Clone + PartialEq + Eq + ::std::fmt::Debug
    {

        let mut encoded = Vec::new();
        encfn(to_encode.clone(), &mut encoded);

        let decoded = decfn(encoded.into());
        assert_eq!(to_encode, decoded.unwrap());
    }

    #[test]
    fn ascii() {
        let to_encode = Ascii { bytes: vec![0x00, 0x23].into() };
        assert_decode_encode(to_encode, encode::ascii, decode::ascii);
    }

    #[test]
    fn ascii_failing() {
        let to_encode = Ascii { bytes: vec![0x00, 0x80] };
        let mut encoded = Vec::new();
        encode::ascii(to_encode.clone(), &mut encoded);
        let decoded = decode::ascii(encoded.into());
        assert!(decoded.is_err());
    }

    #[test]
    fn bigint() {
        let to_encode = Bigint { inner: -123456789 };
        assert_decode_encode(to_encode, encode::bigint, decode::bigint);
    }

    #[test]
    fn blob() {
        let to_encode = Blob { bytes: vec![0x00, 0x81].into() };
        assert_decode_encode(to_encode, encode::blob, decode::blob);
    }

    #[test]
    fn boolean() {
        let to_encode = Boolean { inner: false };
        assert_decode_encode(to_encode, encode::boolean, decode::boolean);
    }

    #[test]
    fn list() {
        let to_encode =
            List { inner: vec![Boolean { inner: false }, Boolean { inner: true }, Boolean { inner: false }] };
        assert_decode_encode(to_encode, encode::list, decode::list);
    }
}
