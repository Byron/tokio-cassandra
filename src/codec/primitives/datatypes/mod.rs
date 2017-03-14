use tokio_core::io::EasyBuf;
use byteorder::{ByteOrder, BigEndian};
use codec::primitives::CqlBytes;

type Buffer = EasyBuf;
type InputBuffer = Vec<u8>;
type BytesLen = i32;

error_chain!{
    errors {
        InvalidAscii
        Incomplete
    }

    foreign_links {
        DecodeErr(::codec::primitives::decode::Error);
    }
}

pub trait CqlSerializable
    where Self: Sized
{
    fn deserialize(data: EasyBuf) -> Result<Self>;
    fn serialize(&self, &mut InputBuffer);
    fn bytes_len(&self) -> BytesLen;
}

// Bounds checking needs to be done in constructor
#[derive(Debug, PartialEq, Eq, Clone)]
struct Ascii {
    bytes: Buffer,
}

impl CqlSerializable for Ascii {
    fn serialize(&self, buf: &mut InputBuffer) {
        buf.extend(self.bytes.as_ref());
    }

    fn deserialize(data: EasyBuf) -> Result<Self> {
        for b in data.as_slice() {
            if *b > 127 as u8 {
                return Err(ErrorKind::InvalidAscii.into());
            }
        }

        Ok(Ascii { bytes: data })
    }

    fn bytes_len(&self) -> BytesLen {
        self.bytes.len() as BytesLen
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Bigint {
    inner: i64,
}

impl CqlSerializable for Bigint {
    fn serialize(&self, buf: &mut InputBuffer) {
        let mut bytes = [0u8; 8];
        BigEndian::write_i64(&mut bytes[..], self.inner);
        buf.extend(&bytes[..]);
    }

    fn deserialize(data: EasyBuf) -> Result<Self> {
        if data.len() != 8 {
            return Err(ErrorKind::Incomplete.into());
        }
        let long = BigEndian::read_i64(data.as_slice());
        Ok(Bigint { inner: long })
    }

    fn bytes_len(&self) -> BytesLen {
        8
    }
}

// Bounds checking needs to be done in constructor
#[derive(Debug, PartialEq, Eq, Clone)]
struct Blob {
    bytes: Buffer,
}

impl CqlSerializable for Blob {
    fn serialize(&self, buf: &mut InputBuffer) {
        buf.extend(self.bytes.as_ref());
    }

    fn deserialize(data: EasyBuf) -> Result<Self> {
        Ok(Blob { bytes: data })
    }

    fn bytes_len(&self) -> BytesLen {
        self.bytes.len() as BytesLen
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Boolean {
    inner: bool,
}

impl CqlSerializable for Boolean {
    fn serialize(&self, buf: &mut InputBuffer) {
        if self.inner {
            buf.push(0x01);
        } else {
            buf.push(0x00);
        }
    }

    fn deserialize(data: EasyBuf) -> Result<Self> {
        if data.len() != 1 {
            return Err(ErrorKind::Incomplete.into());
        }

        let b = data.as_slice()[0];
        Ok(Boolean { inner: b != 0x00 })
    }

    fn bytes_len(&self) -> BytesLen {
        1
    }
}

// Bounds checking needs to be done in constructor
#[derive(Debug, PartialEq, Eq, Clone)]
struct List<T: CqlSerializable> {
    inner: Vec<Option<T>>,
}

impl<T: CqlSerializable> CqlSerializable for List<T> {
    fn serialize(&self, buf: &mut InputBuffer) {
        buf.extend(&::codec::primitives::encode::int(self.inner.len() as BytesLen)[..]);
        for item in &self.inner {
            match item {
                &Some(ref item) => {
                    buf.extend(&::codec::primitives::encode::int(item.bytes_len())[..]);
                    item.serialize(buf);
                }
                &None => ::codec::primitives::encode::bytes(&CqlBytes::<Vec<u8>>::null_value(), buf),
            }
        }
    }

    fn deserialize(data: EasyBuf) -> Result<Self> {
        let (data, n) = ::codec::primitives::decode::int(data)?;
        let mut v = Vec::new();

        let mut d = data;
        for _ in 0..n {
            let (data, bytes) = ::codec::primitives::decode::bytes(d)?;
            v.push(match bytes.buffer() {
                       Some(b) => Some(T::deserialize(b)?),
                       None => None,
                   });
            d = data
        }

        Ok(List { inner: v })
    }

    fn bytes_len(&self) -> BytesLen {
        self.inner.len() as BytesLen
    }
}

#[cfg(test)]
mod test_encode_decode {
    use super::*;

    fn assert_serde<T>(to_encode: T)
        where T: Clone + PartialEq + Eq + ::std::fmt::Debug + CqlSerializable
    {
        let mut encoded = Vec::new();
        to_encode.clone().serialize(&mut encoded);

        let decoded = T::deserialize(encoded.into());
        assert_eq!(to_encode, decoded.unwrap());
    }

    #[test]
    fn ascii() {
        let to_encode = Ascii { bytes: vec![0x00, 0x23].into() };
        assert_serde(to_encode);
    }

    #[test]
    fn ascii_failing() {
        let to_encode = Ascii { bytes: vec![0x00, 0x80].into() };
        let mut encoded = Vec::new();
        to_encode.clone().serialize(&mut encoded);
        let decoded = Ascii::deserialize(encoded.into());
        assert!(decoded.is_err());
    }

    #[test]
    fn bigint() {
        let to_encode = Bigint { inner: -123456789 };
        assert_serde(to_encode);
    }

    #[test]
    fn blob() {
        let to_encode = Blob { bytes: vec![0x00, 0x81].into() };
        assert_serde(to_encode);
    }

    #[test]
    fn boolean() {
        let to_encode = Boolean { inner: false };
        assert_serde(to_encode);
    }

    #[test]
    fn list() {
        let to_encode = List { inner: vec![Some(Boolean { inner: false }), Some(Boolean { inner: true }), None] };
        assert_serde(to_encode);
    }
}
