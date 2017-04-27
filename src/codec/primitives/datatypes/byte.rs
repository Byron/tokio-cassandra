use super::*;
use std::fmt::Debug;

// Bounds checking needs to be done in constructor
#[derive(PartialEq, Eq, Clone)]
pub struct Blob {
    inner: BytesMut,
}

impl TryFrom<Vec<u8>> for Blob {
    fn try_from(vec: Vec<u8>) -> Result<Self> {
        if vec.len() > BytesLen::max_value() as usize {
            Err(ErrorKind::MaximumLengthExceeded.into())
        } else {
            Ok(Blob { inner: vec.into() })
        }
    }
}

impl CqlSerializable for Blob {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.extend(self.inner.as_ref());
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        Ok(Blob { inner: data })
    }

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(self.inner.len() as BytesLen)
    }
}

impl Debug for Blob {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        self.inner.fmt(fmt)
    }
}

#[cfg(feature = "with-serde")]
impl ::serde::Serialize for Blob {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
        where S: ::serde::ser::Serializer
    {
        serializer.serialize_bytes(self.inner.as_ref())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn blob_display() {
        let x = Blob::try_from(vec![0x01, 0x02]).unwrap();
        assert_eq!("b\"\\x01\\x02\"", format!("{:?}", x));
    }
}

#[cfg(feature = "with-serde")]
#[cfg(test)]
mod serde_testing {
    use super::*;

    extern crate serde_test;
    use self::serde_test::{Token, assert_ser_tokens};

    #[test]
    fn blob_serde() {
        let x = Blob::try_from(vec![0x01, 0x02]).unwrap();
        assert_ser_tokens(&x, &[Token::Bytes(b"\x01\x02")]);
    }
}
