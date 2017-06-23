use super::*;
use std::fmt::Debug;
use bytes::BufMut;

#[derive(PartialEq, Eq, Clone)]
pub struct Boolean {
    inner: bool,
}

impl Boolean {
    pub fn new(b: bool) -> Self {
        Boolean { inner: b }
    }
}

impl CqlSerializable for Boolean {
    fn serialize(&self, buf: &mut BytesMut) {
        if self.inner {
            buf.put_u8(0x01);
        } else {
            buf.put_u8(0x00);
        }
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        if data.len() != 1 {
            return Err(ErrorKind::Incomplete.into());
        }

        let b = data.as_ref()[0];
        Ok(Boolean { inner: b != 0x00 })
    }

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(1)
    }
}

impl Debug for Boolean {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        if self.inner {
            fmt.write_str("true")
        } else {
            fmt.write_str("false")
        }
    }
}

#[cfg(feature = "with-serde")]
impl ::serde::Serialize for Boolean {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        serializer.serialize_bool(self.inner)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn boolean_debug() {
        assert_eq!("true", format!("{:?}", Boolean::new(true)));
        assert_eq!("false", format!("{:?}", Boolean::new(false)));
    }
}

#[cfg(feature = "with-serde")]
#[cfg(test)]
mod serde_testing {
    use super::*;

    extern crate serde_test;
    use self::serde_test::{Token, assert_ser_tokens};

    #[test]
    fn boolean_serde() {
        let x = Boolean::new(true);
        assert_ser_tokens(&x, &[Token::Bool(true)]);
    }
}
