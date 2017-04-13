use super::*;
use std::fmt::{Write, Debug};

// Bounds checking needs to be done in constructor
#[derive(PartialEq, Eq, Clone)]
pub struct Ascii {
    inner: BytesMut,
}

impl TryFrom<Vec<u8>> for Ascii {
    fn try_from(vec: Vec<u8>) -> Result<Self> {
        if vec.len() > BytesLen::max_value() as usize {
            Err(ErrorKind::MaximumLengthExceeded.into())
        } else {
            Ok(Ascii { inner: vec.into() })
        }
    }
}

impl CqlSerializable for Ascii {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.extend(self.inner.as_ref());
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        for b in data.as_ref() {
            if *b > 127 as u8 {
                return Err(ErrorKind::InvalidAscii.into());
            }
        }

        Ok(Ascii { inner: data })
    }

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(self.inner.len() as BytesLen)
    }
}

impl Debug for Ascii {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        fmt.write_char('"')?;
        for c in &self.inner {
            fmt.write_char(c as char)?;
        }
        fmt.write_char('"')
    }
}

// Bounds-Checking in Constructor
#[derive(PartialEq, Clone)]
pub struct Text {
    inner: String,
}

impl TryFrom<&'static str> for Text {
    fn try_from(str: &'static str) -> Result<Self> {
        if str.len() > BytesLen::max_value() as usize {
            Err(ErrorKind::MaximumLengthExceeded.into())
        } else {
            Ok(Text { inner: str.to_string() })
        }
    }
}

impl CqlSerializable for Text {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.extend(self.inner.as_bytes());
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        Ok(Text { inner: String::from(::std::str::from_utf8(data.as_ref()).unwrap()) })
    }

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(self.inner.len() as BytesLen)
    }
}

#[cfg(feature = "with-serde")]
impl ::serde::Serialize for Text {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
        where S: ::serde::ser::Serializer
    {
        serializer.serialize_str(&self.inner)
    }
}

pub type Varchar = Text;

impl Debug for Text {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        Debug::fmt(&self.inner, fmt)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(feature = "with-serde")]
    extern crate serde_test;
    #[cfg(feature = "with-serde")]
    use self::serde_test::{Token, assert_ser_tokens};

    #[test]
    fn ascii_debug() {
        let x = Ascii::try_from(vec![0x32, 0x33, 0x34]).unwrap();
        assert_eq!("\"234\"", format!("{:?}", x))
    }

    #[test]
    fn text_debug() {
        let x = Text::try_from("abc123").unwrap();
        assert_eq!("\"abc123\"", format!("{:?}", x))
    }

    #[test]
    fn varchar_debug() {
        let x = Varchar::try_from("abc123").unwrap();
        assert_eq!("\"abc123\"", format!("{:?}", x))
    }

    #[cfg(feature = "with-serde")]
    #[test]
    fn varchar_serde() {
        let vc = Varchar::try_from("abc123").unwrap();
        assert_ser_tokens(&vc, &[Token::Str("abc123")]);
    }
}
