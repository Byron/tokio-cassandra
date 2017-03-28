use super::*;
use std::fmt::Display;

// Bounds checking needs to be done in constructor
#[derive(Debug, PartialEq, Eq, Clone)]
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

    fn bytes_len(&self) -> BytesLen {
        self.inner.len() as BytesLen
    }
}

impl Display for Ascii {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        for c in &self.inner {
            fmt.write_char(c as char)?;
        }
        Ok(())
    }
}

// Bounds-Checking in Constructor
#[derive(Debug, PartialEq, Clone)]
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

    fn bytes_len(&self) -> BytesLen {
        self.inner.len() as BytesLen
    }
}

pub type Varchar = Text;

impl Display for Text {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        Display::fmt(&self.inner, fmt)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn ascii_display() {
        let x = Ascii::try_from(vec![0x32, 0x33, 0x34]).unwrap();
        assert_eq!("234", format!("{}", x))
    }

    #[test]
    fn text_display() {
        let x = Text::try_from("abc123").unwrap();
        assert_eq!("abc123", format!("{}", x))
    }

    #[test]
    fn varchar_display() {
        let x = Varchar::try_from("abc123").unwrap();
        assert_eq!("abc123", format!("{}", x))
    }
}
