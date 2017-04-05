use super::*;
use std::fmt::Display;

// Bounds checking needs to be done in constructor
#[derive(Debug, PartialEq, Eq, Clone)]
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

impl Display for Blob {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        fmt.write_str("[BLOB]")
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn blob_display() {
        let x = Blob::try_from(vec![0x01, 0x02]).unwrap();
        assert_eq!("[BLOB]", format!("{}", x));
    }
}
