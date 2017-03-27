use super::*;

// Bounds checking needs to be done in constructor
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Ascii {
    inner: BytesMut,
}

impl TryFrom<Vec<u8>> for Ascii {
    fn try_from(vec: Vec<u8>) -> Result<Ascii> {
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

#[cfg(test)]
mod test {
    use super::*;

}
