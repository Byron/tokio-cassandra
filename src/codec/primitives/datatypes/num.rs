use super::*;
use std::fmt::Display;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Bigint {
    inner: i64,
}

impl Bigint {
    pub fn new(v: i64) -> Self {
        Bigint { inner: v }
    }
}

impl CqlSerializable for Bigint {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.reserve(8);
        buf.put_i64::<BigEndian>(self.inner);
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        if data.len() != 8 {
            return Err(ErrorKind::Incomplete.into());
        }
        let long = BigEndian::read_i64(data.as_ref());
        Ok(Bigint { inner: long })
    }

    fn bytes_len(&self) -> BytesLen {
        8
    }
}

impl Display for Bigint {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        Display::fmt(&self.inner, fmt)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn bigint_display() {
        let x = Bigint::new(-123);
        assert_eq!("-123", format!("{}", x));
    }
}
