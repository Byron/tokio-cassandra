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

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Varint {
    inner: BytesMut,
}

impl TryFrom<Vec<u8>> for Varint {
    fn try_from(data: Vec<u8>) -> Result<Self> {
        if data.len() > BytesLen::max_value() as usize {
            Err(ErrorKind::MaximumLengthExceeded.into())
        } else {
            Ok(Varint { inner: data.into() })
        }
    }
}

impl CqlSerializable for Varint {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.extend(self.inner.as_ref());
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        Ok(Varint { inner: data })
    }

    fn bytes_len(&self) -> BytesLen {
        self.inner.len() as BytesLen
    }
}

impl Display for Varint {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        use num_bigint::{Sign, BigInt};
        let bytes = self.inner.as_ref();

        let bint = {
            if bytes[0] & 0x80 == 0x80 {
                let v: Vec<u8> = Vec::from(bytes).into_iter().map(|x| !x).collect();
                BigInt::from_bytes_be(Sign::Minus, &v[..]) - BigInt::from(1)
            } else {
                BigInt::from_bytes_be(Sign::Plus, bytes)
            }
        };

        Display::fmt(&bint, fmt)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Double {
    inner: f64,
}

impl Double {
    pub fn new(f: f64) -> Self {
        Double { inner: f }
    }
}

impl Display for Double {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        Display::fmt(&self.inner, fmt)
    }
}

impl CqlSerializable for Double {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.reserve(8);
        buf.put_f64::<BigEndian>(self.inner);
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        if data.len() < 8 {
            return Err(ErrorKind::Incomplete.into());
        }
        let v = BigEndian::read_f64(data.as_ref());
        Ok(Double { inner: v })
    }

    fn bytes_len(&self) -> BytesLen {
        8
    }
}


#[derive(Debug, PartialEq, Clone)]
pub struct Float {
    inner: f32,
}

impl Float {
    pub fn new(f: f32) -> Self {
        Float { inner: f }
    }
}

impl Display for Float {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        Display::fmt(&self.inner, fmt)
    }
}

impl CqlSerializable for Float {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.reserve(4);
        buf.put_f32::<BigEndian>(self.inner);
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        if data.len() < 4 {
            return Err(ErrorKind::Incomplete.into());
        }
        let v = BigEndian::read_f32(data.as_ref());
        Ok(Float { inner: v })
    }

    fn bytes_len(&self) -> BytesLen {
        4
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Int {
    inner: i32,
}

impl Display for Int {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        Display::fmt(&self.inner, fmt)
    }
}

impl Int {
    pub fn new(i: i32) -> Self {
        Int { inner: i }
    }
}

impl CqlSerializable for Int {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.reserve(4);
        buf.put_i32::<BigEndian>(self.inner);
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        if data.len() < 4 {
            return Err(ErrorKind::Incomplete.into());
        }
        let v = BigEndian::read_i32(data.as_ref());
        Ok(Int { inner: v })
    }

    fn bytes_len(&self) -> BytesLen {
        4
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
