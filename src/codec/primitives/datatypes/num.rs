use super::*;
use std::fmt::Display;
use bytes::BufMut;
use byteorder::ByteOrder;

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

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(8)
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

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(self.inner.len() as BytesLen)
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

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(8)
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

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(4)
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

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(4)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Decimal {
    scale: i32,
    unscaled: Varint,
}

impl Decimal {
    pub fn new(scale: i32, unscaled: Varint) -> Self {
        Decimal {
            scale: scale,
            unscaled: unscaled,
        }
    }
}

impl CqlSerializable for Decimal {
    fn serialize(&self, buf: &mut BytesMut) {
        ::codec::primitives::encode::int(self.scale, buf);
        self.unscaled.serialize(buf);
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        let (data, scale) = ::codec::primitives::decode::int(data)?;
        let unscaled = Varint::deserialize(data)?;
        Ok(Decimal {
               scale: scale,
               unscaled: unscaled,
           })
    }

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(4 + self.unscaled.bytes_len().unwrap_or(0))
    }
}

impl Display for Decimal {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        let unscaled = format!("{}", self.unscaled);
        let n = self.scale + 1;
        let n = n - unscaled.len() as i32;
        if n > 0 {
            fmt.write_str("0.")?;
            for _ in 0..n {
                fmt.write_char('0')?;
            }

            fmt.write_str(&unscaled)?;
        } else {
            for (i, c) in unscaled.chars().enumerate() {
                if self.scale != 0 && i == self.scale as usize {
                    fmt.write_char('.')?;
                }
                fmt.write_char(c)?;
            }
        }
        Ok(())
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

    #[test]
    fn varint_display() {
        let x = Varint::try_from(vec![0x00, 0x02]).unwrap();
        assert_eq!("2", format!("{}", x));
    }

    #[test]
    fn float_display() {
        let x = Float::new(-1.23);
        assert_eq!("-1.23", format!("{}", x));
    }

    #[test]
    fn double_display() {
        let x = Double::new(-1.23);
        assert_eq!("-1.23", format!("{}", x));
    }

    #[test]
    fn int_display() {
        let x = Int::new(-123);
        assert_eq!("-123", format!("{}", x));
    }

    #[test]
    fn decimal_display() {
        let x = Decimal::new(2, Varint::try_from(vec![0x09]).unwrap());
        assert_eq!("0.009", format!("{}", x));

        let x = Decimal::new(0, Varint::try_from(vec![0x09]).unwrap());
        assert_eq!("9", format!("{}", x));

        let x = Decimal::new(2, Varint::try_from(vec![0x05, 0x09]).unwrap());
        assert_eq!("12.89", format!("{}", x));
    }
}
