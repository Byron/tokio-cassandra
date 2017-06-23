use super::*;
use std::fmt::{Write, Debug};
use bytes::BufMut;
use byteorder::ByteOrder;

#[derive(PartialEq, Eq, Clone)]
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

impl Debug for Bigint {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        self.inner.fmt(fmt)
    }
}

#[cfg(feature = "with-serde")]
impl ::serde::Serialize for Bigint {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        serializer.serialize_i64(self.inner)
    }
}

#[derive(PartialEq, Eq, Clone)]
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

impl Debug for Varint {
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

        ::std::fmt::Display::fmt(&bint, fmt)
    }
}

impl ::std::fmt::Display for Varint {
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

        ::std::fmt::Display::fmt(&bint, fmt)
    }
}

#[cfg(feature = "with-serde")]
impl ::serde::Serialize for Varint {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        serializer.serialize_str(&format!("{:?}", self))
    }
}

#[derive(PartialEq, Clone)]
pub struct Double {
    inner: f64,
}

impl Double {
    pub fn new(f: f64) -> Self {
        Double { inner: f }
    }
}

impl Debug for Double {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        self.inner.fmt(fmt)
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

#[cfg(feature = "with-serde")]
impl ::serde::Serialize for Double {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        serializer.serialize_f64(self.inner)
    }
}

#[derive(PartialEq, Clone)]
pub struct Float {
    inner: f32,
}

impl Float {
    pub fn new(f: f32) -> Self {
        Float { inner: f }
    }
}

impl Debug for Float {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        self.inner.fmt(fmt)
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

#[cfg(feature = "with-serde")]
impl ::serde::Serialize for Float {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        serializer.serialize_f32(self.inner)
    }
}

#[derive(PartialEq, Clone)]
pub struct Int {
    inner: i32,
}

impl Debug for Int {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        self.inner.fmt(fmt)
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

#[cfg(feature = "with-serde")]
impl ::serde::Serialize for Int {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        serializer.serialize_i32(self.inner)
    }
}

#[derive(PartialEq, Eq, Clone)]
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

impl Debug for Decimal {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        let unscaled = format!("{:?}", self.unscaled);
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

#[cfg(feature = "with-serde")]
impl ::serde::Serialize for Decimal {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        serializer.serialize_str(&format!("{:?}", self))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn bigint_debug() {
        let x = Bigint::new(-123);
        assert_eq!("-123", format!("{:?}", x));
    }

    #[test]
    fn varint_debug() {
        let x = Varint::try_from(vec![0x00, 0x02]).unwrap();
        assert_eq!("2", format!("{:?}", x));
    }

    #[test]
    fn float_debug() {
        let x = Float::new(-1.23);
        assert_eq!("-1.23", format!("{:?}", x));
    }

    #[test]
    fn double_debug() {
        let x = Double::new(-1.23);
        assert_eq!("-1.23", format!("{:?}", x));
    }

    #[test]
    fn int_debug() {
        let x = Int::new(-123);
        assert_eq!("-123", format!("{:?}", x));
    }

    #[test]
    fn decimal_debug() {
        let x = Decimal::new(2, Varint::try_from(vec![0x09]).unwrap());
        assert_eq!("0.009", format!("{:?}", x));

        let x = Decimal::new(0, Varint::try_from(vec![0x09]).unwrap());
        assert_eq!("9", format!("{:?}", x));

        let x = Decimal::new(2, Varint::try_from(vec![0x05, 0x09]).unwrap());
        assert_eq!("12.89", format!("{:?}", x));
    }
}

#[cfg(feature = "with-serde")]
#[cfg(test)]
mod serde_testing {
    use super::*;

    extern crate serde_test;
    use self::serde_test::{Token, assert_ser_tokens};

    #[test]
    fn bigint_serde() {
        let x = Bigint::new(-123);
        assert_ser_tokens(&x, &[Token::I64(-123)]);
    }

    #[test]
    fn varint_serde() {
        let x = Varint::try_from(vec![0x00, 0x02]).unwrap();
        assert_ser_tokens(&x, &[Token::Str("2")]);
    }

    #[test]
    fn float_serde() {
        let x = Float::new(-1.23);
        assert_ser_tokens(&x, &[Token::F32(-1.23)]);
    }

    #[test]
    fn double_serde() {
        let x = Double::new(-1.23);
        assert_ser_tokens(&x, &[Token::F64(-1.23)]);
    }

    #[test]
    fn int_serde() {
        let x = Int::new(-123);
        assert_ser_tokens(&x, &[Token::I32(-123)]);
    }

    #[test]
    fn decimal_serde() {
        let x = Decimal::new(2, Varint::try_from(vec![0x09]).unwrap());
        assert_ser_tokens(&x, &[Token::Str("0.009")]);

        let x = Decimal::new(0, Varint::try_from(vec![0x09]).unwrap());
        assert_ser_tokens(&x, &[Token::Str("9")]);

        let x = Decimal::new(2, Varint::try_from(vec![0x05, 0x09]).unwrap());
        assert_ser_tokens(&x, &[Token::Str("12.89")]);
    }
}
