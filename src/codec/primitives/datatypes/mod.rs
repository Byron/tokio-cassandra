use byteorder::{ByteOrder, BigEndian};
use codec::primitives::CqlBytes;
use bytes::{BufMut, BytesMut};

type BytesLen = i32;

error_chain!{
    errors {
        InvalidAscii
        Incomplete
    }

    foreign_links {
        DecodeErr(::codec::primitives::decode::Error);
    }
}

pub trait CqlSerializable
    where Self: Sized
{
    fn deserialize(data: BytesMut) -> Result<Self>;
    fn serialize(&self, &mut BytesMut);
    fn bytes_len(&self) -> BytesLen;
}

// Bounds checking needs to be done in constructor
#[derive(Debug, PartialEq, Eq, Clone)]
struct Ascii {
    inner: BytesMut,
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

#[derive(Debug, PartialEq, Eq, Clone)]
struct Bigint {
    inner: i64,
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

// Bounds checking needs to be done in constructor
#[derive(Debug, PartialEq, Eq, Clone)]
struct Blob {
    inner: BytesMut,
}

impl CqlSerializable for Blob {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.extend(self.inner.as_ref());
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        Ok(Blob { inner: data })
    }

    fn bytes_len(&self) -> BytesLen {
        self.inner.len() as BytesLen
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Boolean {
    inner: bool,
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

    fn bytes_len(&self) -> BytesLen {
        1
    }
}

// Bounds checking needs to be done in constructor
#[derive(Debug, PartialEq, Eq, Clone)]
struct List<T: CqlSerializable> {
    inner: Vec<Option<T>>,
}

impl<T: CqlSerializable> CqlSerializable for List<T> {
    fn serialize(&self, buf: &mut BytesMut) {
        ::codec::primitives::encode::int(self.inner.len() as BytesLen, buf);
        for item in &self.inner {
            match item {
                &Some(ref item) => {
                    ::codec::primitives::encode::int(item.bytes_len(), buf);
                    item.serialize(buf);
                }
                &None => ::codec::primitives::encode::bytes(&CqlBytes::null_value(), buf),
            }
        }
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        let (data, n) = ::codec::primitives::decode::int(data)?;
        let mut v = Vec::new();

        let mut d = data;
        for _ in 0..n {
            let (data, bytes) = ::codec::primitives::decode::bytes(d)?;
            v.push(match bytes.buffer() {
                       Some(b) => Some(T::deserialize(b)?),
                       None => None,
                   });
            d = data
        }

        Ok(List { inner: v })
    }

    fn bytes_len(&self) -> BytesLen {
        self.inner.len() as BytesLen
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Varint {
    inner: BytesMut,
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

impl ToString for Varint {
    fn to_string(&self) -> String {
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

        format!("{}", bint)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Decimal {
    scale: i32,
    unscaled: Varint,
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

    fn bytes_len(&self) -> BytesLen {
        4 + self.unscaled.bytes_len()
    }
}

#[derive(Debug, PartialEq, Clone)]
struct Double {
    inner: f64,
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
#[cfg(test)]
mod test_encode_decode {
    use super::*;
    use bytes::BytesMut;

    fn assert_serialization_deserialization<T>(to_encode: T)
        where T: Clone + PartialEq + ::std::fmt::Debug + CqlSerializable
    {
        let mut encoded = BytesMut::with_capacity(64);
        to_encode.clone().serialize(&mut encoded);

        let decoded = T::deserialize(encoded.into());
        assert_eq!(to_encode, decoded.unwrap());
    }

    #[test]
    fn ascii() {
        let to_encode = Ascii { inner: vec![0x00, 0x23].into() };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn ascii_failing() {
        let to_encode = Ascii { inner: vec![0x00, 0x80].into() };
        let mut encoded = BytesMut::with_capacity(64);
        to_encode.clone().serialize(&mut encoded);
        let decoded = Ascii::deserialize(encoded.into());
        assert!(decoded.is_err());
    }

    #[test]
    fn bigint() {
        let to_encode = Bigint { inner: -123456789 };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn blob() {
        let to_encode = Blob { inner: vec![0x00, 0x81].into() };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn boolean() {
        let to_encode = Boolean { inner: false };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn decimal() {
        let to_encode = Decimal {
            scale: 1,
            unscaled: Varint { inner: vec![0x00, 0x80].into() },
        };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn double() {
        let to_encode = Double { inner: 1.23 };
        assert_serialization_deserialization(to_encode);
    }


    //    #[test]
    //    fn float() {
    //        let to_encode = Float { inner: 1.23 };
    //        assert_serialization_deserialization(to_encode);
    //    }
    //
    //    #[test]
    //    fn inet_v4() {
    //        let to_encode = Inet::Ipv4(Ipv4Addr::new(127, 0, 0, 1));
    //        assert_serialization_deserialization(to_encode);
    //    }
    //
    //    #[test]
    //    fn inet_v6() {
    //        let to_encode = Inet::Ipv6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc00a, 0x2ff));
    //        assert_serialization_deserialization(to_encode);
    //    }
    //
    //    #[test]
    //    fn int() {
    //        let to_encode = Int { inner: 123 };
    //        assert_serialization_deserialization(to_encode);
    //    }

    #[test]
    fn varint() {
        let to_encode = Varint { inner: vec![0x00, 0x80].into() };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn varint_to_string() {
        assert_eq!(&Varint { inner: vec![0x00].into() }.to_string(), "0");
        assert_eq!(&Varint { inner: vec![0x01].into() }.to_string(), "1");
        assert_eq!(&Varint { inner: vec![0x7F].into() }.to_string(), "127");
        assert_eq!(&Varint { inner: vec![0x00, 0x80].into() }.to_string(),
                   "128");
        assert_eq!(&Varint { inner: vec![0x00, 0x81].into() }.to_string(),
                   "129");
        assert_eq!(&Varint { inner: vec![0xFF].into() }.to_string(), "-1");
        assert_eq!(&Varint { inner: vec![0x80].into() }.to_string(), "-128");
        assert_eq!(&Varint { inner: vec![0xFF, 0x7F].into() }.to_string(),
                   "-129");
    }

    #[test]
    fn list_boolean() {
        let to_encode = List { inner: vec![Some(Boolean { inner: false }), Some(Boolean { inner: true }), None] };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn list_double() {
        let to_encode = List { inner: vec![Some(Double { inner: 1.23 }), Some(Double { inner: 2.34 })] };
        assert_serialization_deserialization(to_encode);
    }
}
