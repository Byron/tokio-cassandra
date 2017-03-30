use byteorder::{ByteOrder, BigEndian};
use codec::primitives::CqlBytes;
use bytes::{BufMut, BytesMut};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::collections::{HashSet, HashMap};
use std::marker::PhantomData;
use std::fmt::{Debug, Formatter, Write};

type BytesLen = i32;

error_chain!{
    errors {
        InvalidAscii
        Incomplete
        MaximumLengthExceeded
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

pub trait TryFrom<T>
    where Self: Sized
{
    fn try_from(data: T) -> Result<Self>;
}

mod byte;
pub use self::byte::*;

mod collections;
pub use self::collections::*;

mod num;
pub use self::num::*;

mod text;
pub use self::text::*;

mod udt;
pub use self::udt::*;

mod primitive;
pub use self::primitive::*;



#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Decimal {
    scale: i32,
    unscaled: Varint,
}

// TODO: impl From<f64> ...
// TODO: impl other useful initializers, also for other types
//impl From<(dDecimal {
//    fn new(unscaled: i64, scale: i32) {
//       Decimal {
//           scale
//       }
//    }
//}

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
pub enum Inet {
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr),
}

impl CqlSerializable for Inet {
    fn serialize(&self, buf: &mut BytesMut) {
        match *self {
            Inet::Ipv4(addr) => buf.extend(&addr.octets()[..]),
            Inet::Ipv6(addr) => buf.extend(&addr.octets()[..]),
        }
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        if data.len() < 4 {
            // FIXME: what if we read in chunks? How to see if ipv4 or ipv6
            // Should not be a problem actually since we should never get a
            // chunk here, since we are passing the CqlBytes read
            return Err(ErrorKind::Incomplete.into());
        }

        Ok(match data.len() {
               4 => Inet::Ipv4(Ipv4Addr::from([data[0], data[1], data[2], data[3]])),
               16 => {
                   Inet::Ipv6(Ipv6Addr::from([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                                              data[8], data[9], data[10], data[11], data[12], data[13], data[14],
                                              data[15]]))
               }
               _ => return Err(ErrorKind::Incomplete.into()),
           })
    }

    fn bytes_len(&self) -> BytesLen {
        match *self {
            Inet::Ipv4(_) => 4,
            Inet::Ipv6(_) => 16,
        }
    }
}

// Bounds checking needs to be done in constructor
#[derive(PartialEq, Eq)]
pub struct Map<K, V>
    where K: CqlSerializable,
          V: CqlSerializable
{
    //    FIXME: is this a good idea to use BytesMut here?
    inner: HashMap<BytesMut, Option<V>>,
    p: PhantomData<K>,
}

impl<K, V> Debug for Map<K, V>
    where V: CqlSerializable + Debug,
          K: CqlSerializable + Debug
{
    fn fmt(&self, fmt: &mut Formatter) -> ::std::fmt::Result {
        for (key, value) in &self.inner {
            let key = K::deserialize(key.clone());
            match key {
                Ok(k) => k.fmt(fmt)?,
                Err(_) => fmt.write_str("[ERROR]")?,
            }

            fmt.write_str("=>")?;

            match value.clone() {
                &Some(ref b) => b.fmt(fmt)?,
                &None => fmt.write_str("NULL")?,
            }

            fmt.write_char(',')?;
        }
        Ok(())
    }
}

impl<K, V> Map<K, V>
    where K: CqlSerializable,
          V: CqlSerializable
{
    pub fn new() -> Self {
        Map {
            inner: HashMap::new(),
            p: PhantomData,
        }
    }

    pub fn insert(&mut self, key: K, value: Option<V>) {
        //        FIXME: find a good length
        let mut bytes = BytesMut::with_capacity(128);
        key.serialize(&mut bytes);
        self.inner.insert(bytes, value);
    }
}


impl<K, V> CqlSerializable for Map<K, V>
    where K: CqlSerializable,
          V: CqlSerializable
{
    fn serialize(&self, buf: &mut BytesMut) {
        ::codec::primitives::encode::int(self.inner.len() as BytesLen, buf);

        for (k, v) in &self.inner {
            // FIXME: bound checks
            ::codec::primitives::encode::int(k.len() as i32, buf);
            buf.extend(k);
            serialize_bytes(v, buf);
        }
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        let (data, n) = ::codec::primitives::decode::int(data)?;
        let mut m = Map::new();
        let mut d = data;
        for _ in 0..n {
            let (data, k) = deserialize_bytes::<K>(d)?;
            let k = match k {
                Some(k) => k,
                None => panic!(),
            };

            let (data, v) = deserialize_bytes::<V>(data)?;
            m.insert(k, v);
            d = data
        }
        Ok(m)
    }

    fn bytes_len(&self) -> BytesLen {
        self.inner.len() as BytesLen
    }
}

// Bounds checking needs to be done in constructor
#[derive(PartialEq, Eq)]
pub struct Set<V>
    where V: CqlSerializable
{
    inner: HashSet<BytesMut>,
    p: PhantomData<V>,
}

impl<V> Debug for Set<V>
    where V: CqlSerializable + Debug
{
    fn fmt(&self, fmt: &mut Formatter) -> ::std::fmt::Result {
        for item in &self.inner {
            let v = V::deserialize(item.clone());
            match v {
                Ok(v) => v.fmt(fmt)?,
                Err(_) => fmt.write_str("[ERROR]")?,
            }
            fmt.write_char(',')?;
        }
        Ok(())
    }
}


impl<V> Set<V>
    where V: CqlSerializable
{
    pub fn new() -> Self {
        Set {
            inner: HashSet::new(),
            p: PhantomData,
        }
    }

    pub fn insert(&mut self, value: V) {
        //        FIXME: find a good length
        let mut bytes = BytesMut::with_capacity(128);
        value.serialize(&mut bytes);
        self.inner.insert(bytes);
    }
}

impl<V> CqlSerializable for Set<V>
    where V: CqlSerializable
{
    fn serialize(&self, buf: &mut BytesMut) {
        // FIXME: bound checks
        ::codec::primitives::encode::int(self.inner.len() as BytesLen, buf);

        for v in &self.inner {
            // FIXME: bound checks
            ::codec::primitives::encode::int(v.len() as i32, buf);
            buf.extend(v);
        }
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        let (data, n) = ::codec::primitives::decode::int(data)?;
        let mut s = Set::new();
        let mut d = data;
        for _ in 0..n {
            let (data, v) = deserialize_bytes::<V>(d)?;
            if let Some(v) = v {
                s.insert(v);
            }
            d = data
        }
        Ok(s)
    }

    fn bytes_len(&self) -> BytesLen {
        self.inner.len() as BytesLen
    }
}
fn serialize_bytes<T>(data: &Option<T>, buf: &mut BytesMut)
    where T: CqlSerializable
{
    match data {
        &Some(ref item) => {
            //            FIXME: bounds check
            ::codec::primitives::encode::int(item.bytes_len(), buf);
            item.serialize(buf);
        }
        &None => ::codec::primitives::encode::bytes(&CqlBytes::null_value(), buf),
    }
}

fn serialize_bytesmut(data: &Option<BytesMut>, buf: &mut BytesMut) {
    match data {
        &Some(ref item) => {
            //            FIXME: bounds check
            ::codec::primitives::encode::int(item.len() as BytesLen, buf);
            buf.extend(item);
        }
        &None => ::codec::primitives::encode::bytes(&CqlBytes::null_value(), buf),
    }
}

fn deserialize_bytes<T>(buf: BytesMut) -> Result<(BytesMut, Option<T>)>
    where T: CqlSerializable
{
    let (data, bytes) = ::codec::primitives::decode::bytes(buf)?;
    Ok((data,
        match bytes.as_option() {
            Some(b) => Some(T::deserialize(b)?),
            None => None,
        }))
}

fn deserialize_bytesmut(buf: BytesMut) -> Result<(BytesMut, Option<BytesMut>)> {
    let (data, bytes) = ::codec::primitives::decode::bytes(buf)?;
    Ok((data, bytes.as_option()))
}


#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Timestamp {
    epoch: i64,
}

impl CqlSerializable for Timestamp {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.reserve(8);
        buf.put_i64::<BigEndian>(self.epoch);
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        if data.len() != 8 {
            return Err(ErrorKind::Incomplete.into());
        }
        let long = BigEndian::read_i64(data.as_ref());
        Ok(Timestamp { epoch: long })
    }

    fn bytes_len(&self) -> BytesLen {
        8
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Uuid {
    inner: [u8; 16],
}

impl CqlSerializable for Uuid {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.reserve(16);
        buf.put_slice(&self.inner[..])
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        if data.len() != 16 {
            return Err(ErrorKind::Incomplete.into());
        }
        let arr = [data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10],
                   data[11], data[12], data[13], data[14], data[15]];
        Ok(Uuid { inner: arr })
    }

    fn bytes_len(&self) -> BytesLen {
        16
    }
}

pub type TimeUuid = Uuid;

// Bounds checking needs to be done in constructor
#[derive(Debug, PartialEq, Eq)]
pub struct BytesMutCollection {
    inner: Vec<Option<BytesMut>>,
}

impl CqlSerializable for BytesMutCollection {
    fn serialize(&self, buf: &mut BytesMut) {
        ::codec::primitives::encode::int(self.inner.len() as BytesLen, buf);
        for item in &self.inner {
            serialize_bytesmut(item, buf);
        }
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        let (data, n) = ::codec::primitives::decode::int(data)?;
        let mut v = Vec::new();

        let mut d = data;
        for _ in 0..n {
            let (data, item) = deserialize_bytesmut(d)?;
            v.push(item);
            d = data
        }

        Ok(BytesMutCollection { inner: v })
    }

    fn bytes_len(&self) -> BytesLen {
        self.inner.len() as BytesLen
    }
}

pub type Tuple = BytesMutCollection;
pub type Udt = BytesMutCollection;

#[cfg(test)]
mod test_encode_decode {
    use super::*;
    use bytes::BytesMut;

    fn assert_serialization_deserialization<T>(to_encode: T)
        where T: PartialEq + ::std::fmt::Debug + CqlSerializable
    {
        let mut encoded = BytesMut::with_capacity(64);
        to_encode.serialize(&mut encoded);

        let decoded = T::deserialize(encoded.into());
        assert_eq!(to_encode, decoded.unwrap());
    }

    #[test]
    fn ascii() {
        let to_encode = Ascii::try_from(vec![0x00, 0x23]).unwrap();
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn ascii_failing() {
        let to_encode = Ascii::try_from(vec![0x00, 0x80]).unwrap();
        let mut encoded = BytesMut::with_capacity(64);
        to_encode.clone().serialize(&mut encoded);
        let decoded = Ascii::deserialize(encoded.into());
        assert!(decoded.is_err());
    }

    #[test]
    fn bigint() {
        let to_encode = Bigint::new(-123456789);
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn blob() {
        let to_encode = Blob::try_from(vec![0x00, 0x81]).unwrap();
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn boolean() {
        let to_encode = Boolean::new(false);
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn decimal() {
        let to_encode = Decimal {
            scale: 1,
            unscaled: Varint::try_from(vec![0x00, 0x80]).unwrap(),
        };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn double() {
        let to_encode = Double::new(1.23);
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn float() {
        let to_encode = Float::new(1.23);
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn inet_v4() {
        let to_encode = Inet::Ipv4(Ipv4Addr::new(127, 0, 0, 1));
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn inet_v6() {
        let to_encode = Inet::Ipv6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc00a, 0x2ff));
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn int() {
        let to_encode = Int::new(123);
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn list_boolean() {
        let to_encode = List::try_from(vec![Some(Boolean::new(false)), Some(Boolean::new(true)), None]).unwrap();
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn list_double() {
        let to_encode = List::try_from(vec![Some(Double::new(1.23)), Some(Double::new(2.34))]).unwrap();
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn map() {
        let to_encode = {
            let mut m = Map::new();
            m.insert(Int::new(1), Some(Boolean::new(true)));
            m.insert(Int::new(2), Some(Boolean::new(true)));
            m.insert(Int::new(3), None);
            m
        };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn set() {
        let to_encode = {
            let mut s = Set::new();
            s.insert(Int::new(1));
            s.insert(Int::new(2));
            s.insert(Int::new(3));
            s
        };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn text() {
        let to_encode = Text::try_from("text").unwrap();
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn timestamp() {
        let to_encode = Timestamp { epoch: 12343521 };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn uuid() {
        let to_encode = Uuid { inner: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn varchar() {
        let to_encode = Varchar::try_from("text").unwrap();
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn varint() {
        let to_encode = Varint::try_from(vec![0x00, 0x80]).unwrap();
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn varint_to_string() {
        assert_eq!(&Varint::try_from(vec![0x00]).unwrap().to_string(), "0");
        assert_eq!(&Varint::try_from(vec![0x01]).unwrap().to_string(), "1");
        assert_eq!(&Varint::try_from(vec![0x7F]).unwrap().to_string(), "127");
        assert_eq!(&Varint::try_from(vec![0x00, 0x80]).unwrap().to_string(),
                   "128");
        assert_eq!(&Varint::try_from(vec![0x00, 0x81]).unwrap().to_string(),
                   "129");
        assert_eq!(&Varint::try_from(vec![0xFF]).unwrap().to_string(), "-1");
        assert_eq!(&Varint::try_from(vec![0x80]).unwrap().to_string(), "-128");
        assert_eq!(&Varint::try_from(vec![0xFF, 0x7F]).unwrap().to_string(),
                   "-129");
    }

    #[test]
    fn timeuuid() {
        let to_encode = TimeUuid { inner: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn tuple() {
        let to_encode = Tuple { inner: vec![Some(vec![0x00, 0x80].into()), None, Some(vec![0x00, 0x80].into())] };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn udt() {
        let to_encode = Udt { inner: vec![Some(vec![0x00, 0x80].into()), None, Some(vec![0x00, 0x80].into())] };
        assert_serialization_deserialization(to_encode);
    }
}
