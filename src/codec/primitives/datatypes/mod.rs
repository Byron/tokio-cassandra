use byteorder::{ByteOrder, BigEndian};
use codec::primitives::CqlBytes;
use bytes::{BufMut, BytesMut};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::collections::{HashSet, HashMap};
use std::marker::PhantomData;

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
pub struct Ascii {
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
pub struct Bigint {
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
pub struct Blob {
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
pub struct Boolean {
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
pub struct List<T: CqlSerializable> {
    inner: Vec<Option<T>>,
}

impl<T: CqlSerializable> CqlSerializable for List<T> {
    fn serialize(&self, buf: &mut BytesMut) {
        ::codec::primitives::encode::int(self.inner.len() as BytesLen, buf);
        for item in &self.inner {
            serialize_bytes(item, buf);
        }
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        let (data, n) = ::codec::primitives::decode::int(data)?;
        let mut v = Vec::new();

        let mut d = data;
        for _ in 0..n {
            let (data, item) = deserialize_bytes(d)?;
            v.push(item);
            d = data
        }

        Ok(List { inner: v })
    }

    fn bytes_len(&self) -> BytesLen {
        self.inner.len() as BytesLen
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Varint {
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

#[derive(Debug, PartialEq, Clone)]
struct Float {
    inner: f32,
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
#[derive(Debug, PartialEq, Eq)]
pub struct Map<K, V>
    where K: CqlSerializable,
          V: CqlSerializable
{
    //    FIXME: is this a good idea to use BytesMut here?
    inner: HashMap<BytesMut, Option<V>>,
    p: PhantomData<K>,
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
#[derive(Debug, PartialEq, Eq)]
pub struct Set<V>
    where V: CqlSerializable
{
    inner: HashSet<BytesMut>,
    p: PhantomData<V>,
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
        match bytes.buffer() {
            Some(b) => Some(T::deserialize(b)?),
            None => None,
        }))
}

fn deserialize_bytesmut(buf: BytesMut) -> Result<(BytesMut, Option<BytesMut>)> {
    let (data, bytes) = ::codec::primitives::decode::bytes(buf)?;
    Ok((data, bytes.buffer()))
}

// Bounds-Checking in Constructor
#[derive(Debug, PartialEq, Clone)]
pub struct Text {
    inner: String,
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

pub type VarChar = Text;

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

    #[test]
    fn float() {
        let to_encode = Float { inner: 1.23 };
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
        let to_encode = Int { inner: 123 };
        assert_serialization_deserialization(to_encode);
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

    #[test]
    fn map() {
        let to_encode = {
            let mut m = Map::new();
            m.insert(Int { inner: 1 }, Some(Boolean { inner: true }));
            m.insert(Int { inner: 2 }, Some(Boolean { inner: true }));
            m.insert(Int { inner: 3 }, None);
            m
        };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn set() {
        let to_encode = {
            let mut s = Set::new();
            s.insert(Int { inner: 1 });
            s.insert(Int { inner: 2 });
            s.insert(Int { inner: 3 });
            s
        };
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn text() {
        let to_encode = Text { inner: String::from("text") };
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
        let to_encode = VarChar { inner: String::from("text") };
        assert_serialization_deserialization(to_encode);
    }

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
