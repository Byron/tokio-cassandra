use byteorder::BigEndian;
use codec::primitives::CqlBytes;
use bytes::BytesMut;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::collections::{HashSet, HashMap};
use std::marker::PhantomData;
use std::fmt::{Formatter, Write};
use codec::response::ColumnType;
use std::ops::Deref;

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
    fn bytes_len(&self) -> Option<BytesLen>;
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

mod primitive;
pub use self::primitive::*;

mod special;
pub use self::special::*;

fn serialize_bytes<T>(data: &Option<T>, buf: &mut BytesMut)
    where T: CqlSerializable
{
    match data {
        &Some(ref item) => {
            if let Some(len) = item.bytes_len() {
                ::codec::primitives::encode::int(len, buf);
                item.serialize(buf);
            } else {
                let mut intermediate = BytesMut::with_capacity(1024); // FIXME: proper constant here
                item.serialize(&mut intermediate);
                // FIXME: bounds check
                ::codec::primitives::encode::int(intermediate.len() as i32, buf);
                buf.extend(intermediate);
            }
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

pub fn deserialize_bytesmut(buf: BytesMut) -> Result<(BytesMut, Option<BytesMut>)> {
    let (data, bytes) = ::codec::primitives::decode::bytes(buf)?;
    Ok((data, bytes.as_option()))
}

macro_rules! debug_cell {
    ($($s : pat => $t : ident ), *) => {
        pub fn debug_cell(coltype: &ColumnType, value: Option<BytesMut>,
                              fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
            use std::fmt::Debug;
            if let Some(value) = value {
                match *coltype {
                    $ (
                        $s => Debug::fmt(&$t::deserialize(value).map_err(|_| ::std::fmt::Error)?, fmt),
                    ) *
                    ColumnType::List(ref d) => {
                        Debug::fmt(&GenericList::new(RawList::deserialize(value)
                            .map_err(|_| ::std::fmt::Error)?, d), fmt)
                    }
                    ColumnType::Set(ref d) => {
                        Debug::fmt(&GenericSet::new(RawSet::deserialize(value)
                            .map_err(|_| ::std::fmt::Error)?, d), fmt)
                    }
                    ColumnType::Map(ref k, ref v) => {
                        Debug::fmt(&GenericMap::new(RawMap::deserialize(value)
                            .map_err(|_| ::std::fmt::Error)?, k, v), fmt)
                    }
                    ColumnType::Udt(ref d) => {
                        Debug::fmt(&Udt::new(RawUdt::deserialize(value)
                            .map_err(|_| ::std::fmt::Error)?, d), fmt)
                    }
                    ColumnType::Tuple(ref d) => {
                        Debug::fmt(&Tuple::new(RawTuple::deserialize(value)
                            .map_err(|_| ::std::fmt::Error)?, d), fmt)
                    }
                }
            } else {
                fmt.write_str("NULL")
            }
        }
    }
}

debug_cell!(
    ColumnType::Bigint => Bigint,
    ColumnType::Blob => Blob,
    ColumnType::Custom(_) => Blob,
    ColumnType::Counter => Bigint,
    ColumnType::Boolean => Boolean,
    ColumnType::Timestamp => Timestamp,
    ColumnType::Uuid => Uuid,
    ColumnType::Timeuuid => TimeUuid,
    ColumnType::Double => Double,
    ColumnType::Float => Float,
    ColumnType::Int => Int,
    ColumnType::Decimal => Decimal,
    ColumnType::Varint => Varint,
    ColumnType::Inet => Inet,
    ColumnType::Varchar => Varchar,
    ColumnType::Ascii => Ascii
);

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
        let to_encode = Decimal::new(1, Varint::try_from(vec![0x00, 0x80]).unwrap());
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
    fn list_nested() {
        let to_encode = List::try_from(vec![Some(List::try_from(vec![Some(Varchar::try_from("a").unwrap())])
                                                     .unwrap()),
                                            Some(List::try_from(vec![Some(Varchar::try_from("b").unwrap()),
                                                                     Some(Varchar::try_from("cd").unwrap())])
                                                         .unwrap())])
                .unwrap();
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
        let to_encode = Timestamp::new(12343521);
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn uuid() {
        let to_encode = Uuid::new([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
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
        let to_encode = TimeUuid::new([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn raw_tuple() {
        let to_encode = RawTuple::try_from(vec![Some(vec![0x00, 0x80].into()),
                                                None,
                                                Some(vec![0x00, 0x80].into())])
                .unwrap();
        assert_serialization_deserialization(to_encode);
    }

    #[test]
    fn raw_udt() {
        let to_encode = RawUdt::try_from(vec![Some(vec![0x00, 0x80].into()),
                                              None,
                                              Some(vec![0x00, 0x80].into())])
                .unwrap();
        assert_serialization_deserialization(to_encode);
    }
}
