use super::*;
use std::fmt::{Write, Debug};
use codec::response::{TupleDefinition, UdtDefinition};

// Bounds checking needs to be done in constructor
#[derive(PartialEq, Eq, Clone)]
pub struct List<T: CqlSerializable> {
    inner: Vec<Option<T>>,
}

impl<T: CqlSerializable> TryFrom<Vec<Option<T>>> for List<T> {
    fn try_from(data: Vec<Option<T>>) -> Result<Self> {
        if data.len() > BytesLen::max_value() as usize {
            Err(ErrorKind::MaximumLengthExceeded.into())
        } else {
            Ok(List { inner: data })
        }
    }
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

    fn bytes_len(&self) -> Option<BytesLen> {
        //        Some(self.inner.len() as BytesLen)
        None
    }
}

impl<T: CqlSerializable + Debug> Debug for List<T> {
    // TODO: maybe room for optimization
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {

        let l = self.inner.len();
        let mut i = 1;
        fmt.write_char('{')?;
        for value in &self.inner {
            match value.clone() {
                &Some(ref b) => b.fmt(fmt)?,
                &None => fmt.write_str("NULL")?,
            }

            if i < l {
                fmt.write_str(", ")?;
            }
            i = i + 1;
        }
        fmt.write_char('}')?;
        Ok(())
    }
}

#[cfg(feature = "with-serde")]
impl<T: ::serde::Serialize + CqlSerializable> ::serde::Serialize for List<T> {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        use serde::ser::SerializeSeq;

        let mut seq = serializer.serialize_seq(Some(self.inner.len()))?;
        for e in &self.inner {
            seq.serialize_element(e)?;
        }
        seq.end()
    }
}

// Bounds checking needs to be done in constructor
#[derive(PartialEq, Eq)]
pub struct Map<K, V>
where
    K: CqlSerializable,
    V: CqlSerializable,
{
    // FIXME: is this a good idea to use BytesMut here?
    // FIXME: Option is probably overengineered, since None is semantically the same as not existent here
    inner: HashMap<BytesMut, Option<V>>,
    p: PhantomData<K>,
}

impl<K, V> Debug for Map<K, V>
where
    V: CqlSerializable + Debug,
    K: CqlSerializable + Debug,
{
    fn fmt(&self, fmt: &mut Formatter) -> ::std::fmt::Result {

        let field_len = self.inner.len();
        fmt.write_char('{')?;

        let mut i = 0;
        for (k, v) in &self.inner {
            let key = K::deserialize(k.clone());
            match key {
                Ok(k) => k.fmt(fmt)?,
                Err(_) => fmt.write_str("[ERROR]")?,
            }

            fmt.write_str(": ")?;
            match v.clone() {
                &Some(ref b) => b.fmt(fmt)?,
                &None => fmt.write_str("NULL")?,
            }

            i += 1;
            if i != field_len {
                fmt.write_str(", ")?;
            }
        }
        fmt.write_char('}')
    }
}

impl<K, V> Map<K, V>
where
    K: CqlSerializable,
    V: CqlSerializable,
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
where
    K: CqlSerializable,
    V: CqlSerializable,
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

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(self.inner.len() as BytesLen)
    }
}

#[cfg(feature = "with-serde")]
impl<K, V> ::serde::Serialize for Map<K, V>
where
    K: CqlSerializable + ::serde::ser::Serialize,
    V: CqlSerializable + ::serde::ser::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.inner.len()))?;
        for (k, v) in &self.inner {
            let key = K::deserialize(k.clone()).map_err(|msg| {
                ::serde::ser::Error::custom(format!("{}", msg))
            })?;
            map.serialize_entry(&key, v)?;
        }
        map.end()
    }
}

#[derive(PartialEq, Eq)]
pub struct RawMap {
    inner: Vec<(Option<BytesMut>, Option<BytesMut>)>,
}


impl CqlSerializable for RawMap {
    fn serialize(&self, buf: &mut BytesMut) {
        ::codec::primitives::encode::int(self.inner.len() as BytesLen, buf);

        for &(ref k, ref v) in &self.inner {
            serialize_bytesmut(k, buf);
            serialize_bytesmut(v, buf);
        }
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        let (data, n) = ::codec::primitives::decode::int(data)?;
        let mut vec = Vec::new();
        let mut d = data;
        for _ in 0..n {
            let (data, k) = deserialize_bytesmut(d)?;
            let (data, v) = deserialize_bytesmut(data)?;

            vec.push((k, v));
            d = data;
        }
        Ok(RawMap { inner: vec })
    }

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(self.inner.len() as BytesLen)
    }
}

pub struct GenericMap<'a> {
    inner: RawMap,
    key_type: &'a ColumnType,
    value_type: &'a ColumnType,
}

impl<'a> GenericMap<'a> {
    pub fn new(inner: RawMap, key_type: &'a ColumnType, value_type: &'a ColumnType) -> Self {
        GenericMap {
            inner: inner,
            key_type: key_type,
            value_type: value_type,
        }
    }
}

impl<'a> Debug for GenericMap<'a> {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        let field_len = self.inner.inner.len();
        fmt.write_char('{')?;

        let mut i = 0;
        for &(ref k, ref v) in &self.inner.inner {
            // FIXME: clone() needed?
            super::debug_cell(self.key_type, k.clone(), fmt)?;
            fmt.write_str(": ")?;
            super::debug_cell(self.value_type, v.clone(), fmt)?;

            i += 1;
            if i != field_len {
                fmt.write_str(", ")?;
            }
        }
        fmt.write_char('}')
    }
}

#[cfg(feature = "with-serde")]
impl<'a> ::serde::Serialize for GenericMap<'a> {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.inner.inner.len()))?;
        for &(ref k, ref v) in &self.inner.inner {
            let cell = SerializableCell(self.key_type, k.clone());
            map.serialize_key(&cell)?;

            let cell = SerializableCell(self.value_type, v.clone());
            map.serialize_value(&cell)?;
        }
        map.end()
    }
}

// Bounds checking needs to be done in constructor
#[derive(PartialEq, Eq)]
pub struct Set<V>
where
    V: CqlSerializable,
{
    inner: HashSet<BytesMut>,
    p: PhantomData<V>,
}

impl<V> Debug for Set<V>
where
    V: CqlSerializable + Debug,
{
    fn fmt(&self, fmt: &mut Formatter) -> ::std::fmt::Result {
        fmt.write_char('{')?;
        let mut c = 0;
        for item in &self.inner {
            let v = V::deserialize(item.clone());
            match v {
                Ok(v) => v.fmt(fmt)?,
                Err(_) => fmt.write_str("[ERROR]")?,
            }
            c = c + 1;
            if c < self.inner.len() {
                fmt.write_str(", ")?;
            }
        }
        fmt.write_char('}')?;
        Ok(())
    }
}


impl<V> Set<V>
where
    V: CqlSerializable,
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
where
    V: CqlSerializable,
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

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(self.inner.len() as BytesLen)
    }
}

#[cfg(feature = "with-serde")]
impl<T: ::serde::Serialize + CqlSerializable> ::serde::Serialize for Set<T> {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        use serde::ser::SerializeSeq;

        let mut seq = serializer.serialize_seq(Some(self.inner.len()))?;
        for e in &self.inner {
            let v = T::deserialize(e.clone()).expect("should not fail here");
            seq.serialize_element(&v)?;
        }
        seq.end()
    }
}

// Bounds checking needs to be done in constructor
#[derive(PartialEq, Eq, Debug)]
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

    fn bytes_len(&self) -> Option<BytesLen> {
        Some(self.inner.len() as BytesLen)
    }
}

impl TryFrom<Vec<Option<BytesMut>>> for BytesMutCollection {
    fn try_from(data: Vec<Option<BytesMut>>) -> Result<Self> {
        if data.len() > BytesLen::max_value() as usize {
            Err(ErrorKind::MaximumLengthExceeded.into())
        } else {
            Ok(BytesMutCollection { inner: data })
        }
    }
}

pub type RawTuple = BytesMutCollection;
pub type RawUdt = BytesMutCollection;
pub type RawList = BytesMutCollection;
pub type RawSet = BytesMutCollection;

pub struct Udt<'a> {
    inner: RawUdt,
    def: &'a UdtDefinition,
}

impl<'a> Udt<'a> {
    pub fn new(inner: RawUdt, def: &'a UdtDefinition) -> Self {
        Udt {
            inner: inner,
            def: def,
        }
    }
}

impl<'a> Debug for Udt<'a> {
    fn fmt(&self, fmt: &mut Formatter) -> ::std::fmt::Result {
        let field_len = self.def.fields.len();
        if self.inner.inner.len() != field_len {
            panic!("Inner data fields do not fit to the number of field definitions");
        }

        fmt.write_char('{')?;

        let mut i = 0;
        for bytes in &self.inner.inner {
            let t = &self.def.fields[i];
            // FIXME: clone() needed?
            fmt.write_str(&t.0.as_ref())?;
            fmt.write_str(": ")?;
            super::debug_cell(&t.1, bytes.clone(), fmt)?;
            i += 1;

            if i != field_len {
                fmt.write_str(", ")?;
            }
        }
        fmt.write_char('}')
    }
}

#[cfg(feature = "with-serde")]
impl<'a> ::serde::Serialize for Udt<'a> {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.inner.inner.len()))?;
        let mut i = 0;
        for e in &self.inner.inner {
            let t = &self.def.fields[i];
            map.serialize_key(t.0.as_ref())?;
            let cell = SerializableCell(&t.1, e.clone());
            map.serialize_value(&cell)?;
            i = i + 1;
        }
        map.end()
    }
}

pub struct Tuple<'a> {
    inner: RawTuple,
    def: &'a TupleDefinition,
}

impl<'a> Tuple<'a> {
    pub fn new(inner: RawTuple, def: &'a TupleDefinition) -> Self {
        Tuple {
            inner: inner,
            def: def,
        }
    }
}

impl<'a> Debug for Tuple<'a> {
    fn fmt(&self, fmt: &mut Formatter) -> ::std::fmt::Result {
        let field_len = self.def.0.len();
        if self.inner.inner.len() != field_len {
            panic!("Inner data fields do not fit to the number of field definitions");
        }

        fmt.write_char('(')?;
        let mut i = 0;
        for bytes in &self.inner.inner {
            let t = &self.def.0[i];
            // FIXME: clone() needed?
            super::debug_cell(t, bytes.clone(), fmt)?;
            i += 1;

            if i != field_len {
                fmt.write_str(", ")?;
            }
        }
        fmt.write_char(')')
    }
}

#[cfg(feature = "with-serde")]
impl<'a> ::serde::Serialize for Tuple<'a> {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        use serde::ser::SerializeTuple;
        let mut tuple = serializer.serialize_tuple(self.inner.inner.len())?;
        let mut i = 0;
        for e in &self.inner.inner {
            let t = &self.def.0[i];
            let cell = SerializableCell(&t, e.clone());
            tuple.serialize_element(&cell)?;
            i = i + 1;
        }
        tuple.end()
    }
}

pub struct GenericList<'a> {
    inner: RawList,
    def: &'a ColumnType,
}

impl<'a> GenericList<'a> {
    pub fn new(inner: RawList, def: &'a ColumnType) -> Self {
        GenericList {
            inner: inner,
            def: def,
        }
    }
}

impl<'a> Debug for GenericList<'a> {
    fn fmt(&self, fmt: &mut Formatter) -> ::std::fmt::Result {
        let field_len = self.inner.inner.len();
        fmt.write_char('[')?;

        let mut i = 0;
        for bytes in &self.inner.inner {
            // FIXME: clone() needed?
            super::debug_cell(self.def, bytes.clone(), fmt)?;
            i += 1;

            if i != field_len {
                fmt.write_str(", ")?;
            }
        }
        fmt.write_char(']')
    }
}

#[cfg(feature = "with-serde")]
impl<'a> ::serde::Serialize for GenericList<'a> {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.inner.inner.len()))?;

        for e in &self.inner.inner {
            let cell = SerializableCell(self.def, e.clone());
            seq.serialize_element(&cell)?;
        }
        seq.end()
    }
}

pub type GenericSet<'a> = GenericList<'a>;

#[cfg(test)]
mod test {
    use super::*;
    use codec::response::{ColumnType, UdtField};
    use codec::primitives::{CqlFrom, CqlString};

    #[test]
    fn list_debug() {
        let x = List::try_from(vec![
            Some(Boolean::new(false)),
            Some(Boolean::new(true)),
            None,
        ]).unwrap();
        assert_eq!("{false, true, NULL}", format!("{:?}", x));
    }

    #[test]
    fn genericlist_debug() {
        let ctype = ColumnType::Varchar;
        let x = GenericList::new(
            RawList::try_from(vec![
                Some(vec![0x66, 0x67].into()),
                Some(vec![0x68, 0x69].into()),
                None,
            ]).unwrap(),
            &ctype,
        );
        assert_eq!("[\"fg\", \"hi\", NULL]", format!("{:?}", x));
    }

    #[test]
    fn set_debug() {
        let x = {
            let mut s = Set::new();
            s.insert(Boolean::new(false));
            s.insert(Boolean::new(true));
            s
        };

        assert!("{false, true}" == format!("{:?}", x) || "{true, false}" == format!("{:?}", x));
    }

    #[test]
    fn genericset_debug() {
        let ctype = ColumnType::Varchar;
        let x = GenericSet::new(
            RawSet::try_from(vec![
                Some(vec![0x66, 0x67].into()),
                Some(vec![0x68, 0x69].into()),
                None,
            ]).unwrap(),
            &ctype,
        );
        assert_eq!("[\"fg\", \"hi\", NULL]", format!("{:?}", x));
    }

    #[test]
    fn udt_debug() {
        let udt = RawUdt::try_from(vec![
            Some(vec![0x66, 0x67, 0x68].into()),
            None,
            Some(vec![0x00, 0x00, 0x00, 0x50].into()),
        ]).unwrap();
        let def = UdtDefinition {
            keyspace: cql_string!("ks"),
            name: cql_string!("table1"),
            fields: vec![
                UdtField(cql_string!("eid"), ColumnType::Varchar),
                UdtField(cql_string!("name"), ColumnType::Varchar),
                UdtField(cql_string!("sales"), ColumnType::Int),
            ],
        };

        assert_eq!(
            "{eid: \"fgh\", name: NULL, sales: 80}",
            format!("{:?}", Udt::new(udt, &def))
        );
    }

    #[test]
    fn tuple_debug() {
        let tuple = RawTuple::try_from(vec![
            Some(vec![0x66, 0x67, 0x68].into()),
            None,
            Some(vec![0x00, 0x00, 0x00, 0x50].into()),
        ]).unwrap();
        let def = TupleDefinition(vec![
            ColumnType::Varchar,
            ColumnType::Varchar,
            ColumnType::Int,
        ]);

        assert_eq!(
            "(\"fgh\", NULL, 80)",
            format!("{:?}", Tuple::new(tuple, &def))
        );
    }

    #[test]
    fn map_debug() {
        let m = {
            let mut map: Map<Int, Varchar> = Map::new();
            map.insert(Int::new(1), Some(Varchar::try_from("fg").unwrap()));
            map.insert(Int::new(2), Some(Varchar::try_from("hi").unwrap()));
            map
        };
        assert!("{1: \"fg\", 2: \"hi\"}" == format!("{:?}", m) || "{2: \"hi\", 1: \"fg\"}" == format!("{:?}", m));
    }

    #[test]
    fn genericmap_debug() {
        let rm = RawMap {
            inner: vec![
                (
                    Some(vec![0x00, 0x00, 0x00, 0x01].into()),
                    Some(vec![0x66, 0x67].into())
                ),
                (
                    Some(vec![0x00, 0x00, 0x00, 0x02].into()),
                    Some(vec![0x68, 0x69].into())
                ),
                (
                    Some(vec![0x00, 0x00, 0x00, 0x03].into()),
                    Some(vec![0x6a, 0x6b].into())
                ),
            ],
        };
        let (kt, vt) = (ColumnType::Int, ColumnType::Varchar);
        let gm = GenericMap::new(rm, &kt, &vt);
        assert_eq!("{1: \"fg\", 2: \"hi\", 3: \"jk\"}", format!("{:?}", gm));
    }
}

#[cfg(feature = "with-serde")]
#[cfg(test)]
mod serde_testing {
    use super::*;
    use std::panic;
    use codec::response::{ColumnType, UdtField};
    use codec::primitives::{CqlFrom, CqlString};

    extern crate serde_test;

    use self::serde_test::{Token, assert_ser_tokens};

    // TODO: Map, GenericMap

    #[test]
    fn list_serde() {
        let x = List::try_from(vec![
            Some(Boolean::new(false)),
            Some(Boolean::new(true)),
            None,
        ]).unwrap();
        assert_ser_tokens(
            &x,
            &[
                Token::Seq { len: Some(3) },
                Token::Some,
                Token::Bool(false),
                Token::Some,
                Token::Bool(true),
                Token::None,
                Token::SeqEnd,
            ],
        );
    }

    #[test]
    fn genericlist_serde() {
        let ctype = ColumnType::Varchar;
        let x = GenericList::new(
            RawList::try_from(vec![
                Some(vec![0x66, 0x67].into()),
                Some(vec![0x68, 0x69].into()),
                None,
            ]).unwrap(),
            &ctype,
        );
        assert_ser_tokens(
            &x,
            &[
                Token::Seq { len: Some(3) },
                Token::Some,
                Token::Str("fg"),
                Token::Some,
                Token::Str("hi"),
                Token::None,
                Token::SeqEnd,
            ],
        );
    }

    #[test]
    fn genericset_serde() {
        let ctype = ColumnType::Varchar;
        let x = GenericSet::new(
            RawSet::try_from(vec![
                Some(vec![0x66, 0x67].into()),
                Some(vec![0x68, 0x69].into()),
                None,
            ]).unwrap(),
            &ctype,
        );

        assert_ser_tokens(
            &x,
            &[
                Token::Seq { len: Some(3) },
                Token::Some,
                Token::Str("fg"),
                Token::Some,
                Token::Str("hi"),
                Token::None,
                Token::SeqEnd,
            ],
        );
    }

    #[test]
    fn set_serde() {
        let x = {
            let mut s = Set::new();
            s.insert(Boolean::new(false));
            s.insert(Boolean::new(true));
            s
        };

        let r1 = panic::catch_unwind(|| {
            assert_ser_tokens(
                &x,
                &[
                    Token::Seq { len: Some(2) },
                    Token::Bool(false),
                    Token::Bool(true),
                    Token::SeqEnd,
                ],
            );
        });

        let r2 = panic::catch_unwind(|| {
            assert_ser_tokens(
                &x,
                &[
                    Token::Seq { len: Some(2) },
                    Token::Bool(true),
                    Token::Bool(false),
                    Token::SeqEnd,
                ],
            );
        });

        assert!(r1.is_ok() || r2.is_ok());



        #[test]
        fn udt_serde() {
            let udt = RawUdt::try_from(vec![
                Some(vec![0x66, 0x67, 0x68].into()),
                None,
                Some(vec![0x00, 0x00, 0x00, 0x50].into()),
            ]).unwrap();
            let def = UdtDefinition {
                keyspace: cql_string!("ks"),
                name: cql_string!("table1"),
                fields: vec![
                    UdtField(cql_string!("eid"), ColumnType::Varchar),
                    UdtField(cql_string!("name"), ColumnType::Varchar),
                    UdtField(cql_string!("sales"), ColumnType::Int),
                ],
            };

            assert_ser_tokens(
                &Udt::new(udt, &def),
                &[
                    Token::Map { len: Some(3) },

                    Token::Str("eid"),
                    Token::Some,
                    Token::Str("fgh"),

                    Token::Str("name"),
                    Token::None,

                    Token::Str("sales"),
                    Token::Some,
                    Token::I32(80),

                    Token::MapEnd,
                ],
            );
        }

        #[test]
        fn tuple_serde() {
            let tuple = RawTuple::try_from(vec![
                Some(vec![0x66, 0x67, 0x68].into()),
                None,
                Some(vec![0x00, 0x00, 0x00, 0x50].into()),
            ]).unwrap();
            let def = TupleDefinition(vec![
                ColumnType::Varchar,
                ColumnType::Varchar,
                ColumnType::Int,
            ]);

            assert_ser_tokens(
                &Tuple::new(tuple, &def),
                &[
                    Token::Tuple { len: 3 },
                    Token::Some,
                    Token::Str("fgh"),
                    Token::None,
                    Token::Some,
                    Token::I32(80),
                    Token::TupleEnd,
                ],
            );
        }

        #[test]
        fn map_serde() {
            let m = {
                let mut map: Map<Int, Varchar> = Map::new();
                map.insert(Int::new(1), Some(Varchar::try_from("fg").unwrap()));
                map.insert(Int::new(2), Some(Varchar::try_from("hi").unwrap()));
                map
            };

            let r1 = panic::catch_unwind(|| {
                assert_ser_tokens(
                    &m,
                    &[
                        Token::Map { len: Some(2) },

                        Token::I32(1),
                        Token::Some,
                        Token::Str("fg"),

                        Token::I32(2),
                        Token::Some,
                        Token::Str("hi"),

                        Token::MapEnd,
                    ],
                );
            });

            let r2 = panic::catch_unwind(|| {
                assert_ser_tokens(
                    &m,
                    &[
                        Token::Map { len: Some(2) },

                        Token::I32(2),
                        Token::Some,
                        Token::Str("hi"),

                        Token::I32(1),
                        Token::Some,
                        Token::Str("fg"),

                        Token::MapEnd,
                    ],
                );
            });

            assert!(r1.is_ok() || r2.is_ok());
        }

        #[test]
        fn genericmap_serde() {
            let rm = RawMap {
                inner: vec![
                    (
                        Some(vec![0x00, 0x00, 0x00, 0x01].into()),
                        Some(vec![0x66, 0x67].into())
                    ),
                    (
                        Some(vec![0x00, 0x00, 0x00, 0x02].into()),
                        Some(vec![0x68, 0x69].into())
                    ),
                    (
                        Some(vec![0x00, 0x00, 0x00, 0x03].into()),
                        Some(vec![0x6a, 0x6b].into())
                    ),
                ],
            };
            let (kt, vt) = (ColumnType::Int, ColumnType::Varchar);
            let gm = GenericMap::new(rm, &kt, &vt);
            assert_ser_tokens(
                &gm,
                &[
                    Token::Map { len: Some(3) },

                    Token::Some,
                    Token::I32(1),
                    Token::Some,
                    Token::Str("fg"),

                    Token::Some,
                    Token::I32(2),
                    Token::Some,
                    Token::Str("hi"),

                    Token::Some,
                    Token::I32(3),
                    Token::Some,
                    Token::Str("jk"),

                    Token::MapEnd,
                ],
            );
        }
    }
}
