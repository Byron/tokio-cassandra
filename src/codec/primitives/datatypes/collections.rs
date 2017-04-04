use super::*;
use std::fmt::Display;

// Bounds checking needs to be done in constructor
#[derive(Debug, PartialEq, Eq, Clone)]
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

    fn bytes_len(&self) -> BytesLen {
        self.inner.len() as BytesLen
    }
}

impl<T: CqlSerializable + Display> Display for List<T> {
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

// Bounds checking needs to be done in constructor
#[derive(PartialEq, Eq, Debug)]
pub struct Map<K, V>
    where K: CqlSerializable,
          V: CqlSerializable
{
    //    FIXME: is this a good idea to use BytesMut here?
    inner: HashMap<BytesMut, Option<V>>,
    p: PhantomData<K>,
}

impl<K, V> Display for Map<K, V>
    where V: CqlSerializable + Display,
          K: CqlSerializable + Display
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
#[derive(PartialEq, Eq, Debug)]
pub struct Set<V>
    where V: CqlSerializable
{
    inner: HashSet<BytesMut>,
    p: PhantomData<V>,
}

impl<V> Display for Set<V>
    where V: CqlSerializable + Display
{
    fn fmt(&self, fmt: &mut Formatter) -> ::std::fmt::Result {
        fmt.write_char('{')?;
        for item in &self.inner {
            let v = V::deserialize(item.clone());
            match v {
                Ok(v) => v.fmt(fmt)?,
                Err(_) => fmt.write_str("[ERROR]")?,
            }
            fmt.write_char(',')?;
        }
        fmt.write_char('}')?;
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

impl TryFrom<Vec<Option<BytesMut>>> for BytesMutCollection {
    fn try_from(data: Vec<Option<BytesMut>>) -> Result<Self> {
        if data.len() > BytesLen::max_value() as usize {
            Err(ErrorKind::MaximumLengthExceeded.into())
        } else {
            Ok(BytesMutCollection { inner: data })
        }
    }
}

impl Display for BytesMutCollection {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        fmt.write_str("BytesMutCollection")
    }
}

pub type Tuple = BytesMutCollection;
pub type Udt = BytesMutCollection;

// TODO: just an idea
//pub type RawList = BytesMutCollection;
//pub type RawSet = BytesMutCollection;

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn list_display() {
        let x = List::try_from(vec![Some(Boolean::new(false)), Some(Boolean::new(true)), None]).unwrap();
        assert_eq!("{false, true, NULL}", format!("{}", x));
    }

    //    TODO: display test for others

}
