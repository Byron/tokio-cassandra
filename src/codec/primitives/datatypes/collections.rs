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
        fmt.write_char('{');
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
        fmt.write_char('}');
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn list_display() {
        let x = List::try_from(vec![Some(Boolean::new(false)), Some(Boolean::new(true)), None]).unwrap();
        assert_eq!("{false, true, NULL}", format!("{}", x));
    }

}
