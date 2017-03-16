use std::fmt::{Formatter, Debug};
use super::CqlFrom;
use bytes::BytesMut;
use std::hash::{Hasher, Hash};

#[derive(Clone, PartialEq, Eq)]
pub struct CqlLongString<T>
    where T: AsRef<[u8]>
{
    buf: T,
}

impl<T> Debug for CqlLongString<T>
    where T: AsRef<[u8]>
{
    fn fmt(&self, f: &mut Formatter) -> ::std::result::Result<(), ::std::fmt::Error> {
        self.as_ref().fmt(f)
    }
}

impl<T> AsRef<str> for CqlLongString<T>
    where T: AsRef<[u8]>
{
    fn as_ref(&self) -> &str {
        // FIXME: this is a costly operation - consider unsafe unchecked_from_utf8
        ::std::str::from_utf8(&self.buf.as_ref()).unwrap()
    }
}


impl<T> Hash for CqlLongString<T>
    where T: AsRef<[u8]>
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state)
    }
}


impl CqlLongString<BytesMut> {
    pub fn from(buf: BytesMut) -> CqlLongString<BytesMut> {
        CqlLongString { buf: buf }
    }
}

impl<'a> CqlFrom<CqlLongString<BytesMut>, &'a str> for CqlLongString<BytesMut> {
    unsafe fn unchecked_from(s: &str) -> CqlLongString<BytesMut> {
        let vec = Vec::from(s);
        CqlLongString { buf: vec.into() }
    }

    fn max_len() -> usize {
        i32::max_value() as usize
    }
}

impl<'a> CqlFrom<CqlLongString<Vec<u8>>, &'a str> for CqlLongString<Vec<u8>> {
    unsafe fn unchecked_from(s: &str) -> CqlLongString<Vec<u8>> {
        let vec = Vec::from(s);
        CqlLongString { buf: vec }
    }

    fn max_len() -> usize {
        i32::max_value() as usize
    }
}

impl<T> CqlLongString<T>
    where T: AsRef<[u8]>
{
    pub fn len(&self) -> i32 {
        self.buf.as_ref().len() as i32
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

impl From<CqlLongString<BytesMut>> for CqlLongString<Vec<u8>> {
    fn from(string: CqlLongString<BytesMut>) -> CqlLongString<Vec<u8>> {
        CqlLongString { buf: string.buf.into_iter().collect() }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BytesMut;
    use super::super::super::{encode, decode};

    #[test]
    fn from_bytesmut_into_vec() {
        let a: CqlLongString<BytesMut> = unsafe { CqlLongString::unchecked_from("AbC") };
        let b: CqlLongString<Vec<u8>> = a.into();
        assert_eq!("AbC", b.as_ref());
    }

    #[test]
    fn string() {
        let s = CqlLongString::try_from("Hello üß in a long String").unwrap();
        let mut buf = BytesMut::with_capacity(64);
        encode::long_string(&s, &mut buf);

        let buf = Vec::from(&buf[..]).into();

        println!("buf = {:?}", buf);
        let res = decode::long_string(buf);
        assert_eq!(res.unwrap().1, s);
    }
}
