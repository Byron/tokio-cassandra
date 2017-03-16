use std::fmt::{Formatter, Debug};
use super::CqlFrom;
use std::hash::{Hasher, Hash};
use bytes::BytesMut;

#[derive(Clone, PartialEq, Eq)]
pub struct CqlString<T>
    where T: AsRef<[u8]>
{
    buf: T,
}

impl<T> Debug for CqlString<T>
    where T: AsRef<[u8]>
{
    fn fmt(&self, f: &mut Formatter) -> ::std::result::Result<(), ::std::fmt::Error> {
        self.as_ref().fmt(f)
    }
}

impl<T> AsRef<str> for CqlString<T>
    where T: AsRef<[u8]>
{
    fn as_ref(&self) -> &str {
        ::std::str::from_utf8(&self.buf.as_ref()).unwrap()
    }
}

impl<T> Into<String> for CqlString<T>
    where T: AsRef<[u8]>
{
    fn into(self) -> String {
        String::from(self.as_ref())
    }
}


impl<T> Hash for CqlString<T>
    where T: AsRef<[u8]>
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state)
    }
}


impl CqlString<BytesMut> {
    pub fn from(buf: BytesMut) -> CqlString<BytesMut> {
        CqlString { buf: buf }
    }
}

impl<'a> CqlFrom<CqlString<BytesMut>, &'a str> for CqlString<BytesMut> {
    unsafe fn unchecked_from(s: &str) -> CqlString<BytesMut> {
        let vec = Vec::from(s);
        CqlString { buf: vec.into() }
    }

    fn max_len() -> usize {
        u16::max_value() as usize
    }
}

impl<'a> CqlFrom<CqlString<Vec<u8>>, &'a str> for CqlString<Vec<u8>> {
    unsafe fn unchecked_from(s: &str) -> CqlString<Vec<u8>> {
        let vec = Vec::from(s);
        CqlString { buf: vec }
    }

    fn max_len() -> usize {
        u16::max_value() as usize
    }
}

impl<T> CqlString<T>
    where T: AsRef<[u8]>
{
    pub fn len(&self) -> u16 {
        self.buf.as_ref().len() as u16
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

impl From<CqlString<BytesMut>> for CqlString<Vec<u8>> {
    fn from(string: CqlString<BytesMut>) -> CqlString<Vec<u8>> {
        CqlString { buf: string.buf.into_iter().collect() }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BytesMut;
    use super::super::super::{encode, decode};

    #[test]
    fn from_bytesmut_into_vec() {
        let a: CqlString<BytesMut> = unsafe { CqlString::unchecked_from("AbC") };
        let b: CqlString<Vec<u8>> = a.into();
        assert_eq!("AbC", b.as_ref());
    }

    #[test]
    fn string() {
        let s = cql_string!("Hello üß");
        let mut buf = BytesMut::with_capacity(10);
        encode::string(&s, &mut buf);

        let buf = Vec::from(&buf[..]).into();

        println!("buf = {:?}", buf);
        let res = decode::string(buf);
        assert_eq!(res.unwrap().1, s);
    }
}
