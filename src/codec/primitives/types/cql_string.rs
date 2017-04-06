use super::CqlFrom;
use std::hash::{Hasher, Hash};
use bytes::BytesMut;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CqlString {
    buf: BytesMut,
}

impl AsRef<str> for CqlString {
    fn as_ref(&self) -> &str {
        ::std::str::from_utf8(&self.buf.as_ref()).unwrap()
    }
}

impl Display for CqlString {
    fn fmt(&self, fmt: &mut Formatter) -> ::std::fmt::Result {
        fmt.write_str(self.as_ref())
    }
}

impl Into<String> for CqlString {
    fn into(self) -> String {
        String::from(self.as_ref())
    }
}

impl Hash for CqlString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state)
    }
}

impl CqlString {
    pub fn from(buf: BytesMut) -> CqlString {
        CqlString { buf: buf }
    }
}

impl<'a> CqlFrom<CqlString, &'a str> for CqlString {
    unsafe fn unchecked_from(s: &str) -> CqlString {
        let vec = Vec::from(s);
        CqlString { buf: vec.into() }
    }

    fn max_len() -> usize {
        u16::max_value() as usize
    }
}

impl CqlString {
    pub fn len(&self) -> u16 {
        self.buf.as_ref().len() as u16
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BytesMut;
    use super::super::super::{encode, decode};

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
