use super::CqlFrom;
use bytes::BytesMut;
use std::hash::{Hasher, Hash};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CqlLongString {
    buf: BytesMut,
}

impl AsRef<str> for CqlLongString {
    fn as_ref(&self) -> &str {
        // FIXME: this is a costly operation - consider unsafe unchecked_from_utf8
        unsafe { ::std::str::from_utf8_unchecked(&self.buf.as_ref()) }
    }
}


impl Hash for CqlLongString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state)
    }
}


impl CqlLongString {
    pub fn from(buf: BytesMut) -> CqlLongString {
        CqlLongString { buf: buf }
    }
}

impl<'a> CqlFrom<CqlLongString, &'a str> for CqlLongString {
    unsafe fn unchecked_from(s: &str) -> CqlLongString {
        let vec = Vec::from(s);
        CqlLongString { buf: vec.into() }
    }

    fn max_len() -> usize {
        i32::max_value() as usize
    }
}

impl CqlLongString {
    pub fn len(&self) -> i32 {
        self.buf.as_ref().len() as i32
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
        let s = CqlLongString::try_from("Hello üß in a long String").unwrap();
        let mut buf = BytesMut::with_capacity(64);
        encode::long_string(&s, &mut buf);

        let buf = Vec::from(&buf[..]).into();

        let res = decode::long_string(buf);
        assert_eq!(res.unwrap().1, s);
    }
}
