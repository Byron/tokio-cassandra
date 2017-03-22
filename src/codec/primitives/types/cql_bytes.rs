use super::CqlFrom;
use bytes::BytesMut;


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CqlBytes {
    buf: Option<BytesMut>,
}

impl CqlBytes {
    pub fn from(buf: BytesMut) -> CqlBytes {
        CqlBytes { buf: Some(buf) }
    }

    pub fn as_option(self) -> Option<BytesMut> {
        self.buf
    }

    pub fn len(&self) -> i32 {
        match &self.buf {
            &Some(ref buf) => buf.as_ref().len() as i32,
            &None => -1,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match &self.buf {
            &Some(ref buf) => Some(buf.as_ref()),
            &None => None,
        }
    }

    pub fn null_value() -> CqlBytes {
        CqlBytes { buf: None }
    }
}

impl<'a> CqlFrom<CqlBytes, Vec<u8>> for CqlBytes {
    unsafe fn unchecked_from(vec: Vec<u8>) -> CqlBytes {
        CqlBytes { buf: Some(vec.into()) }
    }

    fn max_len() -> usize {
        i32::max_value() as usize
    }
}

impl<'a> CqlFrom<CqlBytes, BytesMut> for CqlBytes {
    unsafe fn unchecked_from(vec: BytesMut) -> CqlBytes {
        CqlBytes { buf: Some(vec) }
    }

    fn max_len() -> usize {
        i32::max_value() as usize
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::super::super::{encode, decode};
    use bytes::BytesMut;

    #[test]
    fn bytes() {
        let s = CqlBytes::try_from((0u8..10).collect::<Vec<_>>()).unwrap();
        let mut buf = BytesMut::with_capacity(64);
        encode::bytes(&s, &mut buf);

        let buf = Vec::from(&buf[..]).into();
        let res = decode::bytes(buf);
        assert_eq!(res.unwrap().1, s);
    }

    #[test]
    fn null_value() {
        let s = CqlBytes::null_value();
        let mut buf = BytesMut::with_capacity(64);
        encode::bytes(&s, &mut buf);

        let buf = Vec::from(&buf[..]).into();
        let res = decode::bytes(buf);
        assert_eq!(res.unwrap().1, s);
    }

    #[test]
    fn as_option() {
        let s = CqlBytes::null_value();
        assert_eq!(s.as_option(), None);

        let s = CqlBytes::try_from((0u8..10).collect::<Vec<_>>()).unwrap();
        assert!(s.as_option().is_some());
    }
}
