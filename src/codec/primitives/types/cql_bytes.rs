use std::fmt::{Formatter, Debug};
use super::CqlFrom;
use tokio_core::io::EasyBuf;


#[derive(Clone, PartialEq, Eq)]
pub struct CqlBytes<T>
    where T: AsRef<[u8]>
{
    buf: Option<T>,
}

impl<T> Debug for CqlBytes<T>
    where T: AsRef<[u8]> + Debug
{
    fn fmt(&self, f: &mut Formatter) -> ::std::result::Result<(), ::std::fmt::Error> {
        self.buf.fmt(f)
    }
}

impl CqlBytes<::tokio_core::io::EasyBuf> {
    pub fn from(buf: ::tokio_core::io::EasyBuf) -> CqlBytes<::tokio_core::io::EasyBuf> {
        CqlBytes { buf: Some(buf) }
    }

    pub fn buffer(self) -> Option<EasyBuf> {
        self.buf
    }
}

impl<'a> CqlFrom<CqlBytes<EasyBuf>, Vec<u8>> for CqlBytes<EasyBuf> {
    unsafe fn unchecked_from(vec: Vec<u8>) -> CqlBytes<EasyBuf> {
        CqlBytes { buf: Some(vec.into()) }
    }

    fn max_len() -> usize {
        i32::max_value() as usize
    }
}

impl<'a> CqlFrom<CqlBytes<Vec<u8>>, Vec<u8>> for CqlBytes<Vec<u8>> {
    unsafe fn unchecked_from(vec: Vec<u8>) -> CqlBytes<Vec<u8>> {
        CqlBytes { buf: Some(vec) }
    }

    fn max_len() -> usize {
        i32::max_value() as usize
    }
}

impl<T> CqlBytes<T>
    where T: AsRef<[u8]>
{
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

    pub fn null_value() -> CqlBytes<T> {
        CqlBytes { buf: None }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::super::super::{encode, decode};

    #[test]
    fn bytes() {
        let s = CqlBytes::try_from((0u8..10).collect::<Vec<_>>().into()).unwrap();
        let mut buf = Vec::new();
        encode::bytes(&s, &mut buf);

        let buf = Vec::from(&buf[..]).into();
        let res = decode::bytes(buf);
        assert_eq!(res.unwrap().1, s);
    }

    #[test]
    fn null_value() {
        let s = CqlBytes::null_value();
        let mut buf = Vec::new();
        encode::bytes(&s, &mut buf);

        let buf = Vec::from(&buf[..]).into();
        let res = decode::bytes(buf);
        assert_eq!(res.unwrap().1, s);
    }
}
