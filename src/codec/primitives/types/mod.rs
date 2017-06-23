use std::hash::Hash;
use std::collections::HashMap;
use bytes::BytesMut;

mod cql_string;
pub use self::cql_string::*;

mod cql_long_string;
pub use self::cql_long_string::*;

mod cql_bytes;
pub use self::cql_bytes::*;

mod cql_consistency;
pub use self::cql_consistency::*;

mod collections;
pub use self::collections::*;


mod errors {
    error_chain! {
        errors {
            MaximumLengthExceeded(l: usize) {
              description("Too many elements container")
              display("Expected not more than {} elements, got {}.", u16::max_value(), l)
            }
        }
    }
}

pub use self::errors::{Error, ErrorKind, Result};

pub trait CqlFrom<C, V>
where
    V: HasLength,
{
    fn try_from(s: V) -> Result<C> {
        match s.length() > u16::max_value() as usize {
            true => Err(ErrorKind::MaximumLengthExceeded(s.length()).into()),
            false => {
                Ok({
                    unsafe { Self::unchecked_from(s) }
                })
            }
        }
    }
    unsafe fn unchecked_from(s: V) -> C;
    fn max_len() -> usize;
}

pub trait HasLength {
    fn length(&self) -> usize;
}

impl<'a> HasLength for &'a str {
    fn length(&self) -> usize {
        self.len()
    }
}

impl<T> HasLength for Vec<T> {
    fn length(&self) -> usize {
        self.len()
    }
}

impl HasLength for BytesMut {
    fn length(&self) -> usize {
        self.len()
    }
}

impl<T, U> HasLength for HashMap<T, U>
where
    T: ::std::cmp::Eq + Hash,
{
    fn length(&self) -> usize {
        self.len()
    }
}


#[cfg(test)]
mod test {
    use super::{CqlFrom, CqlString, CqlStringList, CqlStringMap, CqlStringMultiMap};
    use super::super::{encode, decode};
    use bytes::BytesMut;

    #[test]
    fn short() {
        let expected: u16 = 342;
        let mut buf = BytesMut::with_capacity(64);
        encode::short(expected, &mut buf);
        let buf = Vec::from(&buf[..]).into();

        let res = decode::short(buf);
        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn int() {
        let expected: i32 = -342;
        let mut buf = BytesMut::with_capacity(64);
        encode::int(expected, &mut buf);
        let buf = Vec::from(&buf[..]).into();

        let res = decode::int(buf);
        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn long() {
        let expected: i64 = -342;
        let mut buf = BytesMut::with_capacity(64);
        encode::long(expected, &mut buf);
        let buf = Vec::from(&buf[..]).into();

        let res = decode::long(buf);
        assert_eq!(res.unwrap().1, expected);
    }



    #[test]
    fn string_list() {
        let sl: Vec<_> = vec!["a", "b"]
            .iter()
            .map(|&s| CqlString::try_from(s))
            .map(Result::unwrap)
            .collect();
        let sl = CqlStringList::try_from(sl).unwrap();

        let mut buf = BytesMut::with_capacity(64);
        encode::string_list(&sl, &mut buf);
        let buf = Vec::from(&buf[..]).into();

        println!("buf = {:?}", buf);
        let res = decode::string_list(buf).unwrap().1;
        assert_eq!(res, sl);
    }

    #[test]
    fn string_map() {
        let sm = CqlStringMap::try_from_iter(vec![
            (cql_string!("a"), cql_string!("av")),
            (cql_string!("a"), cql_string!("av")),
        ]).unwrap();

        let mut buf = BytesMut::with_capacity(64);
        encode::string_map(&sm, &mut buf);
        let buf = Vec::from(&buf[..]).into();

        let res = decode::string_map(buf).unwrap().1;
        assert_eq!(res, sm);
    }

    #[test]
    fn string_multimap() {
        let sla = ["a", "b"];
        let slb = ["c", "d"];
        let csl1 = CqlStringList::try_from_iter_easy(sla.iter().cloned()).unwrap();
        let csl2 = CqlStringList::try_from_iter_easy(slb.iter().cloned()).unwrap();
        let smm = CqlStringMultiMap::try_from_iter(vec![(cql_string!("a"), csl1), (cql_string!("b"), csl2)]).unwrap();

        let mut buf = BytesMut::with_capacity(64);
        encode::string_multimap(&smm, &mut buf);
        let buf = Vec::from(&buf[..]).into();

        let res = decode::string_multimap(buf).unwrap().1;
        assert_eq!(res, smm);
    }
}
