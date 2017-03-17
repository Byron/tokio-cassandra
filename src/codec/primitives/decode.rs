use codec::primitives::types::{CqlStringList, CqlString, CqlLongString, CqlStringMap, CqlStringMultiMap, CqlBytes,
                               CqlConsistency};
use std::collections::HashMap;
use bytes::BytesMut;
use byteorder::{ByteOrder, BigEndian};
use codec::primitives::CqlFrom;

#[derive(Debug,PartialEq,Eq,Clone,Copy)]
pub enum Needed {
    /// needs more data, but we do not know how much
    Unknown,
    /// contains the total required data size, as opposed to the size still needed
    Size(usize),
}


// Maybe replace by error_chain
quick_error! {
    #[derive(Debug, PartialEq, Eq, Clone)]
    pub enum Error {
        Incomplete(n: Needed) {
            description("Unsufficient bytes")
            display("Buffer contains unsufficient bytes, needed {:?}", n)
        }
        ParseError(e: String) {
            description("Parsing Error")
            display("Error during parsing {:?}", e)
        }
    }
}

use self::Error::*;
use self::Needed::*;

pub type ParseResult<T> = Result<(BytesMut, T), Error>;

pub fn short(mut i: BytesMut) -> ParseResult<u16> {
    if i.len() < 2 {
        return Err(Incomplete(Size(2)));
    }
    let databuf = i.split_to(2);
    // TODO: use bytes crate directly with bytesorder!!!!
    let short = BigEndian::read_u16(databuf.as_ref());
    Ok((i, short))
}

pub fn int(mut i: BytesMut) -> ParseResult<i32> {
    if i.len() < 4 {
        return Err(Incomplete(Size(4)));
    }
    let databuf = i.split_to(4);
    let int = BigEndian::read_i32(databuf.as_ref());
    Ok((i, int))
}

pub fn long(mut i: BytesMut) -> ParseResult<i64> {
    if i.len() < 8 {
        return Err(Incomplete(Size(8)));
    }
    let databuf = i.split_to(8);
    let long = BigEndian::read_i64(databuf.as_ref());
    Ok((i, long))
}

pub fn string(buf: BytesMut) -> ParseResult<CqlString> {
    let (mut buf, len) = short(buf)?;
    if buf.len() < len as usize {
        return Err(Incomplete(Size(len as usize)));
    }
    let str = CqlString::from(buf.split_to(len as usize));
    Ok((buf, str))
}

pub fn long_string(buf: BytesMut) -> ParseResult<CqlLongString> {
    let (mut buf, len) = int(buf)?;
    if buf.len() < len as usize {
        return Err(Incomplete(Size(len as usize)));
    }
    let str = CqlLongString::from(buf.split_to(len as usize));
    Ok((buf, str))
}

pub fn bytes(buf: BytesMut) -> ParseResult<CqlBytes> {
    let (mut buf, len) = int(buf)?;
    if (buf.len() as isize) < len as isize {
        return Err(Incomplete(Size(len as usize)));
    } else if len < 0 {
        return Ok((buf, CqlBytes::null_value()));
    }
    let b = CqlBytes::from(buf.split_to(len as usize));
    Ok((buf, b))
}

pub fn string_list(i: BytesMut) -> ParseResult<CqlStringList> {
    let (mut buf, len) = short(i)?;
    let mut v = Vec::new();
    for _ in 0..len {
        let (nb, str) = string(buf)?;
        buf = nb;
        v.push(str);
    }
    let lst = unsafe { CqlStringList::unchecked_from(v) };
    Ok((buf, lst))
}

pub fn string_map(i: BytesMut) -> ParseResult<CqlStringMap> {
    let (mut buf, len) = short(i)?;
    let mut map = HashMap::new();

    for _ in 0..len {
        let (nb, key) = string(buf)?;
        buf = nb;
        let (nb, value) = string(buf)?;
        buf = nb;
        map.insert(key, value);
    }

    Ok((buf, unsafe { CqlStringMap::unchecked_from(map) }))
}

pub fn string_multimap(i: BytesMut) -> ParseResult<CqlStringMultiMap> {
    let (mut buf, len) = short(i)?;
    let mut map = HashMap::new();

    for _ in 0..len {
        let (nb, key) = string(buf)?;
        buf = nb;
        let (nb, value) = string_list(buf)?;
        buf = nb;
        map.insert(key, value);
    }

    Ok((buf, unsafe { CqlStringMultiMap::unchecked_from(map) }))
}

pub fn consistency(mut i: BytesMut) -> ParseResult<CqlConsistency> {
    if i.len() < 2 {
        return Err(Incomplete(Size(2)));
    }
    let databuf = i.split_to(2);
    let short = BigEndian::read_u16(databuf.as_ref());
    let c = CqlConsistency::try_from(short).map_err(|e| ParseError(format!("{}", e)))?;
    Ok((i, c))
}

mod test {
    // TODO: figure out why it doesn't get it!
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use super::super::encode;

    #[test]
    fn short_incomplete() {
        assert_eq!(short(vec![0].into()).unwrap_err(), Incomplete(Size(2)));
    }
    // TODO: fix test again
    //    #[test]
    //    fn short_complete() {
    //        use std::ops::DerefMut;
    //        let mut b: BytesMut = vec![0u8, 1, 2, 3, 4].into();
    //        let b2 = b.clone();
    //        let expected = 16723;
    //        BigEndian::write_u16(&mut b.get_mut().deref_mut(), expected);
    //        let (nb, res) = short(b).unwrap();
    //        assert_eq!(res, expected);
    //        assert_eq!(nb.as_slice(), &b2.as_slice()[2..]);
    //    }

    #[test]
    fn int_incomplete() {
        assert_eq!(int(vec![0].into()).unwrap_err(), Incomplete(Size(4)));
    }

    // TODO: fix test again
    //    #[test]
    //    fn int_complete() {
    //        use std::ops::DerefMut;
    //        let mut b: BytesMut = vec![0u8, 1, 2, 3, 4].into();
    //        let b2 = b.clone();
    //        let expected = -16723;
    //        BigEndian::write_i32(&mut b.get_mut().deref_mut(), expected);
    //        let (nb, res) = int(b).unwrap();
    //        assert_eq!(res, expected);
    //        assert_eq!(nb.as_slice(), &b2.as_slice()[4..]);
    //    }

    #[test]
    fn long_incomplete() {
        assert_eq!(long(vec![0].into()).unwrap_err(), Incomplete(Size(8)));
    }

    // TODO: fix test again
    //    #[test]
    //    fn long_complete() {
    //        use std::ops::DerefMut;
    //        let mut b: BytesMut = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8].into();
    //        let b2 = b.clone();
    //        let expected = -16723;
    //        BigEndian::write_i64(&mut b.get_mut().deref_mut(), expected);
    //        let (nb, res) = long(b).unwrap();
    //        assert_eq!(res, expected);
    //        assert_eq!(nb.as_slice(), &b2.as_slice()[8..]);
    //    }

    #[test]
    fn string_complete() {
        let s = CqlString::try_from("hello").unwrap();
        let mut b = BytesMut::with_capacity(64);
        encode::string(&s, &mut b);
        b.extend(0..2);
        println!("b = {:?}", b);
        let (b, str) = string(b).unwrap();
        assert_eq!(b.len(), 2);
        assert_eq!(str, s);
    }

    #[test]
    fn string_incomplete() {
        let s: CqlString = CqlString::try_from("hello").unwrap();
        let mut b = BytesMut::with_capacity(64);
        encode::string(&s, &mut b);
        let e: BytesMut = b.into();

        assert_eq!(string(e.clone().split_to(1)).unwrap_err(),
                   Incomplete(Size(2)));
        assert_eq!(string(e.clone().split_to(3)).unwrap_err(),
                   Incomplete(Size(5)));
    }

    // TODO: move tests from types here, cause it seems very similar
    //
    //    #[test]
    //    fn string_list_incomplete_and_complete() {
    //        let vs = vec!["hello", "world"];
    //        let v = borrowed::CqlStringList::try_from_iter(vs.iter().cloned()).unwrap();
    //        let ofs = 5usize;
    //        let mut b: Vec<_> = (0u8..ofs as u8).collect();
    //        encode::string_list(&v, &mut b);
    //        let sls = b.len() - ofs;
    //        b.extend(1..2);
    //
    //        assert_eq!(string_list(ofs, &b[ofs..ofs + 1]), Err(Incomplete(Size(2))));
    //        assert_eq!(string_list(ofs, &b[ofs..ofs + 2]), Err(Incomplete(Unknown)));
    //        assert_eq!(string_list(ofs, &b[ofs..]),
    //        Ok((&b[ofs + sls..],
    //            indexed::CqlStringList {
    //                at: ofs + 2,
    //                len: 2,
    //            })));
    //    }
}
