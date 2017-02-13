use byteorder::{ByteOrder, BigEndian};
use super::{CqlStringList, CqlString, CqlStringMap, CqlStringMultiMap};

pub fn short(v: u16) -> [u8; 2] {
    let mut bytes = [0u8; 2];
    BigEndian::write_u16(&mut bytes[..], v);
    bytes
}

pub fn int(v: i32) -> [u8; 4] {
    let mut bytes = [0u8; 4];
    BigEndian::write_i32(&mut bytes[..], v);
    bytes
}

pub fn long(v: i64) -> [u8; 8] {
    let mut bytes = [0u8; 8];
    BigEndian::write_i64(&mut bytes[..], v);
    bytes
}

pub fn string<T>(s: &CqlString<T>, buf: &mut Vec<u8>)
    where T: AsRef<[u8]> + PartialEq + Eq
{
    buf.extend(&short(s.len())[..]);
    buf.extend(s.as_bytes());
}

pub fn string_list<T>(l: &CqlStringList<T>, buf: &mut Vec<u8>)
    where T: AsRef<[u8]> + PartialEq + Eq
{
    buf.extend(&short(l.len())[..]);
    for s in l.iter() {
        string(s, buf);
    }
}

pub fn string_map<T>(m: &CqlStringMap<T>, buf: &mut Vec<u8>)
    where T: AsRef<[u8]> + PartialEq + Eq
{
    buf.extend(&short(m.len())[..]);
    for (k, v) in m.iter() {
        string(k, buf);
        string(v, buf);
    }
}

pub fn string_multimap<T>(m: &CqlStringMultiMap<T>, buf: &mut Vec<u8>)
    where T: AsRef<[u8]> + PartialEq + Eq
{
    buf.extend(&short(m.len())[..]);
    for (k, lst) in m.iter() {
        string(k, buf);
        string_list(lst, buf);
    }
}
