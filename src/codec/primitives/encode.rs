use byteorder::{ByteOrder, BigEndian};
use bytes::BytesMut;
use super::{CqlStringList, CqlLongString, CqlString, CqlBytes, CqlStringMap, CqlStringMultiMap, CqlConsistency};

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

pub fn string(s: &CqlString, buf: &mut BytesMut) {
    buf.extend(&short(s.len())[..]);
    buf.extend(s.as_bytes());
}

pub fn long_string(s: &CqlLongString, buf: &mut BytesMut) {
    buf.extend(&int(s.len())[..]);
    buf.extend(s.as_bytes());
}

pub fn bytes(b: &CqlBytes, buf: &mut BytesMut) {
    buf.extend(&int(b.len())[..]);
    if let Some(b) = b.as_bytes() {
        buf.extend(b);
    }
}

pub fn string_list(l: &CqlStringList, buf: &mut BytesMut) {
    buf.extend(&short(l.len())[..]);
    for s in l.iter() {
        string(s, buf);
    }
}

pub fn string_map(m: &CqlStringMap, buf: &mut BytesMut) {
    buf.extend(&short(m.len())[..]);
    for (k, v) in m.iter() {
        string(k, buf);
        string(v, buf);
    }
}

pub fn string_multimap(m: &CqlStringMultiMap, buf: &mut BytesMut) {
    buf.extend(&short(m.len())[..]);
    for (k, lst) in m.iter() {
        string(k, buf);
        string_list(lst, buf);
    }
}

pub fn consistency(v: &CqlConsistency) -> [u8; 2] {
    let mut bytes = [0u8; 2];
    BigEndian::write_u16(&mut bytes[..], v.as_short());
    bytes
}
