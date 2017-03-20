use bytes::{BufMut, BytesMut, BigEndian};
use super::{CqlStringList, CqlLongString, CqlString, CqlBytes, CqlStringMap, CqlStringMultiMap, CqlConsistency};

pub fn short(v: u16, buf: &mut BytesMut) {
    buf.reserve(2);
    buf.put_u16::<BigEndian>(v);
}

pub fn int(v: i32, buf: &mut BytesMut) {
    buf.reserve(4);
    buf.put_i32::<BigEndian>(v);
}

pub fn long(v: i64, buf: &mut BytesMut) {
    buf.reserve(8);
    buf.put_i64::<BigEndian>(v);
}

pub fn string(s: &CqlString, buf: &mut BytesMut) {
    short(s.len(), buf);
    buf.extend(s.as_bytes());
}

pub fn long_string(s: &CqlLongString, buf: &mut BytesMut) {
    int(s.len(), buf);
    buf.extend(s.as_bytes());
}

pub fn bytes(b: &CqlBytes, buf: &mut BytesMut) {
    int(b.len(), buf);
    if let Some(b) = b.as_bytes() {
        buf.extend(b);
    }
}

pub fn string_list(l: &CqlStringList, buf: &mut BytesMut) {
    short(l.len(), buf);
    for s in l.iter() {
        string(s, buf);
    }
}

pub fn string_map(m: &CqlStringMap, buf: &mut BytesMut) {
    short(m.len(), buf);
    for (k, v) in m.iter() {
        string(k, buf);
        string(v, buf);
    }
}

pub fn string_multimap(m: &CqlStringMultiMap, buf: &mut BytesMut) {
    short(m.len(), buf);
    for (k, lst) in m.iter() {
        string(k, buf);
        string_list(lst, buf);
    }
}

pub fn consistency(v: &CqlConsistency, buf: &mut BytesMut) {
    short(v.as_short(), buf);
}
