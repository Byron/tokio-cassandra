use super::*;

use byteorder::{ByteOrder, BigEndian};
use std::io::Write;

type InputBuffer = Vec<u8>;

pub fn ascii<T: Buffer>(data: Ascii<T>, buf: &mut InputBuffer) {
    buf.extend(data.bytes.as_ref());
}

pub fn bigint(data: Bigint, buf: &mut InputBuffer) {
    let mut bytes = [0u8; 8];
    BigEndian::write_i64(&mut bytes[..], data.inner);
    buf.write(&bytes).expect("should not fail");
}

pub fn blob<T: Buffer>(data: Blob<T>, buf: &mut InputBuffer) {
    buf.extend(data.bytes.as_ref());
}


pub fn boolean(data: Boolean, buf: &mut InputBuffer) {
    if data.inner {
        buf.push(0x01);
    } else {
        buf.push(0x00);
    }
}
