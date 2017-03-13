use super::*;

type InputBuffer = Vec<u8>;

pub fn ascii<T: Buffer>(data: Ascii<T>, buf: &mut InputBuffer) {
    buf.extend(data.bytes.as_ref());
}
