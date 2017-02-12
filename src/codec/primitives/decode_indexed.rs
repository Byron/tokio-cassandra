use super::indexed;


#[derive(Debug,PartialEq,Eq,Clone,Copy)]
pub enum Needed {
    /// needs more data, but we do not know how much
    Unknown,
    /// contains the total required data size, as opposed to the size still needed
    Size(usize),
}

quick_error! {
    #[derive(Debug, PartialEq, Eq, Clone)]
    pub enum Error {
        Incomplete(n: Needed) {
            description("Unsufficient bytes")
            display("Buffer contains unsufficient bytes, needed {:?}", n)
        }
    }
}

pub type ParseResult<'a, T> = Result<(&'a [u8], T), Error>;

use self::Error::*;
use self::Needed::*;


pub fn short(i: &[u8]) -> ParseResult<u16> {
    if i.len() < 2 {
        return Err(Incomplete(Size(2)));
    }
    let res = ((i[0] as u16) << 8) + i[1] as u16;
    Ok((&i[2..], res))
}

#[cfg(test)]
mod test {
    use super::*;
    use byteorder::{ByteOrder, BigEndian};

    # [test]
    fn short_incomplete() {
        assert_eq!(short(&[0]), Err(Error::Incomplete(Needed::Size(2))));
    }

    #[test]
    fn short_complete() {
        let mut b = [0u8, 1, 2, 3, 4];
        let expected = 16723;
        BigEndian::write_u16(&mut b, expected);
        assert_eq!(short(&b[..]), Ok((&b[2..], expected)));
    }
}
