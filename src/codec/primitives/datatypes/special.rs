use super::*;
use std::fmt::Display;

#[derive(Debug, PartialEq, Clone)]
pub enum Inet {
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr),
}

impl CqlSerializable for Inet {
    fn serialize(&self, buf: &mut BytesMut) {
        match *self {
            Inet::Ipv4(addr) => buf.extend(&addr.octets()[..]),
            Inet::Ipv6(addr) => buf.extend(&addr.octets()[..]),
        }
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        if data.len() < 4 {
            // FIXME: what if we read in chunks? How to see if ipv4 or ipv6
            // Should not be a problem actually since we should never get a
            // chunk here, since we are passing the CqlBytes read
            return Err(ErrorKind::Incomplete.into());
        }

        Ok(match data.len() {
               4 => Inet::Ipv4(Ipv4Addr::from([data[0], data[1], data[2], data[3]])),
               16 => {
                   Inet::Ipv6(Ipv6Addr::from([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                                              data[8], data[9], data[10], data[11], data[12], data[13], data[14],
                                              data[15]]))
               }
               _ => return Err(ErrorKind::Incomplete.into()),
           })
    }

    fn bytes_len(&self) -> BytesLen {
        match *self {
            Inet::Ipv4(_) => 4,
            Inet::Ipv6(_) => 16,
        }
    }
}

impl Display for Inet {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Inet::Ipv4(i) => ::std::fmt::Display::fmt(&i, fmt),
            Inet::Ipv6(i) => ::std::fmt::Display::fmt(&i, fmt),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Timestamp {
    epoch: i64,
}

impl Timestamp {
    pub fn new(epoch: i64) -> Self {
        Timestamp { epoch: epoch }
    }
}

impl CqlSerializable for Timestamp {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.reserve(8);
        buf.put_i64::<BigEndian>(self.epoch);
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        if data.len() != 8 {
            return Err(ErrorKind::Incomplete.into());
        }
        let long = BigEndian::read_i64(data.as_ref());
        Ok(Timestamp { epoch: long })
    }

    fn bytes_len(&self) -> BytesLen {
        8
    }
}

impl Display for Timestamp {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        let naive = ::chrono::naive::datetime::NaiveDateTime::from_timestamp(self.epoch, 0);
        ::std::fmt::Display::fmt(&naive, fmt)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Uuid {
    inner: [u8; 16],
}

impl Uuid {
    pub fn new(data: [u8; 16]) -> Self {
        Uuid { inner: data }
    }
}

impl CqlSerializable for Uuid {
    fn serialize(&self, buf: &mut BytesMut) {
        buf.reserve(16);
        buf.put_slice(&self.inner[..])
    }

    fn deserialize(data: BytesMut) -> Result<Self> {
        if data.len() != 16 {
            return Err(ErrorKind::Incomplete.into());
        }
        let arr = [data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10],
                   data[11], data[12], data[13], data[14], data[15]];
        Ok(Uuid { inner: arr })
    }

    fn bytes_len(&self) -> BytesLen {
        16
    }
}

impl Display for Uuid {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        let s = format!("{:02X}{:02X}{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}\
                        -{:02X}{:02X}-{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}",
                        self.inner[0],
                        self.inner[1],
                        self.inner[2],
                        self.inner[3],
                        self.inner[4],
                        self.inner[5],
                        self.inner[6],
                        self.inner[7],
                        self.inner[8],
                        self.inner[9],
                        self.inner[10],
                        self.inner[11],
                        self.inner[12],
                        self.inner[13],
                        self.inner[14],
                        self.inner[15]);
        ::std::fmt::Display::fmt(&s, fmt)
    }
}

pub type TimeUuid = Uuid;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn inet4_display() {
        let iv4 = Inet::Ipv4(Ipv4Addr::new(127, 0, 0, 1));
        assert_eq!("127.0.0.1", format!("{}", iv4));
    }

    #[test]
    fn inet6_display() {
        let iv6 = Inet::Ipv6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc00a, 0x2ff));
        assert_eq!("::ffff:192.10.2.255", format!("{}", iv6));
    }

    #[test]
    fn timestamp_display() {
        let timestamp = Timestamp::new(1491283495);
        assert_eq!("2017-04-04 05:24:55", format!("{}", timestamp));
    }

    #[test]
    fn uuid_display() {
        let uuid = Uuid::new([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
        assert_eq!("00010203-0405-0607-0809-0A0B0C0D0E0F", format!("{}", uuid));
    }

    #[test]
    fn timeuuid_display() {
        let uuid = TimeUuid::new([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
        assert_eq!("00010203-0405-0607-0809-0A0B0C0D0E0F", format!("{}", uuid));
    }
}
