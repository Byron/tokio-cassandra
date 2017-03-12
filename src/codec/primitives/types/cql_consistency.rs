use super::*;

#[derive(Debug, PartialEq, Eq)]
pub enum CqlConsistency {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalQuorum,
    EachQuorum,
    Serial,
    LocalSerial,
    LocalOne,
}

impl CqlConsistency {
    pub fn try_from(short: u16) -> Result<CqlConsistency> {
        Ok(match short {
               0x0000 => CqlConsistency::Any,
               0x0001 => CqlConsistency::One,
               0x0002 => CqlConsistency::Two,
               0x0003 => CqlConsistency::Three,
               0x0004 => CqlConsistency::Quorum,
               0x0005 => CqlConsistency::All,
               0x0006 => CqlConsistency::LocalQuorum,
               0x0007 => CqlConsistency::EachQuorum,
               0x0008 => CqlConsistency::Serial,
               0x0009 => CqlConsistency::LocalSerial,
               0x000A => CqlConsistency::LocalOne,
               _ => return Err("Unknown Consistency".into()),
           })
    }

    pub fn as_short(&self) -> u16 {
        use self::CqlConsistency::*;
        match *self {
            Any => 0x0000,
            One => 0x0001,
            Two => 0x0002,
            Three => 0x0003,
            Quorum => 0x0004,
            All => 0x0005,
            LocalQuorum => 0x0006,
            EachQuorum => 0x0007,
            Serial => 0x0008,
            LocalSerial => 0x0009,
            LocalOne => 0x000A,
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use super::super::super::{encode, decode};

    #[test]
    fn consistency() {
        let c = CqlConsistency::Any;
        let buf = encode::consistency(&c);
        let buf = Vec::from(&buf[..]).into();

        let res = decode::consistency(buf);
        assert_eq!(res.unwrap().1, c);
    }

}
