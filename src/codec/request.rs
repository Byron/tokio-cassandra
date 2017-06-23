use codec::header::{ProtocolVersion, OpCode, Header, Version};
use std::collections::HashMap;

use codec::primitives::{CqlConsistency, CqlFrom, CqlStringMap, CqlString, CqlBytes, CqlLongString};
use codec::primitives::encode;
use bytes::{BufMut, BytesMut};

mod errors {
    error_chain! {
        foreign_links {
            Io(::std::io::Error);
            HeaderError(::codec::header::Error);
            PrimitiveError(::codec::primitives::Error);
        }
        errors {
            BodyLengthExceeded(len: usize) {
                description("The length of the body exceeded the \
                maximum length specified by the protocol")
                display("The current body length {} exceeded the \
                maximum allowed length for a body", len)
            }
        }
    }
}

pub use self::errors::{Error, ErrorKind, Result};

pub trait CqlEncode {
    fn encode(&self, v: ProtocolVersion, f: &mut BytesMut) -> Result<usize>;
}

#[derive(Debug)]
pub enum Message {
    Options,
    Startup(StartupMessage),
    AuthResponse(AuthResponseMessage),
    Query(QueryMessage),
}

#[derive(Debug)]
pub struct StartupMessage {
    pub cql_version: CqlString,
    pub compression: Option<CqlString>,
}

impl CqlEncode for StartupMessage {
    fn encode(&self, _v: ProtocolVersion, buf: &mut BytesMut) -> Result<usize> {
        use codec::primitives::CqlFrom;

        let mut sm: HashMap<CqlString, CqlString> = HashMap::new();
        sm.insert(
            unsafe { CqlString::unchecked_from("CQL_VERSION") },
            self.cql_version.clone(),
        );

        if let Some(ref c) = self.compression {
            sm.insert(
                unsafe { CqlString::unchecked_from("COMPRESSION") },
                c.clone(),
            );
        }
        let sm = unsafe { CqlStringMap::unchecked_from(sm) };
        let l = buf.len();
        encode::string_map(&sm, buf);
        Ok(buf.len() - l)
    }
}

#[derive(Debug)]
pub struct AuthResponseMessage {
    pub auth_data: CqlBytes,
}

impl CqlEncode for AuthResponseMessage {
    fn encode(&self, _v: ProtocolVersion, buf: &mut BytesMut) -> Result<usize> {
        let l = buf.len();
        encode::bytes(&self.auth_data, buf);
        Ok(buf.len() - l)
    }
}

#[derive(Debug)]
pub enum QueryValues {
    Positional(Vec<CqlBytes>),
    Named(HashMap<CqlString, CqlBytes>),
}

impl CqlEncode for QueryValues {
    fn encode(&self, _v: ProtocolVersion, buf: &mut BytesMut) -> Result<usize> {
        use self::QueryValues::*;
        let len = buf.len();

        match self {
            &Positional(ref values) => {
                // TODO: possible overflow return ERR then
                encode::short(values.len() as u16, buf);
                for value in values {
                    encode::bytes(value, buf);
                }
            }
            &Named(ref values) => {
                encode::short(values.len() as u16, buf);
                for (key, value) in values {
                    encode::string(key, buf);
                    encode::bytes(value, buf);
                }
            }
        }

        Ok(buf.len() - len)
    }
}

#[derive(Debug)]
pub struct QueryMessage {
    pub query: CqlLongString,
    pub values: Option<QueryValues>,
    pub consistency: CqlConsistency,
    pub skip_metadata: bool,
    pub page_size: Option<i32>,
    pub paging_state: Option<CqlBytes>,
    pub serial_consistency: Option<CqlConsistency>,
    pub timestamp: Option<i64>,
}

impl CqlEncode for QueryMessage {
    fn encode(&self, version: ProtocolVersion, buf: &mut BytesMut) -> Result<usize> {
        let l = buf.len();
        encode::long_string(&self.query, buf);
        encode::consistency(&self.consistency, buf);

        buf.put_u8(self.compute_flags());

        self.values.as_ref().map(|v| v.encode(version, buf));
        self.page_size.map(|v| encode::int(v, buf));
        self.paging_state.as_ref().map(|v| encode::bytes(v, buf));
        self.serial_consistency.as_ref().map(|v| {
            encode::consistency(&v, buf)
        });
        self.timestamp.map(|v| encode::long(v, buf));

        Ok(buf.len() - l)
    }
}

impl QueryMessage {
    pub fn compute_flags(&self) -> u8 {
        let mut flags = 0x00;

        self.values.as_ref().map(|_| flags |= 0x01);

        if self.skip_metadata {
            flags |= 0x02
        }

        self.page_size.as_ref().map(|_| flags |= 0x04);
        self.paging_state.as_ref().map(|_| flags |= 0x08);
        self.serial_consistency.as_ref().map(|_| flags |= 0x10);
        self.page_size.as_ref().map(|_| flags |= 0x04);
        self.timestamp.as_ref().map(|_| flags |= 0x20);

        if let Some(QueryValues::Named(_)) = self.values {
            flags |= 0x40;
        }

        flags
    }
}

impl Default for QueryMessage {
    fn default() -> Self {
        QueryMessage {
            query: CqlLongString::try_from("").expect("an empty string to be valid"),
            values: None,
            consistency: CqlConsistency::One,
            skip_metadata: false,
            page_size: None,
            paging_state: None,
            serial_consistency: None,
            timestamp: None,
        }
    }
}

impl Message {
    fn opcode(&self) -> OpCode {
        use self::Message::*;
        match self {
            &Options => OpCode::Options,
            &Startup(_) => OpCode::Startup,
            &AuthResponse(_) => OpCode::AuthResponse,
            &Query(_) => OpCode::Query,
        }
    }
}

impl CqlEncode for Message {
    fn encode(&self, v: ProtocolVersion, buf: &mut BytesMut) -> Result<usize> {
        match *self {
            Message::Options => Ok(0),
            Message::Startup(ref msg) => msg.encode(v, buf),
            Message::AuthResponse(ref msg) => msg.encode(v, buf),
            Message::Query(ref msg) => msg.encode(v, buf),
        }
    }
}

pub fn cql_encode(
    version: ProtocolVersion,
    flags: u8,
    stream_id: u16,
    to_encode: Message,
    sink: &mut BytesMut,
) -> Result<()> {
    sink.put(&[0; ::codec::header::HEADER_LENGTH][..]);

    let len = to_encode.encode(version, sink)?;
    if len > u32::max_value() as usize {
        return Err(ErrorKind::BodyLengthExceeded(len).into());
    }
    let len = len as u32;

    let header = Header {
        version: Version::request(version),
        flags: flags,
        stream_id: stream_id,
        op_code: to_encode.opcode(),
        length: len,
    };

    let header_bytes = header.encode()?;
    sink[0..::codec::header::HEADER_LENGTH].copy_from_slice(&header_bytes);

    Ok(())
}


#[cfg(test)]
mod test {
    use super::*;
    use codec::header::ProtocolVersion::*;
    use codec::primitives::{CqlConsistency, CqlFrom, CqlBytes};
    use codec::authentication::Authenticator;
    use std::collections::HashMap;
    use bytes::BytesMut;

    #[test]
    fn from_options_request() {
        let o = Message::Options;

        let mut buf = BytesMut::with_capacity(64);
        let flags = 0;
        let stream_id = 270;
        cql_encode(Version3, flags, stream_id, o, &mut buf).unwrap();

        let expected_bytes = b"\x03\x00\x01\x0e\x05\x00\x00\x00\x00";

        assert_eq!(&buf[..], &expected_bytes[..]);
    }

    #[test]
    fn from_startup_req() {
        let o = Message::Startup(StartupMessage {
            cql_version: cql_string!("3.2.1"),
            compression: None,
        });

        let mut buf = BytesMut::with_capacity(64);
        let flags = 0;
        let stream_id = 1;
        cql_encode(Version3, flags, stream_id, o, &mut buf).unwrap();

        let expected_bytes = include_bytes!("../../tests/fixtures/v3/requests/cli_startup.msg");

        assert_eq!(&buf[..], &expected_bytes[..]);
    }

    #[test]
    fn from_auth_response_req() {
        let a = Authenticator::PlainTextAuthenticator {
            username: String::from("abcdef12"),
            password: String::from("123456789asdfghjklqwertyuiopzx"),
        };

        let mut v = BytesMut::with_capacity(64);
        a.encode_auth_response(&mut v);

        let o = Message::AuthResponse(AuthResponseMessage {
            auth_data: CqlBytes::try_from(v).unwrap(),
        });

        let mut buf = BytesMut::with_capacity(64);
        let flags = 0;
        let stream_id = 2;
        cql_encode(Version3, flags, stream_id, o, &mut buf).unwrap();

        let expected_bytes = include_bytes!("../../tests/fixtures/v3/requests/auth_response.msg");

        assert_eq!(&buf[..], &expected_bytes[..]);
    }

    #[test]
    fn from_query_req() {
        let mut buf = BytesMut::with_capacity(64);
        let flags = 0;
        let stream_id = 2;


        let o = Message::Query(QueryMessage {
            query: CqlLongString::try_from("select * from system.local where key = 'local'").unwrap(),
            values: None,
            consistency: CqlConsistency::One,
            skip_metadata: false,
            page_size: Some(5000),
            paging_state: None,
            serial_consistency: None,
            timestamp: Some(1486294317376770),
        });

        cql_encode(Version3, flags, stream_id, o, &mut buf).unwrap();

        let expected_bytes = include_bytes!("../../tests/fixtures/v3/requests/cli_query.msg");
        assert_eq!(&buf[..], &expected_bytes[..]);
    }

    #[test]
    fn query_flags() {
        let mut o = QueryMessage::default();
        assert_eq!(o.compute_flags(), 0x00u8);

        o.values = Some(QueryValues::Positional(Vec::new()));
        assert_eq!(o.compute_flags(), 0x01u8);

        o.values = Some(QueryValues::Named(HashMap::new()));
        assert_eq!(o.compute_flags(), 0x41u8);

        o.skip_metadata = true;
        assert_eq!(o.compute_flags(), 0x43u8);

        o.page_size = Some(2);
        assert_eq!(o.compute_flags(), 0x47u8);

        o.paging_state = Some(cql_bytes!());
        assert_eq!(o.compute_flags(), 0x4fu8);

        o.serial_consistency = Some(CqlConsistency::LocalSerial);
        assert_eq!(o.compute_flags(), 0x5fu8);

        o.timestamp = Some(1);
        assert_eq!(o.compute_flags(), 0x7fu8);
    }

    #[test]
    fn encode_query_values_positional() {
        let values = vec![cql_bytes!(0u8, 1), cql_bytes!(2u8, 3)];
        let values = QueryValues::Positional(values);

        let mut buf = BytesMut::with_capacity(64);
        values.encode(Version3, &mut buf).unwrap();

        let expected = vec![
            0x00,
            0x02,
            0x00,
            0x00,
            0x00,
            0x02,
            0x00,
            0x01,
            0x00,
            0x00,
            0x00,
            0x02,
            0x02,
            0x03,
        ];

        assert_eq!(expected, buf);
    }

    #[test]
    fn encode_query_values_named() {
        let values = {
            let mut m = HashMap::new();
            m.insert(cql_string!("a"), cql_bytes!(0, 1));
            m
        };

        let values = QueryValues::Named(values);

        let mut buf = BytesMut::with_capacity(64);
        values.encode(Version3, &mut buf).unwrap();

        let expected = vec![
            0x00,
            0x01,
            0x00,
            0x01,
            97,
            0x00,
            0x00,
            0x00,
            0x02,
            0x00,
            0x01,
        ];
        assert_eq!(expected, buf);
    }
}
