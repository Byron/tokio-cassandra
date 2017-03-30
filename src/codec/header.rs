//!  The CQL binary protocol is a frame based protocol. Frames are defined as:
//!
//! ```ascii
//!      0         8        16        24        32         40
//!      +---------+---------+---------+---------+---------+
//!      | version |  flags  |      stream       | opcode  |
//!      +---------+---------+---------+---------+---------+
//!      |                length                 |
//!      +---------+---------+---------+---------+
//!      |                                       |
//!      .            ...  body ...              .
//!      .                                       .
//!      .                                       .
//!      +----------------------------------------
//! ```
//!
//!  The protocol is big-endian (network byte order).
//!
//!  Each frame contains a fixed size header (9 bytes) followed by a variable size
//!  body. The header is described in Section 2. The content of the body depends
//!  on the header opcode value (the body can in particular be empty for some
//!  opcode values). The list of allowed opcode is defined Section 2.4 and the
//!  details of each corresponding message is described Section 4.
//!
//!  The protocol distinguishes 2 types of frames: requests and responses. Requests
//!  are those frame sent by the clients to the server, response are the ones sent
//!  by the server. Note however that the protocol supports server pushes (events)
//!  so responses does not necessarily come right after a client request.
//!
//!  Note to client implementors: clients library should always assume that the
//!  body of a given frame may contain more data than what is described in this
//!  document. It will however always be safe to ignore the remaining of the frame
//!  body in such cases. The reason is that this may allow to sometimes extend the
//!  protocol with optional features without needing to change the protocol
//!  version.
use byteorder::{BigEndian, ReadBytesExt, ByteOrder};
pub const HEADER_LENGTH: usize = 9;

error_chain! {
    errors {
        UnsupportedVersion(v: u8) {
            description("The protocol version of cassandra is unsupported")
            display("The protocol version {} of cassandra is unsupported", v)
        }
        InvalidDataLength(l: usize) {
            description("The size of data is incorrect")
            display("Expected a data size of {} bytes, but got {}", HEADER_LENGTH, l)
        }
        InvalidOpCode(c: u8) {
            description("Could not parse value as opcode.")
            display("Value {} is not a valid opcode", c)
        }
    }
    foreign_links {
        Io(::std::io::Error);
    }
}

#[cfg_attr(feature = "with-serde", derive(Deserialize, Serialize))]
#[derive(PartialEq, Debug, Clone)]
pub enum Direction {
    Request,
    Response,
}

#[cfg_attr(feature = "with-serde", derive(Deserialize, Serialize))]
#[derive(PartialEq, Debug, Clone)]
pub enum OpCode {
    Error,
    Startup,
    Ready,
    Authenticate,
    Options,
    Supported,
    Query,
    Result,
    Prepare,
    Execute,
    Register,
    Event,
    Batch,
    AuthChallenge,
    AuthResponse,
    AuthSuccess,
}


impl OpCode {
    pub fn try_from(b: u8) -> Result<OpCode> {
        Ok(match b {
               0x00 => OpCode::Error,
               0x01 => OpCode::Startup,
               0x02 => OpCode::Ready,
               0x03 => OpCode::Authenticate,
               0x05 => OpCode::Options,
               0x06 => OpCode::Supported,
               0x07 => OpCode::Query,
               0x08 => OpCode::Result,
               0x09 => OpCode::Prepare,
               0x0A => OpCode::Execute,
               0x0B => OpCode::Register,
               0x0C => OpCode::Event,
               0x0D => OpCode::Batch,
               0x0E => OpCode::AuthChallenge,
               0x0F => OpCode::AuthResponse,
               0x10 => OpCode::AuthSuccess,
               _ => return Err(ErrorKind::InvalidOpCode(b).into()),
           })
    }

    pub fn as_u8(&self) -> u8 {
        match *self {
            OpCode::Error => 0x00,
            OpCode::Startup => 0x01,
            OpCode::Ready => 0x02,
            OpCode::Authenticate => 0x03,
            OpCode::Options => 0x05,
            OpCode::Supported => 0x06,
            OpCode::Query => 0x07,
            OpCode::Result => 0x08,
            OpCode::Prepare => 0x09,
            OpCode::Execute => 0x0A,
            OpCode::Register => 0x0B,
            OpCode::Event => 0x0C,
            OpCode::Batch => 0x0D,
            OpCode::AuthChallenge => 0x0E,
            OpCode::AuthResponse => 0x0F,
            OpCode::AuthSuccess => 0x10,
        }
    }
}

#[cfg_attr(feature = "with-serde", derive(Deserialize, Serialize))]
#[derive(PartialEq, Debug, Clone)]
pub struct Header {
    pub version: Version,

    /// Flags applying to this frame. The flags have the following meaning (described
    /// by the mask that allow to select them):
    /// 0x01: Compression flag. If set, the frame body is compressed. The actual
    /// compression to use should have been set up beforehand through the
    /// Startup message (which thus cannot be compressed; Section 4.1.1).
    /// 0x02: Tracing flag. For a request frame, this indicate the client requires
    /// tracing of the request. Note that not all requests support tracing.
    /// Currently, only QUERY, PREPARE and EXECUTE queries support tracing.
    /// Other requests will simply ignore the tracing flag if set. If a
    /// request support tracing and the tracing flag was set, the response to
    /// this request will have the tracing flag set and contain tracing
    /// information.
    /// If a response frame has the tracing flag set, its body contains
    /// a tracing ID. The tracing ID is a [uuid] and is the first thing in
    /// the frame body. The rest of the body will then be the usual body
    /// corresponding to the response opcode.
    /// The rest of the flags is currently unused and ignored.
    pub flags: u8,
    /// A frame has a stream id (a [short] value). When sending request messages, this
    /// stream id must be set by the client to a non-negative value (negative stream id
    /// are reserved for streams initiated by the server; currently all EVENT messages
    /// (section 4.2.6) have a streamId of -1). If a client sends a request message
    /// with the stream id X, it is guaranteed that the stream id of the response to
    /// that message will be X.

    /// This allow to deal with the asynchronous nature of the protocol. If a client
    /// sends multiple messages simultaneously (without waiting for responses), there
    /// is no guarantee on the order of the responses. For instance, if the client
    /// writes REQ_1, REQ_2, REQ_3 on the wire (in that order), the server might
    /// respond to REQ_3 (or REQ_2) first. Assigning different stream id to these 3
    /// requests allows the client to distinguish to which request an received answer
    /// respond to. As there can only be 32768 different simultaneous streams, it is up
    /// to the client to reuse stream id.

    /// Note that clients are free to use the protocol synchronously (i.e. wait for
    /// the response to REQ_N before sending REQ_N+1). In that case, the stream id
    /// can be safely set to 0. Clients should also feel free to use only a subset of
    /// the 32768 maximum possible stream ids if it is simpler for those
    /// implementation.
    pub stream_id: u16,
    pub op_code: OpCode,
    /// A 4 byte integer representing the length of the body of the frame (note:
    /// currently a frame is limited to 256MB in length).
    pub length: u32,
}

impl Header {
    pub fn try_from(b: &[u8]) -> Result<Header> {
        if b.len() < HEADER_LENGTH {
            return Err(ErrorKind::InvalidDataLength(b.len()).into());
        }

        let version = Version::try_from(b[0])?;
        Ok(Header {
               version: version,
               flags: b[1],
               stream_id: (&b[2..4])
                   .read_u16::<BigEndian>()
                   .expect("to have 2 bytes exactly"),
               op_code: OpCode::try_from(b[4])?,
               length: (&b[5..9])
                   .read_u32::<BigEndian>()
                   .expect("to have 4 bytes exactly"),
           })
    }

    pub fn is_compressed(&self) -> bool {
        self.flags & 0x01 == 0x01
    }

    pub fn is_traced(&self) -> bool {
        self.flags & 0x02 == 0x02
    }

    pub fn encode(&self) -> Result<[u8; 9]> {
        let version = self.version.encode();
        let mut buf = [0; 9];
        buf[0] = version;
        buf[1] = self.flags;
        {
            let stream_id = &mut buf[2..4];
            BigEndian::write_u16(stream_id, self.stream_id);
        }
        buf[4] = self.op_code.as_u8();
        {
            let length = &mut buf[5..];
            BigEndian::write_u32(length, self.length);
        }
        Ok(buf)
    }

    pub fn encoded_len() -> usize {
        9
    }
}

/// The version is a single byte that indicate both the direction of the message
/// (request or response) and the version of the protocol in use. The up-most bit
/// of version is used to define the direction of the message: 0 indicates a
/// request, 1 indicates a responses. This can be useful for protocol analyzers to
/// distinguish the nature of the packet from the direction which it is moving.
/// The rest of that byte is the protocol version (3 for the protocol defined in
/// this document). In other words, for this version of the protocol, version will
/// have one of:
/// 0x03    Request frame for this protocol version
/// 0x83    Response frame for this protocol version
///
/// Please note that the while every message ship with the version, only one version
/// of messages is accepted on a given connection. In other words, the first message
/// exchanged (STARTUP) sets the version for the connection for the lifetime of this
/// connection.
/// This document describe the version 3 of the protocol. For the changes made since
/// version 2, see Section 10.
#[cfg_attr(feature = "with-serde", derive(Deserialize, Serialize))]
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum ProtocolVersion {
    Version3,
}

#[cfg_attr(feature = "with-serde", derive(Deserialize, Serialize))]
#[derive(PartialEq, Debug, Clone)]
pub struct Version {
    pub version: ProtocolVersion,
    pub direction: Direction,
}

impl Version {
    pub fn try_from(b: u8) -> Result<Version> {
        Ok(match b {
               0x03 => Version::v3_request(),
               0x83 => Version::v3_response(),
               _ => return Err(ErrorKind::UnsupportedVersion(b).into()),
           })
    }

    pub fn request(version: ProtocolVersion) -> Version {
        Version {
            version: version,
            direction: Direction::Request,
        }
    }

    pub fn encode(&self) -> u8 {
        match (&self.version, &self.direction) {
            (&ProtocolVersion::Version3, &Direction::Request) => 0x03,
            (&ProtocolVersion::Version3, &Direction::Response) => 0x83,
        }
    }
    pub fn v3_response() -> Version {
        Version {
            version: ProtocolVersion::Version3,
            direction: Direction::Response,
        }
    }
    pub fn v3_request() -> Version {
        Version {
            version: ProtocolVersion::Version3,
            direction: Direction::Request,
        }
    }
}

#[cfg(test)]
mod test {
    use std::ops::Deref;
    use std::fmt::{Display, Debug};
    use std::result::Result as StdResult;
    use error_chain::ChainedError;
    use super::*;

    #[test]
    fn version() {
        assert_eq!(Version::v3_request().encode(), b'\x03');
        assert_eq!(Version::v3_response().encode(), b'\x83');
        assert_eq!(Version::request(ProtocolVersion::Version3),
                   Version::v3_request());
        assert_eq!(Version::try_from(b'\x03').unwrap(), Version::v3_request());
        assert_eq!(Version::try_from(b'\x83').unwrap(), Version::v3_response());
        assert!(Version::try_from(b'\x88').is_err());
    }

    #[test]
    fn complete_decode() {
        let bytes = b"\x03\x00\x01\x01\x05\x00\x00\x01\x05";
        let h = Header::try_from(&bytes[..]).unwrap();

        assert_eq!(h.version, Version::v3_request());
        assert_eq!(h.is_compressed(), false);
        assert_eq!(h.is_traced(), false);
        assert_eq!(h.stream_id, 257);
        assert_eq!(h.op_code, OpCode::Options);
        assert_eq!(h.length, 261);
    }

    #[test]
    fn complete_encode() {
        let h = Header {
            version: Version::v3_request(),
            flags: 0x00,
            stream_id: 257,
            op_code: OpCode::Options,
            length: 261,
        };
        let expected_bytes = b"\x03\x00\x01\x01\x05\x00\x00\x01\x05";
        let buf = h.encode().unwrap();

        assert_eq!(&buf[..], &expected_bytes[..]);
    }

    #[test]
    fn version3_response() {
        let bytes = b"\x83\x00\x01\x01\x05\x00\x00\x01\x05";
        let h = Header::try_from(&bytes[..]).unwrap();

        assert_eq!(h.version, Version::v3_response());
    }

    #[test]
    fn invalid_op_code() {
        let bytes = b"\x03\x00\x01\x01\x04\x00\x00\x00\x00";
        let res = Header::try_from(&bytes[..]);

        assert!(err_is(res, ErrorKind::InvalidOpCode(0x04)));
    }
    #[test]
    fn invalid_length_of_data() {
        let bytes = b"\x03\x00\x01\x01\x05\x00\x00\x00";
        let res = Header::try_from(&bytes[..]);

        assert!(err_is(res, ErrorKind::InvalidDataLength(8)));
    }

    #[test]
    fn flags_compressed() {
        let bytes = b"\x03\x01\x00\x00\x05\x00\x00\x00\x00";
        let h = Header::try_from(&bytes[..]).unwrap();

        assert_eq!(h.is_compressed(), true);
        assert_eq!(h.is_traced(), false);
    }

    #[test]
    fn flags_traced() {
        let bytes = b"\x03\x02\x00\x00\x05\x00\x00\x00\x00";
        let h = Header::try_from(&bytes[..]).unwrap();

        assert_eq!(h.is_compressed(), false);
        assert_eq!(h.is_traced(), true);
    }

    #[test]
    fn unsupported_version() {
        let bytes = b"\x04\x02\x00\x00\x05\x00\x00\x00\x00";
        let res = Header::try_from(&bytes[..]);

        assert!(err_is(res, ErrorKind::UnsupportedVersion(0x04)));
    }


    fn err_is<T, E>(res: StdResult<T, E>, kind: E::Target) -> bool
        where E: Deref + ChainedError + Debug + Display,
              E::Target: Display + Sized,
              E::ErrorKind: Display,
              T: Debug
    {
        format!("{}", res.unwrap_err().kind()) == format!("{}", kind)
    }
}
