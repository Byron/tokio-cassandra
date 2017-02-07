use codec::header::{OpCode, Header, ProtocolVersion, Direction};

error_chain! {
    foreign_links {
        Io(::std::io::Error);
        HeaderError(::codec::header::Error);
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

pub trait CqlEncode {
    fn encode(&self, f: &mut Vec<u8>) -> Result<usize>;
}

pub enum Message {
    Options,
}

impl Message {
    fn opcode(&self) -> OpCode {
        use self::Message::*;
        match self {
            &Options => OpCode::Options,
        }

    }
    fn protocol_version() -> ProtocolVersion {
        ProtocolVersion::Version3(Direction::Request)
    }
}


impl CqlEncode for Message {
    fn encode(&self, _: &mut Vec<u8>) -> Result<usize> {
        match *self {
            Message::Options => Ok(0),
        }
    }
}


pub fn cql_encode(flags: u8, stream_id: u16, to_encode: Message, sink: &mut Vec<u8>) -> Result<()> {
    sink.resize(Header::encoded_len(), 0);

    let len = to_encode.encode(sink)?;
    if len > u32::max_value() as usize {
        return Err(ErrorKind::BodyLengthExceeded(len).into());
    }
    let len = len as u32;

    let header = Header {
        version: Message::protocol_version(),
        flags: flags,
        stream_id: stream_id,
        op_code: to_encode.opcode(),
        length: len,
    };

    let header_bytes = header.encode()?;
    sink[0..Header::encoded_len()].copy_from_slice(&header_bytes);

    Ok(())
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn from_options_request() {
        let o = Message::Options;

        let mut buf = Vec::new();
        let flags = 0;
        let stream_id = 270;
        cql_encode(flags, stream_id, o, &mut buf).unwrap();

        let expected_bytes = b"\x03\x00\x01\x0e\x05\x00\x00\x00\x00";

        assert_eq!(&buf[..], &expected_bytes[..]);
    }
}
