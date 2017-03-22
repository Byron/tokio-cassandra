use codec::header::ProtocolVersion;
use bytes::BytesMut;

mod result;
pub use self::result::*;

mod row;
pub use self::row::*;

mod simple_messages;
pub use self::simple_messages::*;

error_chain! {
    foreign_links {
        Io(::std::io::Error);
        HeaderError(::codec::header::Error);
        DecodeError(::codec::primitives::decode::Error);
    }

    errors {
        Incomplete(err: String) {
            description("Unsufficient bytes")
            display("Buffer contains unsufficient bytes: {}", err)
        }
        ParserError(err: String) {
            description("Error during parsing")
            display("{}", err)
        }
    }
}

#[derive(Debug)]
pub enum Message {
    Supported(SupportedMessage),
    Ready,
    Authenticate(AuthenticateMessage),
    AuthSuccess(AuthSuccessMessage),
    Error(ErrorMessage),
    Result,
}

pub trait CqlDecode<T> {
    fn decode(v: ProtocolVersion, buf: BytesMut) -> Result<T>;
}
