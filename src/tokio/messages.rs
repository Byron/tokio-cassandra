use codec::request;
use codec::response;
use tokio_proto::streaming::{Message, Body};
use std::io;

/// A chunk of a result - similar to response::ResultMessage, but only a chunk of it
/// TODO: this is just a dummy to show the intent - this is likely to change
#[derive(Debug)]
pub struct ResultChunk;

/// A message representing a partial response
#[derive(Debug)]
pub enum ChunkedMessage {
    Result(ResultChunk),
}

/// Streamable responses use the body type, which implements stream, with the streamable response.
/// In our case, this will only be the Result response
/// TODO: fix comment above once things get clearer
#[derive(Debug)]
pub enum StreamingMessage {
    Supported(response::SupportedMessage),
    Error(response::ErrorMessage),
    Partial(ResponseStream),
    Authenticate(response::AuthenticateMessage),
    AuthSuccess(response::AuthSuccessMessage),
    Ready,
    Result(response::ResultMessage), // FIXME: later use chunked message
}

impl From<StreamingMessage> for response::Message {
    fn from(f: StreamingMessage) -> Self {
        use self::StreamingMessage::*;
        match f {
            Ready => response::Message::Ready,
            Supported(msg) => response::Message::Supported(msg),
            Error(msg) => response::Message::Error(msg),
            AuthSuccess(msg) => response::Message::AuthSuccess(msg),
            Authenticate(msg) => response::Message::Authenticate(msg),
            Result(msg) => response::Message::Result(msg),
            Partial(_) => panic!("Partials are not suppported - this is just used during handshake"),
        }
    }
}

impl From<response::Message> for StreamingMessage {
    fn from(f: response::Message) -> Self {
        match f {
            response::Message::Ready => StreamingMessage::Ready,
            response::Message::Supported(msg) => StreamingMessage::Supported(msg),
            response::Message::AuthSuccess(msg) => StreamingMessage::AuthSuccess(msg),
            response::Message::Authenticate(msg) => StreamingMessage::Authenticate(msg),
            response::Message::Error(msg) => StreamingMessage::Error(msg),
            response::Message::Result(msg) => StreamingMessage::Result(msg),
        }
    }
}

pub type ResponseStream = Body<ChunkedMessage, io::Error>;
pub type ResponseMessage = Message<StreamingMessage, ResponseStream>;

pub type RequestMessage = Message<request::Message, RequestStream>;
pub type RequestStream = Body<request::Message, io::Error>;
