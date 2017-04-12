use super::client::ClientHandle as ComplexClientHandle;
use super::messages::StreamingMessage;
use tokio_service::Service;
use futures::Future;
use codec::{response, request};
use std::io;

pub struct ClientHandle {
    inner: ComplexClientHandle,
}

impl From<ComplexClientHandle> for ClientHandle {
    fn from(f: ComplexClientHandle) -> Self {
        ClientHandle { inner: f }
    }
}

#[derive(Debug)]
pub enum Message {
    Supported(response::SupportedMessage),
    Error(response::ErrorMessage),
    AuthSuccess(response::AuthSuccessMessage),
    Authenticate(response::AuthenticateMessage),
    Result(response::ResultMessage),
    Ready,
}

impl From<StreamingMessage> for Message {
    fn from(f: StreamingMessage) -> Self {
        match f {
            StreamingMessage::Ready => Message::Ready,
            StreamingMessage::Supported(msg) => Message::Supported(msg),
            StreamingMessage::Error(msg) => Message::Error(msg),
            StreamingMessage::AuthSuccess(msg) => Message::AuthSuccess(msg),
            StreamingMessage::Authenticate(msg) => Message::Authenticate(msg),
            StreamingMessage::Result(msg) => Message::Result(msg),
            StreamingMessage::Partial(_stream) => {
                // TODO: exhaust stream and build a singular response in a blocking fashion
                unimplemented!()
            }
        }
    }
}

impl Service for ClientHandle {
    type Request = request::Message;
    type Response = StreamingMessage;
    type Error = io::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        Box::new(self.inner.call(req).map(From::from))
    }
}
