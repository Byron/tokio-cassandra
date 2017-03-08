use codec::request;
use codec::header::ProtocolVersion;
use codec::authentication::Credentials;
use tokio_service::Service;
use futures::Future;
use tokio_core::reactor::Handle;
use tokio_proto::util::client_proxy::{Response as ClientProxyResponse, ClientProxy};
use tokio_proto::streaming::Message;
use tokio_proto::streaming::multiplex::ClientProto;
use tokio_proto::TcpClient;
use tokio_core::io::{Io, Framed};
use std::io;
use std::net::SocketAddr;
use semver;
use super::ssl;

use super::error::*;
use super::messages::{RequestMessage, ResponseMessage, ChunkedMessage, StreamingMessage};
use super::codec::{CqlCodec, CqlCodecDebuggingOptions};
use super::handshake::interpret_response_and_handle;

#[derive(PartialEq, Debug, Clone)]
pub struct CqlProto {
    pub version: ProtocolVersion,
    pub debug: Option<CqlCodecDebuggingOptions>,
}

impl<T: Io + 'static> ClientProto<T> for CqlProto {
    type Request = request::Message;
    type RequestBody = request::Message;
    type Response = StreamingMessage;
    type ResponseBody = ChunkedMessage;
    type Error = io::Error;

    /// `Framed<T, LineCodec>` is the return value of `io.framed(LineCodec)`
    type Transport = Framed<T, CqlCodec>;
    type BindTransport = io::Result<Self::Transport>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        debug!("binding transport!");
        Ok(io.framed(CqlCodec::new(self.version, self.debug.clone().unwrap_or_default())))
    }
}


pub struct ClientHandle {
    inner: Box<Service<Request = RequestMessage,
                       Response = ResponseMessage,
                       Error = io::Error,
                       Future = ClientProxyResponse<ResponseMessage, io::Error>>>,
}

impl From<request::Message> for RequestMessage {
    fn from(msg: request::Message) -> Self {
        Message::WithoutBody(msg)
    }
}

impl From<ResponseMessage> for StreamingMessage {
    fn from(msg: ResponseMessage) -> Self {
        match msg {
            Message::WithoutBody(res) => res,
            Message::WithBody(_head, bodystream) => StreamingMessage::Partial(bodystream),
        }
    }
}

impl Service for ClientHandle {
    type Request = request::Message;
    type Response = StreamingMessage;
    type Error = io::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        Box::new(self.inner.call(req.into()).map(From::from))
    }
}

/// Currently acts more like a builder, and the desired semantics are yet to be determined.
#[derive(PartialEq, Debug, Clone)]
pub struct Client {
    pub protocol: CqlProto,
}

#[cfg(not(feature = "with-openssl"))]
fn ssl_client(_protocol: CqlProto,
              _addr: &SocketAddr,
              _handle: &Handle,
              _tls: ssl::Options)
              -> Box<Future<Item = ClientProxy<RequestMessage, ResponseMessage, io::Error>, Error = io::Error>> {
    use futures::future;
    use super::utils::io_err;
    Box::new(future::err(io_err("Please compile this library with \
                                                     --features=ssl")))
}

#[cfg(feature = "with-openssl")]
fn ssl_client(protocol: CqlProto,
              addr: &SocketAddr,
              handle: &Handle,
              tls: ssl::Options)
              -> Box<Future<Item = ClientProxy<RequestMessage, ResponseMessage, io::Error>, Error = io::Error>> {
    use super::ssl::ssl_client::SslClient;
    Box::new(SslClient::new(protocol, tls).connect(addr, handle))
}

#[derive(Clone, Default)]
pub struct ConnectOptions {
    pub creds: Option<Credentials>,
    pub tls: Option<ssl::Options>,
    pub desired_cql_version: Option<semver::Version>,
}

impl Client {
    pub fn connect(self,
                   addr: &SocketAddr,
                   handle: &Handle,
                   options: ConnectOptions)
                   -> Box<Future<Item = ClientHandle, Error = Error>> {
        let ConnectOptions { creds, tls, desired_cql_version } = options;
        let ret = match tls {
                Some(tls) => ssl_client(self.protocol, addr, handle, tls),
                None => Box::new(TcpClient::new(self.protocol).connect(addr, handle)),
            }
            .map(|client_proxy| ClientHandle { inner: Box::new(client_proxy) })
            .and_then(|client_handle| client_handle.call(request::Message::Options).map(|r| (r, client_handle)))
            .map_err(|e| e.into())
            .and_then(|(res, ch)| interpret_response_and_handle(ch, res, creds, desired_cql_version))
            .and_then(|ch| Ok(ch));

        Box::new(ret)
    }
}
