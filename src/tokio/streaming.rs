use codec::request::{self, cql_encode};
use codec::response;
use codec::header::{Header, ProtocolVersion, Direction};
use codec::authentication::{Authenticator, Credentials};
use codec::primitives::{CqlBytes, CqlFrom};
use tokio_service::Service;
use futures::{Sink, Stream, Future};
use futures::future;
use tokio_core::reactor::Handle;
use tokio_proto::util::client_proxy::ClientProxy;
use tokio_proto::streaming::{Message, Body};
use tokio_proto::streaming::multiplex::{RequestId, ClientProto, Frame};
use tokio_proto::TcpClient;
use tokio_core::io::{EasyBuf, Codec, Io, Framed};
use std::{io, mem};
use std::net::SocketAddr;
use super::utils::{io_err, decode_complete_message_by_opcode};

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
            Partial(_) => {
                panic!("Partials are not suppported - this is just used during handshake")
            }
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
        }
    }
}

type ResponseStream = Body<ChunkedMessage, io::Error>;
type ResponseMessage = Message<StreamingMessage, ResponseStream>;

type RequestMessage = Message<request::Message, RequestStream>;
type RequestStream = Body<request::Message, io::Error>;


#[derive(PartialEq, Debug, Clone)]
pub struct CqlCodec {
    state: Machine,
    flags: u8,
    version: ProtocolVersion,
}


#[derive(PartialEq, Debug, Clone)]
enum Machine {
    NeedHeader,
    WithHeader { header: Header, body_len: usize },
}

impl CqlCodec {
    fn new(v: ProtocolVersion) -> Self {
        CqlCodec {
            state: Machine::NeedHeader,
            flags: 0,
            version: v,
        }
    }
}

type CodecInputFrame = Frame<StreamingMessage, ChunkedMessage, io::Error>;
type CodecOutputFrame = Frame<request::Message, request::Message, io::Error>;

impl Codec for CqlCodec {
    type In = CodecInputFrame;
    type Out = CodecOutputFrame;
    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        use self::Machine::*;
        match self.state {
            NeedHeader => {
                if buf.len() < Header::encoded_len() {
                    return Ok(None);
                }
                let h = Header::try_from(buf.drain_to(Header::encoded_len())
                        .as_slice()).map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
                assert!(h.version.direction == Direction::Response,
                        "As a client protocol, I can only handle response decoding");
                let len = h.length;
                self.state = WithHeader {
                    header: h,
                    body_len: len as usize,
                };

                return self.decode(buf);
            }
            WithHeader { body_len, .. } => {
                if body_len as usize > buf.len() {
                    return Ok(None);
                }
                let h = match mem::replace(&mut self.state, NeedHeader) {
                    WithHeader { header, .. } => header,
                    _ => unreachable!(),
                };
                /* TODO: implement version mismatch test */
                let code = h.op_code.clone();
                let version = h.version.version;
                assert_stream_id(h.stream_id);
                let msg = Frame::Message {
                    id: h.stream_id as RequestId,
                    /* TODO: verify amount of consumed bytes equals the ones actually parsed */
                    message: decode_complete_message_by_opcode(version,
                                                               code,
                                                               buf.drain_to(body_len))
                        .map_err(|err| io_err(err))?
                        .into(),
                    body: false,
                    solo: false,
                };
                debug!("decoded msg: {:?}", msg);
                Ok(Some(msg))
            }
        }
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        match msg {
            Frame::Message { id, message, .. } => {
                debug!("encoded msg: {:?}", message);
                assert_stream_id(id as u16);
                let msg = cql_encode(self.version,
                                     self.flags,
                                     id as u16, /* FIXME safe cast */
                                     message,
                                     buf)
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err));
                msg
            }
            Frame::Error { error, .. } => Err(error),
            Frame::Body { .. } => panic!("Streaming of Requests is not currently supported"),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct CqlProto {
    pub version: ProtocolVersion,
    pub credentials: Option<Credentials>,
}

impl<T: Io + 'static> ClientProto<T> for CqlProto {
    type Request = request::Message;
    type RequestBody = request::Message;
    type Response = StreamingMessage;
    type ResponseBody = ChunkedMessage;
    type Error = io::Error;

    /// `Framed<T, LineCodec>` is the return value of `io.framed(LineCodec)`
    type Transport = Framed<T, CqlCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        debug!("binding transport!");
        Ok(io.framed(CqlCodec::new(self.version)))
    }
}


pub struct ClientHandle {
    inner: ClientProxy<RequestMessage, ResponseMessage, io::Error>,
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
        self.inner.call(req.into()).map(From::from).boxed()
    }
}

/// Currently acts more like a builder, and the desired semantics are yet to be determined.
pub struct Client {
    pub protocol: CqlProto,
}

impl Client {
    pub fn connect(self,
                   addr: &SocketAddr,
                   handle: &Handle,
                   creds: Option<Credentials>)
                   -> Box<Future<Item = ClientHandle, Error = io::Error>> {
        let ret = TcpClient::new(self.protocol)
            .connect(addr, handle)
            .map(|client_proxy| ClientHandle { inner: client_proxy })
            .and_then(|client_handle| {
                let f = client_handle.call(request::Message::Options);
                f.join(future::ok(client_handle))
            })
            .and_then(|(res, ch)| interpret_response_and_handle(ch, res, creds))
            .and_then(|ch| Ok(ch));

        Box::new(ret)
    }
}

impl From<CodecInputFrame> for SimpleResponse {
    fn from(f: CodecInputFrame) -> Self {
        match f {
            Frame::Message { id, message, .. } => SimpleResponse(id, message.into()),
            Frame::Error { .. } => {
                panic!("Frame errors cannot happen here - this is only done during the handshake")
            }
            Frame::Body { .. } => {
                panic!("Streamed bodies must not happen for the simple responses we expect here")
            }
        }
    }
}

impl From<SimpleRequest> for CodecOutputFrame {
    fn from(SimpleRequest(id, msg): SimpleRequest) -> Self {
        Frame::Message {
            id: id,
            message: msg,
            body: false,
            solo: false,
        }
    }
}

pub struct SimpleResponse(pub RequestId, pub response::Message);
pub struct SimpleRequest(pub RequestId, pub request::Message);

fn interpret_response_and_handle(handle: ClientHandle,
                                 res: StreamingMessage,
                                 creds: Option<Credentials>)
                                 -> Box<Future<Item = ClientHandle, Error = io::Error>> {
    Box::new({
        let res: response::Message = res.into();
        match res {
            response::Message::Ready => future::ok(handle),
            response::Message::Error(msg) => {
                future::err(io_err(format!("Got Error {}: {:?}", msg.code, msg.text)))
            }
            msg => {
                future::err(io_err(format!("Did not expect to receive the following message {:?}",
                                           msg)))
            }
        }
    })
    //            let SimpleResponse(_id, res) = response.into();
    //            match res {
    //                response::Message::Supported(msg) => {
    //                    let startup = request::StartupMessage {
    //                        cql_version: msg.latest_cql_version()
    //              .ok_or(io_err("Expected CQL_VERSION to contain at least one version"))?
    //                            .clone(),
    //                        compression: None,
    //                    };
    //                    debug!("startup {:?}", startup);
    //
    //                    Ok((handle, request::Message::Startup(startup)))
    //                }
    //                response::Message::Authenticate(msg) => {
    //                    let creds =
    //       creds.ok_or(io_err(format!("No credentials provided but server requires \
    //                                                   authentication by {}",
    //                                                  msg.authenticator.as_ref())))?;
    //
    //                    let authenticator = Authenticator::from_name(msg.authenticator.as_ref(),
    //                                                                 creds)
    //                        .map_err(|err| io_err(format!("Authenticator Err: {}", err)))?;
    //
    //                    let mut buf = Vec::new();
    //                    authenticator.encode_auth_response(&mut buf);
    //
    //                    Ok((transport,
    //                        request::Message::AuthResponse(request::AuthResponseMessage {
    //                            auth_data: CqlBytes::try_from(buf)
    //                                .map_err(|err| io_err(format!("Message Err: {}", err)))?,
    //                        })))
    //                }
    //                 TODO: Return a Proper Value saying it has been completed
    //                response::Message::Ready => Err(io_err("ready not expected")),
    //                response::Message::Error(msg) => {
    //                    Err(io_err(format!("Got Error {}: {:?}", msg.code, msg.text)))
    //                }
    //                msg => {
    //                    Err(io_err(format!("Did not expect to receive the following message {:?}",
    //                                       msg)))
    //                }
    //            }
    //        }))
}

fn assert_stream_id(id: u16) {
    // TODO This should not be an assertion, but just an error to be returned.
    // The actual goal is to gain control over the domain of our request IDs, which right
    // now is not present when clients use the service call interface.
    // This should only be possible if there are more than i16::max_value() requests in flight!
    assert!(id as i16 > -1,
            "stream-id {} was negative, which makes it a broadcast id with a special meaning",
            id);
}
