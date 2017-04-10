use codec::request::{self, cql_encode};
use codec::header::{Header, ProtocolVersion, Direction};
use std::path::PathBuf;
use std::fs::{File, OpenOptions};
use tokio_proto::streaming::multiplex::{RequestId, Frame};
use tokio_io::codec::{Decoder, Encoder};
use bytes::BytesMut;
use std::{io, mem};
use std::io::Write;
use codec::header::OpCode;
use codec::response::{self, CqlDecode};
use super::utils::io_err;


// FIXME - don't use pub here, fix imports
pub use super::messages::*;
pub use super::error::*;
#[derive(PartialEq, Debug, Clone)]
pub struct CqlCodec {
    state: Machine,
    flags: u8,
    version: ProtocolVersion,
    debug: CqlCodecDebuggingOptions,
}

#[derive(PartialEq, Debug, Clone, Default)]
pub struct CqlCodecDebuggingOptions {
    pub dump_decoded_frames_into: Option<PathBuf>,
    pub dump_encoded_frames_into: Option<PathBuf>,
    pub frames_count: usize,
}

#[derive(PartialEq, Debug, Clone)]
enum Machine {
    NeedHeader,
    WithHeader { header: Header, body_len: usize },
}

impl CqlCodec {
    pub fn new(v: ProtocolVersion, debug: CqlCodecDebuggingOptions) -> Self {
        CqlCodec {
            state: Machine::NeedHeader,
            flags: 0,
            version: v,
            debug: debug,
        }
    }

    fn do_encode_debug(&mut self, buf: &mut BytesMut) -> io::Result<()> {
        if let Some(path) = self.debug.dump_encoded_frames_into.clone() {
            let h = Header::try_from(buf.as_ref()).expect("header encoded at beginning of buf");
            let mut f = open_at(self.debug_path(path, &h))?;
            f.write_all(buf)?;
        }
        Ok(())
    }

    fn debug_path(&mut self, mut path: PathBuf, h: &Header) -> PathBuf {
        path.push(format!("{:02}-{:02x}_{:?}.bytes",
                          self.debug.frames_count,
                          h.op_code.as_u8(),
                          h.op_code));
        self.debug.frames_count += 1;
        path
    }

    fn do_decode_debug(&mut self, h: &Header, buf: &BytesMut, body_len: usize) -> io::Result<()> {
        if let Some(path) = self.debug.dump_decoded_frames_into.clone() {
            let mut f = open_at(self.debug_path(path, h))?;
            f.write_all(&h.encode().expect("header encode to work")[..])?;
            f.write_all(&buf.as_ref()[..body_len])?;
        }
        Ok(())
    }
}

fn open_at(path: PathBuf) -> io::Result<File> {
    OpenOptions::new()
        .read(false)
        .create(true)
        .write(true)
        .open(&path)
        .map_err(|e| {
                     io_err(format!("Failed to open '{}' for writing with error with error: {:?}",
                                    path.display(),
                                    e))
                 })
}

pub type CodecInputFrame = Frame<StreamingMessage, ChunkedMessage, io::Error>;
pub type CodecOutputFrame = Frame<request::Message, request::Message, io::Error>;

impl Decoder for CqlCodec {
    type Item = CodecInputFrame;
    type Error = io::Error;

    // TODO/ IDEA: Remove io::Error

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        use self::Machine::*;
        match self.state {
            NeedHeader => {
                if src.len() < Header::encoded_len() {
                    return Ok(None);
                }
                let h = Header::try_from(src.split_to(Header::encoded_len()).as_ref())
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
                assert!(h.version.direction == Direction::Response,
                        "As a client protocol, I can only handle response decoding");
                let len = h.length;
                self.state = WithHeader {
                    header: h,
                    body_len: len as usize,
                };

                return self.decode(src);
            }
            WithHeader { body_len, .. } => {
                if body_len as usize > src.len() {
                    return Ok(None);
                }
                let h = match mem::replace(&mut self.state, NeedHeader) {
                    WithHeader { header, .. } => header,
                    _ => unreachable!(),
                };
                self.do_decode_debug(&h, &src, body_len)?;
                /* TODO: implement version mismatch test */
                let code = h.op_code.clone();
                let version = h.version.version;
                assert_stream_id(h.stream_id);
                let msg = Frame::Message {
                    id: h.stream_id as RequestId,
                    /* TODO: verify amount of consumed bytes equals the ones actually parsed */
                    message: decode_complete_message_by_opcode(version, code, src.split_to(body_len))
                        .map_err(io_err)?
                        .into(),
                    body: false,
                    solo: false,
                };
                debug!("decoded msg: {:?}", msg);
                Ok(Some(msg))
            }
        }
    }
}

impl Encoder for CqlCodec {
    type Item = CodecOutputFrame;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> io::Result<()> {
        match item {
            Frame::Message { id, message, .. } => {
                debug!("encoded msg: {:?}", message);
                assert_stream_id(id as u16);
                let res = cql_encode(self.version,
                                     self.flags,
                                     id as u16, /* FIXME safe cast */
                                     message,
                                     dst)
                        .map_err(io_err);
                self.do_encode_debug(dst)?;
                res
            }
            Frame::Error { error, .. } => Err(error),
            Frame::Body { .. } => panic!("Streaming of Requests is not currently supported"),
        }
    }
}

fn assert_stream_id(id: u16) {
    // TODO This should not be an assertion, but just a result to be returned.
    // The actual goal is to gain control over the domain of our request IDs, which right
    // now is not present when clients use the service call interface.
    // This should only be possible if there are more than i16::max_value() requests in flight!
    assert!(id as i16 > -1,
            "stream-id {} was negative, which makes it a broadcast id with a special meaning",
            id);
}

fn decode_complete_message_by_opcode(version: ProtocolVersion,
                                     code: OpCode,
                                     buf: BytesMut)
                                     -> response::Result<response::Message> {
    use codec::header::OpCode::*;
    Ok(match code {
           Supported => response::Message::Supported(response::SupportedMessage::decode(version, buf)?),
           Ready => response::Message::Ready,
           Authenticate => response::Message::Authenticate(response::AuthenticateMessage::decode(version, buf)?),
           AuthSuccess => response::Message::AuthSuccess(response::AuthSuccessMessage::decode(version, buf)?),
           Error => response::Message::Error(response::ErrorMessage::decode(version, buf)?),
           _ => unimplemented!(),
       })
}
