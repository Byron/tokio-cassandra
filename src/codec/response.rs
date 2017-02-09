use codec::header::Header;
use codec::primitives::{CqlStringMultiMap, decode};
use nom::IResult;

error_chain! {
    foreign_links {
        Io(::std::io::Error);
        HeaderError(::codec::header::Error);
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

#[derive(Debug, PartialEq)]
pub struct SupportedMessage<'a>(pub CqlStringMultiMap<'a>);

#[derive(Debug, PartialEq)]
pub enum Message<'a> {
    Supported(SupportedMessage<'a>),
}

#[derive(Debug, PartialEq)]
struct Frame<'a> {
    header: Header,
    body: Message<'a>,
}

impl<'a> CqlDecode<'a, SupportedMessage<'a>> for SupportedMessage<'a> {
    fn decode(buf: &'a [u8]) -> Result<DecodeResult<SupportedMessage<'a>>> {
        into_decode_result(decode::string_multimap(buf))
    }
}

impl<'a> From<CqlStringMultiMap<'a>> for SupportedMessage<'a> {
    fn from(v: CqlStringMultiMap<'a>) -> Self {
        SupportedMessage(v)
    }
}

#[derive(Debug)]
pub struct DecodeResult<T> {
    pub remaining_bytes: usize,
    pub decoded: T,
}

pub fn into_decode_result<'a, F, T>(r: IResult<&'a [u8], F, u32>) -> Result<DecodeResult<T>>
    where F: Into<T>
{
    match r {
        IResult::Done(buf, output) => {
            Ok(DecodeResult {
                decoded: output.into(),
                remaining_bytes: buf.len(),
            })
        }
        IResult::Error(err) => Err(ErrorKind::ParserError(format!("{}", err)).into()),
        IResult::Incomplete(err) => Err(ErrorKind::Incomplete(format!("{:?}", err)).into()),
    }
}

pub trait CqlDecode<'a, T> {
    fn decode(buf: &'a [u8]) -> Result<DecodeResult<T>>;
}


#[cfg(test)]
mod test {
    use codec::header::Header;
    use super::*;

    fn skip_header(b: &[u8]) -> &[u8] {
        &b[Header::encoded_len()..]
    }

    #[test]
    fn decode_supported_message() {
        let msg = include_bytes!("../../tests/fixtures/v3/responses/supported.msg");
        let res = SupportedMessage::decode(skip_header(&msg[..])).unwrap();
        println!("res = {:?}", res);

        // TODO: do actual asserts
    }
}
