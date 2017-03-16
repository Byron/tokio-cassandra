use super::*;

use codec::primitives::{CqlFrom, CqlString, CqlBytes, CqlStringList, CqlStringMultiMap};
use codec::header::ProtocolVersion;
use codec::primitives::decode;
use bytes::BytesMut;
use semver::Version;

#[derive(Debug)]
pub struct SupportedMessage(pub CqlStringMultiMap<BytesMut>);

impl SupportedMessage {
    pub fn cql_versions(&self) -> Option<&CqlStringList<BytesMut>> {
        self.0.get(unsafe { &CqlString::unchecked_from("CQL_VERSION") })
    }

    pub fn compression(&self) -> Option<&CqlStringList<BytesMut>> {
        self.0.get(unsafe { &CqlString::unchecked_from("COMPRESSION") })
    }

    pub fn latest_cql_version(&self) -> Option<&CqlString<BytesMut>> {
        self.cql_versions().and_then(|lst| {
                                         lst.iter()
                                             .filter_map(|v| Version::parse(v.as_ref()).ok().map(|vp| (vp, v)))
                                             .max_by_key(|t| t.0.clone())
                                             .map(|(_vp, v)| v)
                                     })
    }
}

impl CqlDecode<SupportedMessage> for SupportedMessage {
    fn decode(_v: ProtocolVersion, buf: BytesMut) -> Result<SupportedMessage> {
        decode::string_multimap(buf).map(|d| d.1.into()).map_err(|err| {
                                                                     ErrorKind::ParserError(format!("{}", err)).into()
                                                                 })
    }
}

impl From<CqlStringMultiMap<BytesMut>> for SupportedMessage {
    fn from(v: CqlStringMultiMap<BytesMut>) -> Self {
        SupportedMessage(v)
    }
}

#[derive(Debug)]
pub struct AuthenticateMessage {
    pub authenticator: CqlString<BytesMut>,
}

impl CqlDecode<AuthenticateMessage> for AuthenticateMessage {
    fn decode(_v: ProtocolVersion, buf: BytesMut) -> Result<AuthenticateMessage> {
        decode::string(buf)
            .map(|d| AuthenticateMessage { authenticator: d.1 })
            .map_err(|err| ErrorKind::ParserError(format!("{}", err)).into())
    }
}

#[derive(Debug)]
pub struct AuthSuccessMessage {
    pub payload: CqlBytes<BytesMut>,
}

impl CqlDecode<AuthSuccessMessage> for AuthSuccessMessage {
    fn decode(_v: ProtocolVersion, buf: BytesMut) -> Result<AuthSuccessMessage> {
        decode::bytes(buf).map(|d| AuthSuccessMessage { payload: d.1 }).map_err(|err| {
                                                                                    ErrorKind::ParserError(format!("{}",
                                                                                                                   err))
                                                                                            .into()
                                                                                })
    }
}

#[derive(Debug)]
pub struct ErrorMessage {
    pub code: i32,
    pub text: CqlString<BytesMut>,
}

impl CqlDecode<ErrorMessage> for ErrorMessage {
    fn decode(_v: ProtocolVersion, buf: BytesMut) -> Result<ErrorMessage> {
        let (buf, code) = decode::int(buf)?;
        let (_, text) = decode::string(buf)?;
        Ok(ErrorMessage {
               code: code,
               text: text,
           })
    }
}


#[cfg(test)]
mod test {
    use codec::header::Header;
    use codec::header::ProtocolVersion::*;
    use codec::primitives::{CqlStringMultiMap, CqlStringList, CqlString};
    use super::*;

    fn skip_header(b: &[u8]) -> &[u8] {
        &b[Header::encoded_len()..]
    }

    #[test]
    fn decode_supported_message() {
        let msg = include_bytes!("../../../tests/fixtures/v3/responses/supported.msg");
        let buf = Vec::from(skip_header(&msg[..])).into();
        let res = SupportedMessage::decode(Version3, buf).unwrap();

        let sla = ["3.2.1"];
        let slb = ["snappy", "lz4"];
        let csl1 = CqlStringList::try_from_iter_easy(sla.iter().cloned()).unwrap();
        let csl2 = CqlStringList::try_from_iter_easy(slb.iter().cloned()).unwrap();

        assert_eq!(res.cql_versions().unwrap(), &csl1);
        assert_eq!(res.compression().unwrap(), &csl2);
    }

    #[test]
    fn supported_message_latest_cql_version() {
        let versions = ["3.2.1", "3.1.2", "4.0.1"];
        let vm = CqlStringList::try_from_iter_easy(versions.iter().cloned()).unwrap();
        let smm = CqlStringMultiMap::try_from_iter(vec![(CqlString::try_from("CQL_VERSION").unwrap(), vm)]).unwrap();
        let msg = SupportedMessage::from(smm);

        assert_eq!(msg.latest_cql_version(),
                   Some(&CqlString::try_from("4.0.1").unwrap()));
    }

    #[test]
    fn decode_authenticate_message() {
        let msg = include_bytes!("../../../tests/fixtures/v3/responses/authenticate.msg");
        let buf = Vec::from(skip_header(&msg[..])).into();
        let res = AuthenticateMessage::decode(Version3, buf).unwrap();

        let authenticator = CqlString::try_from("org.apache.cassandra.auth.PasswordAuthenticator").unwrap();

        assert_eq!(res.authenticator, authenticator);
    }

    #[test]
    fn decode_auth_success_message() {
        let msg = include_bytes!("../../../tests/fixtures/v3/responses/auth_success.msg");
        let buf = Vec::from(skip_header(&msg[..])).into();
        let res = AuthSuccessMessage::decode(Version3, buf).unwrap();

        assert_eq!(res.payload.as_bytes(), None);
    }

    #[test]
    fn decode_error_message() {
        let msg = include_bytes!("../../../tests/fixtures/v3/responses/error_credentials.msg");
        let buf = Vec::from(skip_header(&msg[..])).into();
        let res = ErrorMessage::decode(Version3, buf).unwrap();

        assert_eq!(res.code, 256);
        assert_eq!(res.text,
                   CqlString::try_from("Username and/or password are incorrect").unwrap());
    }
}
