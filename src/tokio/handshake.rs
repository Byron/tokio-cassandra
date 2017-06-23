use codec::request;
use codec::response;
use codec::authentication::{Authenticator, Credentials};
use codec::primitives::{CqlString, CqlBytes, CqlFrom};
use bytes::BytesMut;
use tokio_service::Service;
use futures::{future, Future};
use semver;

use super::error::{Error, Result, ErrorKind, ResultExt};
use super::client::ClientHandle;
use super::messages::StreamingMessage;

// TODO: prevent infinite recursion on malformed input
pub fn interpret_response_and_handle(
    handle: ClientHandle,
    res: StreamingMessage,
    creds: Option<Credentials>,
    desired_cql_version: Option<semver::Version>,
) -> Box<Future<Item = ClientHandle, Error = Error>> {
    let res: response::Message = res.into();
    match res {
        response::Message::Supported(msg) => {
            let startup = startup_message_from_supported(msg, desired_cql_version.as_ref());
            let f = future::done(startup).and_then(|s| {
                handle.call(s).map_err(|e| e.into()).map(|r| (r, handle))
            });
            Box::new(
                f.and_then(move |(res, ch)| {
                    interpret_response_and_handle(ch, res, creds, desired_cql_version)
                }).and_then(|ch| Ok(ch)),
            )
        }
        response::Message::Authenticate(msg) => {
            let auth_response = auth_response_from_authenticate(creds.clone(), msg);
            let f = future::done(auth_response).and_then(|s| {
                handle.call(s).map_err(|e| e.into()).map(|r| (r, handle))
            });
            Box::new(
                f.and_then(move |(res, ch)| {
                    interpret_response_and_handle(ch, res, creds, desired_cql_version)
                }).and_then(|ch| Ok(ch)),
            )
        }
        response::Message::Ready => Box::new(future::ok(handle)),
        response::Message::AuthSuccess(msg) => {
            debug!("Authentication Succeded: {:?}", msg);
            Box::new(future::ok(handle))
        }
        response::Message::Error(msg) => Box::new(future::err(
            ErrorKind::CqlError(msg.code, msg.text.into()).into(),
        )),
        msg => {
            Box::new(future::err(
                ErrorKind::HandshakeError(format!(
                    "Did not expect to receive \
                                                                    the following message {:?}",
                    msg
                )).into(),
            ))
        }
    }


}

fn startup_message_from_supported(
    msg: response::SupportedMessage,
    dv: Option<&semver::Version>,
) -> Result<request::Message> {
    let startup = {
        request::StartupMessage {
            cql_version: dv.map(|v| {
                CqlString::try_from(&v.to_string()).expect("semver to be unicode compatible")
            }).or_else(|| msg.latest_cql_version().cloned())
                .ok_or(ErrorKind::HandshakeError(
                    "Expected CQL_VERSION to contain at least one version"
                        .into(),
                ))?
                .clone(),
            compression: None,
        }
    };

    debug!("startup message generated: {:?}", startup);
    Ok(request::Message::Startup(startup))
}

fn auth_response_from_authenticate(
    creds: Option<Credentials>,
    msg: response::AuthenticateMessage,
) -> Result<request::Message> {
    let creds = creds.ok_or(ErrorKind::HandshakeError(format!(
        "No credentials provided but\
                                                        server requires authentication \
                                                      by {}",
        msg.authenticator.as_ref()
    )))?;

    let authenticator = Authenticator::from_name(msg.authenticator.as_ref(), creds)
        .chain_err(|| "Authenticator Err")?;

    let mut buf = BytesMut::with_capacity(128);
    authenticator.encode_auth_response(&mut buf);

    Ok(request::Message::AuthResponse(
        request::AuthResponseMessage {
            auth_data: CqlBytes::try_from(buf).chain_err(|| "Message Err")?,
        },
    ))
}
