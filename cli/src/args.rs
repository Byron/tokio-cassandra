use clap;
use super::errors::*;
use std::net::{self, SocketAddr};
use std::str::{self, FromStr};
use std::fs::File;
use std::io::Read;
use futures::Future;
use tokio_cassandra::streaming::{self, ClientHandle, CqlCodecDebuggingOptions, CqlProto, Client};
use tokio_cassandra::ssl;
use tokio_cassandra::codec::authentication::Credentials;
use tokio_cassandra::codec::header::ProtocolVersion;
use tokio_core::reactor::Core;
use dns_lookup::lookup_host;

pub struct ConnectionOptions {
    pub client: Client,
    pub addr: SocketAddr,
    pub host: String,
    pub port: u16,
    pub creds: Option<Credentials>,
    pub tls: Option<ssl::Options>,
}

struct Pk12WithOptionalPassword {
    content: Vec<u8>,
    password: String,
}

impl From<Pk12WithOptionalPassword> for ssl::Credentials {
    fn from(pk12: Pk12WithOptionalPassword) -> Self {
        ssl::Credentials::Pk12 {
            contents: pk12.content,
            passphrase: pk12.password,
        }
    }
}

impl FromStr for Pk12WithOptionalPassword {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut it = s.as_bytes().splitn(2, |&b| b == b':');
        match (it.next(), it.next()) {
            (Some(fp), Some(password)) => {
                let fp = str::from_utf8(fp).expect("str -> bytes -> str to work");
                let mut buf = Vec::new();
                File::open(fp).chain_err(|| format!("Failed to open file at '{}'", fp))?.read_to_end(&mut buf)?;
                Ok(Pk12WithOptionalPassword {
                    content: buf,
                    password: str::from_utf8(password).expect("str -> bytes -> str to work").into(),
                })
            }
            _ => {
                bail!(ErrorKind::Pk12PathFormat(format!("Need two tokens formatted like <path>:<password>, got '{}'",
                                                        s)))
            }
        }
    }
}

impl ConnectionOptions {
    pub fn try_from(args: &clap::ArgMatches) -> Result<ConnectionOptions> {
        let host = args.value_of("host").expect("clap to work");
        let port = args.value_of("port").expect("clap to work");
        let port: u16 = port.parse()
            .chain_err(|| format!("Port '{}' could not be parsed as number", port))?;
        Ok(ConnectionOptions {
            host: host.into(),
            port: port,
            client: Client {
                protocol: CqlProto {
                    version: ProtocolVersion::Version3,
                    debug: match (args.value_of("debug-dump-encoded-frames-into-directory"),
                                  args.value_of("debug-dump-decoded-frames-into-directory")) {
                        (None, None) => None,
                        (encode_path, decode_path) => {
                            Some(CqlCodecDebuggingOptions {
                                dump_encoded_frames_into: encode_path.map(Into::into),
                                dump_decoded_frames_into: decode_path.map(Into::into),
                                ..Default::default()
                            })
                        }
                    },
                },
            },
            tls: match (args.is_present("tls"), args.value_of("cert"), args.value_of("ca-file")) {
                (true, cert, ca_file) |
                (false, cert @ Some(_), ca_file) |
                (false, cert, ca_file @ Some(_)) => {
                    Some(ssl::Options {
                        domain: match net::IpAddr::from_str(host) {
                            Ok(ip) => {
                                bail!(format!("When using TLS, the host name must not be an IP address, got {}",
                                              ip))
                            }
                            Err(_) => host.into(),
                        },

                        configuration: ssl::Configuration::Predefined(ssl::EasyConfiguration {
                            credentials: match cert {
                                Some(s) => {
                                    Some(Pk12WithOptionalPassword::from_str(s)
                                        .chain_err(|| {
                                            format!("Failed to interpret Pk12 file with password from '{}'", s)
                                        })?
                                        .into())
                                }
                                None => None,
                            },
                            certificate_authority_file: ca_file.map(String::from),
                        }),
                    })
                }
                _ => None,
            },
            addr: {
                net::IpAddr::from_str(host).or_else(|parse_err| {
                        lookup_host(host)
                            .map_err(|err| {
                                Error::from_kind(format!("Failed to parse '{}' with error: {:?} and could not lookup \
                                                          host with error {:?}",
                                                         host,
                                                         parse_err,
                                                         err)
                                    .into())
                            })
                            .and_then(|mut it| {
                                it.next()
                                    .ok_or_else(|| {
                                        Error::from_kind(format!("Not a single IP found for host '{}', even though \
                                                                  lookup succeeded",
                                                                 host)
                                            .into())
                                    })
                                    .and_then(|res| res.map_err(Into::into))
                            })
                    })
                    .map(|ip| SocketAddr::new(ip, port))?
            },
            creds: match (args.value_of("user"), args.value_of("password")) {
                (Some(usr), Some(pwd)) => {
                    Some(Credentials::Login {
                        username: usr.to_string(),
                        password: pwd.to_string(),
                    })
                }
                _ => None,
            },
        })
    }

    pub fn connect(self) -> (Core, Box<Future<Item = ClientHandle, Error = streaming::Error>>) {
        let core = Core::new().expect("Core can be created");
        let handle = core.handle();
        let client = self.client.connect(&self.addr, &handle, self.creds, self.tls);
        (core, client)
    }
}
