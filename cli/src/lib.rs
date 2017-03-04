extern crate clap;

#[macro_use]
extern crate error_chain;
extern crate tokio_cassandra;
extern crate tokio_core;
extern crate tokio_service;
extern crate futures;

pub mod errors {
    use std::num::ParseIntError;
    use std::net::AddrParseError;
    use std::io;

    error_chain!{
        foreign_links {
            ParseInt(ParseIntError);
            AddrParse(AddrParseError);
            Other(io::Error);
        }
    }
}

pub mod args {
    use clap;
    use super::errors::*;
    use std::net::SocketAddr;
    use futures::Future;
    use tokio_cassandra::streaming::{self, ClientHandle, CqlCodecDebuggingOptions, CqlProto, Client};
    use tokio_cassandra::ssl;
    use tokio_cassandra::codec::authentication::Credentials;
    use tokio_cassandra::codec::header::ProtocolVersion;
    use tokio_core::reactor::Core;

    pub struct ConnectionOptions {
        pub client: Client,
        pub addr: SocketAddr,
        pub creds: Option<Credentials>,
        pub tls: Option<ssl::Options>,
    }

    impl ConnectionOptions {
        pub fn from(args: &clap::ArgMatches) -> Result<ConnectionOptions> {
            let host = args.value_of("host").expect("clap to work");
            let port = args.value_of("port").expect("clap to work");
            let port: u16 = port.parse()
                .chain_err(|| format!("Port '{}' could not be parsed as number", port))?;
            let addr = format!("{}:{}", host, port).parse()
                .chain_err(|| format!("Host '{}' could not be parsed as IP", host))?;
            let debug = args.value_of("debug-dump-frames-into-directory")
                .map(|p| CqlCodecDebuggingOptions { dump_frames_into: Some(p.into()), ..Default::default() });

            let creds = {
                if let (Some(usr), Some(pwd)) = (args.value_of("user"), args.value_of("password")) {
                    Some(Credentials::Login {
                        username: usr.to_string(),
                        password: pwd.to_string(),
                    })
                } else {
                    None
                }
            };

            Ok(ConnectionOptions {
                client: Client {
                    protocol: CqlProto {
                        version: ProtocolVersion::Version3,
                        debug: debug,
                    },
                },
                tls: None,
                addr: addr,
                creds: creds,
            })
        }

        pub fn connect(self) -> (Core, Box<Future<Item = ClientHandle, Error = streaming::Error>>) {
            let core = Core::new().expect("Core can be created");
            let handle = core.handle();
            let client = self.client.connect(&self.addr, &handle, self.creds, self.tls);
            (core, client)
        }
    }
}


mod scmds {
    use clap;
    use super::args::ConnectionOptions;
    use super::errors::*;

    pub fn test_connection(opts: ConnectionOptions, _args: &clap::ArgMatches) -> Result<()> {
        let addr = opts.addr.clone();
        let (mut core, client) = opts.connect();
        core.run(client)
            .chain_err(|| format!("Failed to connect to {}", addr))
            .map(|_response| {
                println!("Connection to {} successful", addr);
                ()
            })
            .map_err(|e| e.into())
    }

}

pub use self::scmds::*;
pub use self::args::*;
