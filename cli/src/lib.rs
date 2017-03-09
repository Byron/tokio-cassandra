#[macro_use]
extern crate clap;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde_yaml;

extern crate linefeed;

#[macro_use]
extern crate error_chain;
extern crate tokio_cassandra;
extern crate tokio_core;
extern crate tokio_service;
extern crate futures;
extern crate dns_lookup;
extern crate semver;

pub mod errors {
    use std::num::ParseIntError;
    use std::net::AddrParseError;
    use semver::SemVerError;
    use tokio_cassandra::codec;
    use tokio_cassandra::tokio;
    use std::io;

    error_chain!{
// FIXME: use links {} instead - however, failed for me.
//        links {
//            CodecPrimitive(codec::primitives::Error, codec::primitives::ErrorKind);
//        }
        foreign_links {
            ParseInt(ParseIntError);
            AddrParse(AddrParseError);
            SemVerParse(SemVerError);
            // FIXME: use links {} instead - however, failed for me.
            CodecPrimitive(codec::primitives::Error);
            // FIXME: use links {} instead - however, failed for me.
            Tokio(tokio::error::Error);
            SerdeJson(::serde_json::Error);
            SerdeYaml(::serde_yaml::Error);
            Other(io::Error);
        }

        errors {
            Pk12PathFormat(s: String) {
                description("Could not parse pk12 file path description: <path>:<password> is required")
                display("Failed to parse '{}' as <path>:<password>", s)
            }
        }
    }
}

mod args;
mod scmds;

pub use self::scmds::*;
pub use self::args::*;
