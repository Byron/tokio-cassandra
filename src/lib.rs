#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate quick_error;
extern crate byteorder;

#[cfg(feature = "with-serde")]
extern crate serde;

#[cfg(feature = "with-serde")]
#[macro_use]
extern crate serde_derive;

#[cfg_attr(feature = "with-openssl", macro_use)]
extern crate futures;
extern crate tokio_core;
extern crate tokio_service;
extern crate tokio_proto;
extern crate tokio_io;
extern crate bytes;
extern crate chrono;

#[cfg(feature = "with-openssl")]
extern crate tokio_openssl;
#[cfg(feature = "with-openssl")]
extern crate openssl;

extern crate semver;

#[macro_use]
extern crate log;

extern crate num_bigint;

#[macro_use]
mod macros;

pub mod codec;
pub mod tokio;
