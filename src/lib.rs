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

extern crate tokio_core;
extern crate tokio_proto;
#[macro_use]
extern crate nom;

#[cfg(test)]
#[macro_use]
extern crate nom_test_helpers;

pub mod codec;
pub mod adapter;
