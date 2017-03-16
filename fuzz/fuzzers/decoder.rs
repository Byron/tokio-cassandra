#![no_main]
extern crate libfuzzer_sys;
extern crate tokio_cassandra;
extern crate tokio_core;

use tokio_cassandra::streaming::CqlCodec;
use tokio_cassandra::codec::header::ProtocolVersion;

#[export_name="rust_fuzzer_test_input"]
pub extern fn go(data: &[u8]) {
    let c = CqlCodec::new(ProtocolVersion::Version3, Default::default());
    
    // fuzzed code goes here
}
