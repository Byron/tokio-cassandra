[![Build Status linux+osx](https://travis-ci.org/nhellwig/tokio-cassandra.svg?branch=master)](https://travis-ci.org/nhellwig/tokio-cassandra)
[![crates.io version](https://img.shields.io/crates/v/tokio-cassandra.svg)](https://crates.io/crates/tokio-cassandra)

A Cassandra Native Protocol 3 implementation using Tokio for IO.

# Usage

Add this to your Cargo.toml
```toml
[dependencies]
tokio-cassandra = "*"
```

Add this to your lib ...
```Rust
extern crate tokio_cassandra;
```

# Goals
* implement cassandra v3 protocol leveraging the tokio ecosystem to the fullest. Stream as much as possible to reduce the amount of copies to a minium.
* safety first - the client will verify all input received from the server
* leave it flexible enough to easily provide support for protocol version 4
* test-first development - no code exists unless a test needs it to pass


# Library Status
* **Multi-Protocol Support**
  * [ ] [architecture and API](https://github.com/nhellwig/tokio-cassandra/issues/4)
  * [ ] version 4
* **Transport**
  * **Multiplexed**
    * [x] non-streaming
    * [ ] [streaming](https://github.com/nhellwig/tokio-cassandra/issues/3)
  * [x] unencrypted
  * [ ] encryption via TLS
* **Connection**
  * [x] unauthenticated
  * [ ] authenticated
* **Codec**
  * [x] frame-header
  * **Message Data Types (MDT)**
    * [x] int
    * [x] long
    * [x] short
    * [x] string
    * [x] long string
    * [ ] [uuid](https://github.com/nhellwig/tokio-cassandra/projects/2#card-1774756)
    * [ ] [option](https://github.com/nhellwig/tokio-cassandra/projects/2#card-1774765)
    * [ ] [option list](https://github.com/nhellwig/tokio-cassandra/projects/2#card-1774766)
    * [ ] [inet](https://github.com/nhellwig/tokio-cassandra/projects/2#card-1774767)
    * [ ] [consistency](https://github.com/nhellwig/tokio-cassandra/projects/2#card-1774768)
    * [x] string map
    * [x] string multi-map
  * **Messages**
    * [ ] Paging
    * **Compression**
      * [ ] Snappy
      * [ ] LZ4
    * **Requests**
      * [x] Startup
      * [ ] Auth-Response
      * [x] Options
      * [ ] Query
      * [ ] Prepare
      * [ ] Execute
      * [ ] Batch
      * [ ] Register
    * **Responses**
      * [ ] Error
      * [x] Ready
      * [ ] Authenticate
      * [x] Supported
      * [ ] Event
      * [ ] Auth-Challenge
      * [ ] Auth-Success
      * **Result**
        * [ ] Void
        * [ ] Rows
        * [ ] Set-Keyspace
        * [ ] Prepared
        * [ ] Schema-Change
  * **Data Serialization Formats**
    * [ ] ascii
    * [ ] big-int
    * [ ] blob
    * [ ] boolean
    * [ ] decimal
    * [ ] double
    * [ ] float
    * [ ] inet
    * [ ] int
    * [ ] list
    * [ ] map
    * [ ] set
    * [ ] text
    * [ ] timestamp
    * [ ] uuid
    * [ ] varchar
    * [ ] varint
    * [ ] timeuuid
    * [ ] tuple
    * [ ] UDT (User Defined Type)

# Commandline Interface Status
* **test-connection**
  * [x] unauthenticated
  * [ ] authenticated
  * [ ] with-TLS
  * [ ] choice of cql version to use
  * [ ] choice of which protocol version to use
  * [x] use latest-supported cql version