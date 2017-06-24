<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
A Cassandra Native Protocol 3 implementation using Tokio for IO.

- [Goals](#goals)
- [Milestones](#milestones)
- [What we are working on](#what-we-are-working-on)
- [Want to contribute?](#want-to-contribute)
- [Usage](#usage)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

[![Build Status linux+osx](https://travis-ci.org/nhellwig/tokio-cassandra.svg?branch=master)](https://travis-ci.org/nhellwig/tokio-cassandra)
[![crates.io version](https://img.shields.io/crates/v/tokio-cassandra.svg)](https://crates.io/crates/tokio-cassandra)

# Goals
* implement cassandra v3 protocol leveraging the tokio ecosystem to the fullest.
* safety first - the client will verify all input received from the server.
* test-first development - no code exists unless a test needs it to pass.
* high-performance - stream as much as possible and reduce amount of allocations to a minimum.
* leave it flexible enough to easily provide support for protocol version 4 and later 5.
* develop breadth first - thus we are implementing orthogonal features first to learn how that affects the API and architecture, before implementing every single data-type or message-type.
* strive for an MVP and version 1.0 fast, even if that includes only the most common usecases.
* Prefer to increment major version rapidly instead of keeping major version zero for longer than needed.

# Milestones
1. [x] The first connection
1. [x] TLS Support
1. [x] Decode `UDT`s ,`Tuples`, `Rows`, `(Nested) Collections`, and everything returned by a query
1. [x] `Debug` trait for _CQL Datatypes_ to support outputting to console/file
1. [ ] Decode `Result` with support for chunking and full-body-at-once
1. [x] First query support for TUI, outputting result using serde
1. [x] Serde support for `Result` types
1. [v1.0 Minimal Viable Product](https://github.com/nhellwig/tokio-cassandra/milestone/2)

# What we are working on

We are making [work][kanban] and [progress][progress] transparent by placing cards on the board.

# Want to contribute?

Helping should be easy, so there is not much process to follow. Just have a look at the [backlog][kanban], pick something up by creating an issue so we know you are working on it - that way we don't risk picking up the same. Write tests first and you should be good to send a PR with the implementation.

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

[kanban]: https://github.com/nhellwig/tokio-cassandra/projects/2
[progress]: https://github.com/nhellwig/tokio-cassandra/milestone/2
