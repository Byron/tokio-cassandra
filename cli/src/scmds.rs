mod query {
    use clap;
    use super::super::args::ConnectionOptions;
    use super::super::errors::*;
    use tokio_cassandra::codec::primitives::{CqlFrom, CqlLongString};

    struct Options {
        execute: String,
        keyspace: Option<String>
    }

    impl Options {
        fn try_from(args: &clap::ArgMatches) -> Result<Options> {
            Ok(Options {
                execute: args.value_of("execute").map(Into::into).unwrap_or_else(Default::default),
                keyspace: args.value_of("keyspace").map(Into::into)
            })
        }

        fn try_into_query_string(self) -> Result<String> {
            let mut q = String::new();
            if let Some(ks) = self.keyspace {
                q.push_str(&format!("use {}; ", ks))
            }
            q.push_str(&self.execute);
            if q.len() == 0 {
                bail!("Query cannot be empty")
            } else {
                if !q.ends_with(';') {
                    q.push(';');
                }
                Ok(q)
            }
        }
    }

    pub fn query(opts: ConnectionOptions, args: &clap::ArgMatches) -> Result<()> {
        let addr = format!("{}:{}", opts.host, opts.port);
        let query = Options::try_from(args)?.try_into_query_string()?;
        let (mut core, client) = opts.connect();
        core.run(client)
            .chain_err(|| format!("Failed to connect to {}", addr))
            .and_then(|_client| {
                if args.is_present("dry-run") {
                    println!("{}", query);
                } else {
                    let _query = CqlLongString::<Vec<u8>>::try_from(&query)?;
                    unimplemented!();
                }
                Ok(())
            })
            .map_err(|e| e.into())
    }
}

mod testcon {
    use clap;
    use super::super::args::ConnectionOptions;
    use super::super::errors::*;

    pub fn test_connection(opts: ConnectionOptions, _args: &clap::ArgMatches) -> Result<()> {
        let addr = format!("{}:{}", opts.host, opts.port);
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

pub use self::testcon::*;
pub use self::query::*;
