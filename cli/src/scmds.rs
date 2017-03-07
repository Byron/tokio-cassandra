mod query {
    use clap;
    use super::super::args::ConnectionOptions;
    use super::super::errors::*;
    use tokio_cassandra::codec::primitives::{CqlFrom, CqlLongString};
    use std::fs::File;
    use std::io::{self, Read};

    struct Options {
        file_content: String,
        execute: String,
        keyspace: Option<String>,
    }

    impl Options {
        fn try_from(args: &clap::ArgMatches) -> Result<Options> {
            Ok(Options {
                file_content: match args.value_of("file") {
                    None => String::new(),
                    Some(fp) => {
                        let mut f: Box<Read> = match fp {
                            "-" => Box::new(io::stdin()),
                            _ => Box::new(File::open(&fp)
                                .chain_err(|| format!("Failed to open CQL file at '{}' for reading", fp))?),
                        };
                        let mut buf = String::new();
                        f.read_to_string(&mut buf)?;
                        buf
                    }
                },
                execute: args.value_of("execute").map(Into::into).unwrap_or_default(),
                keyspace: args.value_of("keyspace").map(Into::into),
            })
        }

        fn try_into_query_string(self) -> Result<String> {
            fn trim(mut s: String) -> String {
                let len;
                {
                    let trimmed = s.trim_right();
                    len = trimmed.len();
                }
                s.truncate(len);
                s
            }
            fn ensure_statement(mut s: String) -> String {
                if !s.is_empty() && !s.ends_with(';') {
                    s.push(';');
                }
                s
            }
            fn sanitize(s: String) -> String {
                ensure_statement(trim(s))
            }
            fn extend_sanitized(mut t: String, f: String) -> String {
                let mut f = sanitize(f);
                t.extend(f.drain(..));
                t
            }
            let mut q = String::new();
            if let Some(ks) = self.keyspace {
                // FIXME: This can be used for CQL-injection. Is there a better way? Should this
                // be a query parameter? Is this even an issue for our use-case? After all files
                // can be read too ... .
                q.push_str(&format!("use {}; ", ks))
            }

            q = extend_sanitized(q, self.file_content);
            q = extend_sanitized(q, self.execute);
            q = sanitize(q);

            if q.len() == 0 {
                bail!("Query cannot be empty")
            }

            Ok(q)
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
