use clap;
use linefeed;
use super::super::args::ConnectionOptions;
use super::super::errors::*;
use super::shell;
use tokio_cassandra::codec::primitives::{CqlFrom, CqlLongString};
use tokio_cassandra::codec::header::Header;
use std::fs::File;
use std::io::{self, Read};

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug)]
    pub enum OutputFormat {
        yaml,
        json
    }
}


struct Options {
    file_content: String,
    execute: String,
    keyspace: Option<String>,
    interactive: bool,
}

impl Options {
    fn try_from(args: &clap::ArgMatches) -> Result<Options> {
        Ok(Options {
            interactive: args.is_present("interactive"),
            file_content: match args.value_of("file") {
                None => String::new(),
                Some(fp) => {
                    let s = io::stdin();
                    let mut f: Box<Read> = match fp {
                        "-" => Box::new(s.lock()),
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

        if !self.interactive && q.len() == 0 {
            bail!("Query cannot be empty")
        }

        Ok(q)
    }
}

pub fn query(opts: ConnectionOptions, args: &clap::ArgMatches) -> Result<()> {
    let addr = format!("{}:{}", opts.host, opts.port);
    let qopts = Options::try_from(args)?;
    let reader = match qopts.interactive {
        true => Some((linefeed::Reader::new("cqlshell")?, opts.clone(), qopts.keyspace.clone())),
        false => None,
    };
    let query = qopts.try_into_query_string()?;

    if args.is_present("dry-run") {
        println!("{}", query);
        if reader.is_none() {
            return Ok(());
        }
    }

    let (mut core, client) = opts.connect();
    core.run(client)
        .chain_err(|| format!("Failed to connect to {}", addr))
        .and_then(|_client| {
            // FIXME: provide a consuming version stat consumes a string directly into the vec
            // and thus prevents an entirely unnecessary copy
            let _query = CqlLongString::<Vec<u8>>::try_from(&query)?;

            #[derive(Deserialize, Serialize)]
            struct Demo {
                result_example: Header,
                description: String,
            }
            let demo = Demo {
                result_example: Header::try_from(b"\x03\x02\x00\x00\x05\x00\x00\x00\x00").unwrap(),
                description: "I believe we need to implement the serde-traits manually on our response types to \
                              implement it in a controlled fashion without extra copies."
                    .into(),
            };
            let s = io::stdout();
            let mut lio = s.lock();
            match args.value_of("output-format").expect("clap to work").parse().expect("clap to work") {
                OutputFormat::json => ::serde_json::ser::to_writer_pretty(&mut lio, &demo)?,
                OutputFormat::yaml => ::serde_yaml::to_writer(&mut lio, &demo)?,
            }
            println!();
            Ok(())
        })?;

    if let Some((reader, opts, keyspace)) = reader {
        shell::interactive(reader, opts, keyspace)
    } else {
        Ok(())
    }
}
