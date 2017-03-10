use clap;
use linefeed;
use super::super::args::ConnectionOptions;
use super::super::errors::*;
use super::utils::{output_result, OutputFormat, Demo};
use super::shell;
use tokio_cassandra::codec::primitives::{CqlFrom, CqlLongString};
use std::fs::File;
use std::io::{self, Read};

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
                _ => {
                    Box::new(File::open(&fp).chain_err(|| format!("Failed to open CQL file at '{}' for reading", fp))?)
                }
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

    fn into_query_string(self) -> Option<String> {
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
            return None;
        }

        Some(q)
    }
}

pub fn query(opts: ConnectionOptions, args: &clap::ArgMatches) -> Result<()> {
    let addr = format!("{}:{}", opts.host, opts.port);
    let qopts = Options::try_from(args)?;
    let (interactive, query) = (qopts.interactive, qopts.into_query_string());

    let query = match (query, interactive, args.is_present("dry-run")) {
        (Some(query), _interactive, true) => {
            println!("{}", query);
            return Ok(());
        }
        (Some(query), false, false) => query,
        (query, true, false) => return shell::interactive(linefeed::Reader::new("cqlshell")?, opts, query),
        (None, _interactive, _dry_run) => bail!("Query cannot be empty"),
    };

    let (mut core, client) = opts.connect();
    core.run(client).chain_err(|| format!("Failed to connect to {}", addr)).and_then(|_client| {
        // FIXME: provide a consuming version stat consumes a string directly into the vec
        // and thus prevents an entirely unnecessary copy
        let _query = CqlLongString::<Vec<u8>>::try_from(&query)?;

        let s = io::stdout();
        let mut lio = s.lock();
        let demo = Demo::default();
        output_result(&mut lio,
                      &demo,
                      args.value_of("output-format")
                          .expect("clap to work")
                          .parse()
                          .expect("clap to work"))?;
        println!();
        Ok(())
    })
}