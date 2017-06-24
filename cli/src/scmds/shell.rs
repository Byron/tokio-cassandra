use super::super::errors::{ResultExt, Result};
use super::super::args::ConnectionOptions;
use super::utils::{handle_call_result, request_from_query};

use std::io::{Write, stderr};
use clap;
use std::rc::Rc;
use std::ascii::AsciiExt;

use linefeed::{Completion, Completer, ReadResult, Reader};
use linefeed::Terminal;
use tokio_core::reactor::Core;
use tokio_service::Service;
use tokio_cassandra::tokio::client::ClientHandle;

enum PromptKind {
    Idle,
    Busy,
}
use self::PromptKind::*;

fn prompt<T: Terminal>(rd: &mut Reader<T>, s: PromptKind) {
    rd.set_prompt(match s {
        Idle => "cql > ",
        Busy => "cql ! ",
    });
}

fn execute<T: Terminal>(
    rd: &mut Reader<T>,
    client: &mut ClientHandle,
    core: &mut Core,
    query: &str,
    args: &clap::ArgMatches,
) -> Result<()> {
    prompt(rd, Busy);

    let query_req = request_from_query(query)?;
    let req = client.call(query_req);
    let res = core.run(req).map_err(Into::into).and_then(|res| {
        handle_call_result(res, args)
    });

    prompt(rd, Idle);

    res
}

pub fn interactive<T: Terminal>(
    mut rd: Reader<T>,
    opts: ConnectionOptions,
    initial_query: Option<String>,
    args: &clap::ArgMatches,
) -> Result<()> {

    rd.set_completer(Rc::new(CqlCompleter));
    prompt(&mut rd, Idle);
    let addr = opts.addr.clone();

    let (mut core, client) = opts.connect();
    let mut client = core.run(client).chain_err(
        || format!("failed to connect to {}", addr),
    )?;

    if let Some(query) = initial_query {
        execute(&mut rd, &mut client, &mut core, &query, args)
            .chain_err(|| format!("Initial query failed '{}'", query))?;
    }

    while let Ok(res) = rd.read_line() {
        match res {
            ReadResult::Eof => {
                println!();
                break;
            }
            ReadResult::Input(line) => {
                if line.len() > 0 {
                    rd.add_history(line.to_owned());
                    execute(&mut rd, &mut client, &mut core, &line, args)
                        .map_err(|err| { writeln!(stderr(), "{}", err).ok(); })
                        .ok();
                }
            }
            ReadResult::Signal(sig) => {
                println!();
                println!(
                    "signal received: {:?} - FIXME/TBD: do we need to shut anything down?",
                    sig
                );
            }
        }
    }
    Ok(())
}

struct CqlCompleter;
const CQL_KEYWORDS: &'static [&'static str] = &[
    "add",
    "all",
    "allow",
    "alter",
    "and",
    "any",
    "apply",
    "as",
    "asc",
    "ascii",
    "authorize",
    "batch",
    "begin",
    "bigint",
    "blob",
    "boolean",
    "by",
    "clustering",
    "columnfamily",
    "compact",
    "consistency",
    "count",
    "counter",
    "create",
    "custom",
    "decimal",
    "delete",
    "desc",
    "distinct",
    "double",
    "drop",
    "each_quorum",
    "exists",
    "filtering",
    "float",
    "from",
    "frozen",
    "full",
    "grant",
    "if",
    "in",
    "index",
    "inet",
    "infinity",
    "insert",
    "int",
    "into",
    "key",
    "keyspace",
    "keyspaces",
    "level",
    "limit",
    "list",
    "local_one",
    "local_quorum",
    "map",
    "modify",
    "nan",
    "nonrecursive",
    "nosuperuser",
    "not",
    "of",
    "on",
    "one",
    "order",
    "password",
    "permission",
    "permissions",
    "primary",
    "quorum",
    "rename",
    "revoke",
    "schema",
    "select",
    "set",
    "static",
    "storage",
    "superuser",
    "table",
    "text",
    "timestamp",
    "timeuuid",
    "three",
    "to",
    "token",
    "truncate",
    "ttl",
    "tuple",
    "two",
    "type",
    "unlogged",
    "update",
    "use",
    "user",
    "users",
    "using",
    "uuid",
    "values",
    "varchar",
    "varint",
    "where",
    "with",
    "writetime",
];

impl<T: Terminal> Completer<T> for CqlCompleter {
    fn complete(&self, word: &str, _reader: &Reader<T>, _start: usize, _end: usize) -> Option<Vec<Completion>> {
        let mut res = Vec::new();
        let word = word.to_ascii_lowercase();

        for kw in CQL_KEYWORDS {
            if kw.starts_with(&word) {
                res.push(Completion::simple(kw.to_string()));
            }
        }

        Some(res)
    }
}
