use super::super::errors::*;
use super::super::args::ConnectionOptions;
use super::utils::Demo;

use clap;
use std::rc::Rc;
use std::ascii::AsciiExt;

use linefeed::{Completion, Completer, ReadResult, Reader};
use linefeed::Terminal;
use tokio_core::reactor::Core;
use tokio_cassandra::tokio::client::ClientHandle;
use super::utils::{output_result, OutputFormat};

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
    _client: &mut ClientHandle,
    core: &mut Core,
    query: String,
    args: &clap::ArgMatches,
) -> Result<()> {
    {
        use futures::future;
        let req = future::lazy(|| {
            prompt(rd, Busy);
            Ok::<_, ()>({
                let mut d = Demo::default();
                d.description = query;
                ::std::thread::sleep(::std::time::Duration::from_millis(100));
                d
            })
        });
        match core.run(req) {
            Ok(result) => {
                output_result(&result, OutputFormat::yaml, args)?;
                println!();
            }
            Err(err) => {
                println!("{:?}", err);
            }
        }
    }

    prompt(rd, Idle);
    Ok(())
}

pub fn interactive<T: Terminal>(
    mut rd: Reader<T>,
    opts: ConnectionOptions,
    initial_query: Option<String>,
    args: &clap::ArgMatches,
) -> Result<()> {

    rd.set_completer(Rc::new(CqlCompleter));
    prompt(&mut rd, Idle);

    let (mut core, client) = opts.connect();
    let mut client = core.run(client)?;

    if let Some(query) = initial_query {
        execute(&mut rd, &mut client, &mut core, query, args)?;
    }

    while let Ok(res) = rd.read_line() {
        match res {
            ReadResult::Eof => {
                println!();
                break;
            }
            ReadResult::Input(line) => {
                rd.add_history(line.to_owned());
                execute(&mut rd, &mut client, &mut core, line, args)?;
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
