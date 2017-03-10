use super::super::errors::*;
use super::super::args::ConnectionOptions;
use super::query::Demo;

use std::io;
use std::rc::Rc;

use linefeed::{Completion, Completer, ReadResult, Reader};
use linefeed::Terminal;
use tokio_core::reactor::Core;
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

fn execute<T: Terminal>(rd: &mut Reader<T>, _client: &mut ClientHandle, core: &mut Core, query: String) -> Result<()> {
    {
        use futures::future;
        let req = future::lazy(|| {
            prompt(rd, Busy);
            Ok::<_, ()>({
                            let mut d = Demo::default();
                            d.description = query;
                            d
                        })
        });
        match core.run(req) {
            Ok(result) => {
                let s = io::stdout();
                let mut lio = s.lock();
                ::serde_yaml::to_writer(&mut lio, &result)?;
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

pub fn interactive<T: Terminal>(mut rd: Reader<T>,
                                opts: ConnectionOptions,
                                initial_query: Option<String>)
                                -> Result<()> {

    rd.set_completer(Rc::new(CqlCompleter));
    prompt(&mut rd, Idle);

    let (mut core, client) = opts.connect();
    let mut client = core.run(client)?;

    if let Some(query) = initial_query {
        execute(&mut rd, &mut client, &mut core, query)?;
    }

    while let Ok(res) = rd.read_line() {
        match res {
            ReadResult::Eof => {
                println!();
                break;
            }
            ReadResult::Input(line) => {
                execute(&mut rd, &mut client, &mut core, line)?;
            }
            ReadResult::Signal(sig) => {
                println!();
                println!("signal received: {:?} - FIXME/TBD: do we need to shut anything down?",
                         sig);
            }
        }
    }
    Ok(())
}

struct CqlCompleter;

impl<T: Terminal> Completer<T> for CqlCompleter {
    fn complete(&self, word: &str, reader: &Reader<T>, start: usize, _end: usize) -> Option<Vec<Completion>> {
        let line = reader.buffer();
        let mut words = line[..start].split_whitespace();
        println!("word = {:?}", word);
        println!("start = {:?}", start);
        println!("_end = {:?}", _end);

        match words.next() {
            // Complete command name
            None => {
                let compls = Vec::new();

                Some(compls)
            }
            // Complete command parameters
            Some("get") | Some("set") => None,
            _ => None,
        }
    }
}
