use super::super::errors::*;
use super::super::args::ConnectionOptions;

use linefeed::{ReadResult, Reader};
use linefeed::Terminal;

pub fn interactive<T: Terminal>(mut rd: Reader<T>, _opts: ConnectionOptions, _initial_query: Option<String>) -> Result<()> {
    rd.set_prompt("cql > ");
    while let Ok(res) = rd.read_line() {
        match res {
            ReadResult::Eof => {
                println!();
                break;
            }
            ReadResult::Input(line) => {
                println!("{}", line);
            }
            ReadResult::Signal(sig) => {
                println!("");
                println!("signal received: {:?} - FIXME/TBD: do we need to shut anything down?", sig);
            }
        }
    }
    Ok(())
}
