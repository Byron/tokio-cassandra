use super::super::errors::*;
use super::super::args::ConnectionOptions;

use linefeed::Reader;
use linefeed::Terminal;

pub fn interactive<T: Terminal>(rd: Reader<T>, opts: ConnectionOptions, initial_query: Option<String>) -> Result<()> {
    Ok(())
}
