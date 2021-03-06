use super::super::errors::{ErrorKind, Result};
use tokio_cassandra::codec::header::Header;
#[cfg(not(feature = "colors"))]
use std::io::Write;
use std::io;
use clap;
use serde::Serialize;
use tokio_cassandra::codec::primitives::{CqlFrom, CqlLongString, CqlConsistency};
use tokio_cassandra::codec::request::{QueryMessage, Message};
use tokio_cassandra::tokio::easy;
use tokio_cassandra::codec::response::ErrorMessage;

pub const THEME_NAMES: [&'static str; 3] = ["base16-ocean.dark", "Solarized (dark)", "Solarized (light)"];

#[derive(Deserialize, Serialize)]
pub struct Demo {
    pub result_example: Header,
    pub description: String,
}

impl Default for Demo {
    fn default() -> Self {
        Demo {
            result_example: Header::try_from(b"\x03\x02\x00\x00\x05\x00\x00\x00\x00").unwrap(),
            description: "I believe we need to implement the serde-traits manually on our response types to \
                              implement it in a controlled fashion without extra copies."
                .into(),
        }
    }
}

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug)]
    pub enum ColorMode {
        auto,
        always,
        off
    }
}

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug)]
    pub enum OutputFormat {
        yaml,
        json
    }
}

fn output_result_to_stdout_without_color<S: Serialize>(res: &S, fmt: OutputFormat) -> Result<()> {
    let s = io::stdout();
    let mut out = s.lock();
    match fmt {
        OutputFormat::json => ::serde_json::ser::to_writer_pretty(&mut out, res)?,
        OutputFormat::yaml => ::serde_yaml::to_writer(&mut out, res)?,
    }
    Ok(())
}

pub fn handle_call_result(res: easy::Message, args: &clap::ArgMatches) -> Result<()> {
    match res {
        easy::Message::Error(ErrorMessage { text, code }) => Err(ErrorKind::CqlError(code, text).into()),
        easy::Message::Result(res) => {
            let res = output_result(
                &res,
                args.value_of("output-format")
                    .expect("clap to work")
                    .parse()
                    .expect("clap to work"),
                args,
            );
            println!();
            res
        }
        _ => Err(ErrorKind::Unimplemented(format!("{:?}", res)).into()),
    }
}

pub fn request_from_query(query: &str) -> Result<Message> {
    Ok(Message::Query(QueryMessage {
        // FIXME: provide a consuming version that consumes a string directly into the vec
        // and thus prevents an entirely unnecessary copy
        query: CqlLongString::try_from(query)?,
        values: None,
        consistency: CqlConsistency::All,
        skip_metadata: false,
        page_size: None,
        paging_state: None,
        serial_consistency: Some(CqlConsistency::All),
        timestamp: None,
    }))
}

#[cfg(not(feature = "colors"))]
pub fn output_result<S: Serialize>(res: &S, fmt: OutputFormat, _args: &clap::ArgMatches) -> Result<()> {
    output_result_to_stdout_without_color(res, fmt)
}

#[cfg(feature = "colors")]
pub use self::highlighting::output_result;

#[cfg(feature = "colors")]
mod highlighting {
    use std::io::{self, Write, Cursor, BufRead, SeekFrom, Seek};
    use syntect::easy::HighlightLines;
    use syntect::util::as_24_bit_terminal_escaped;
    use syntect::highlighting::ThemeSet;
    use syntect::parsing::SyntaxSet;
    use syntect::dumps::from_binary;
    use serde::Serialize;
    use super::{OutputFormat, Result, ColorMode, output_result_to_stdout_without_color};
    use isatty;

    use clap;

    struct Highlighter<'a, W>
    where
        W: Write,
    {
        hl: HighlightLines<'a>,
        cursor: Cursor<Vec<u8>>,
        writer: W,
    }

    impl<'a, W> Drop for Highlighter<'a, W>
    where
        W: Write,
    {
        fn drop(&mut self) {
            self.flush().ok();
        }
    }

    impl<'a, W> Write for Highlighter<'a, W>
    where
        W: Write,
    {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.cursor.write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            let mut line = String::new();
            self.cursor.seek(SeekFrom::Start(0))?;
            while let Ok(nbr) = self.cursor.read_line(&mut line) {
                if nbr == 0 {
                    break;
                }
                let escaped = {
                    let regions = self.hl.highlight(&line);
                    as_24_bit_terminal_escaped(&regions[..], true)
                };
                self.writer.write(escaped.as_bytes())?;
                line.clear();
            }
            Ok(())
        }
    }

    pub fn output_result<S: Serialize>(res: &S, fmt: OutputFormat, args: &clap::ArgMatches) -> Result<()> {
        let use_color = {
            let color_mode = args.value_of("color")
                .expect("clap to work")
                .parse()
                .expect("clap to work");
            match color_mode {
                ColorMode::always => true,
                ColorMode::off => false,
                ColorMode::auto => isatty::stdout_isatty(),
            }
        };

        if !use_color {
            return output_result_to_stdout_without_color(res, fmt);
        }

        let ss = {
            let mut ss: SyntaxSet = from_binary(include_bytes!("../../packs/syntax.newlines.packdump"));
            ss.link_syntaxes();
            ss
        };
        let ts: ThemeSet = from_binary(include_bytes!("../../packs/themes.themedump"));
        let theme = ts.themes
            .get(args.value_of("theme").expect("clap to work"))
            .expect("theme to exist");

        let s = io::stdout();
        let out = s.lock();

        let mut hl = Highlighter {
            hl: HighlightLines::new(
                ss.find_syntax_by_extension(match fmt {
                    OutputFormat::json => "json",
                    OutputFormat::yaml => "yaml",
                }).expect("yaml syntax to be compiled in"),
                theme,
            ),
            writer: out,
            cursor: Cursor::new(Vec::new()),
        };

        match fmt {
            OutputFormat::json => ::serde_json::ser::to_writer_pretty(&mut hl, res)?,
            OutputFormat::yaml => ::serde_yaml::to_writer(&mut hl, res)?,
        };

        Ok(())
    }

}
