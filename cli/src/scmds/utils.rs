use super::super::errors::Result;
use tokio_cassandra::codec::header::Header;
#[cfg(not(feature = "colors"))]
use std::io::Write;
use std::io;
#[cfg(not(feature = "colors"))]
use clap;
use serde::Serialize;

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
        match fmt {
            OutputFormat::json => {
                let mut hl = Highlighter {
                    hl: HighlightLines::new(
                        ss.find_syntax_by_extension("yaml").expect(
                            "yaml syntax to be compiled in",
                        ),
                        theme,
                    ),
                    writer: out,
                    cursor: Cursor::new(Vec::new()),
                };
                ::serde_json::ser::to_writer_pretty(&mut hl, res)?
            }
            OutputFormat::yaml => {
                let mut hl = Highlighter {
                    hl: HighlightLines::new(
                        ss.find_syntax_by_extension("json").expect(
                            "json syntax to be compiled in",
                        ),
                        theme,
                    ),
                    writer: out,
                    cursor: Cursor::new(Vec::new()),
                };
                ::serde_yaml::to_writer(&mut hl, res)?
            }
        }
        Ok(())
    }

}
