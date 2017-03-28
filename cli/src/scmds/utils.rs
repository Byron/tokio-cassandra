use super::super::errors::*;
use std::io::{self, Write};
use tokio_cassandra::codec::header::Header;
#[cfg(feature = "colors")]
use syntect::easy::HighlightLines;
#[cfg(feature = "colors")]
use syntect::parsing::SyntaxSet;
#[cfg(feature = "colors")]
use syntect::highlighting::ThemeSet;
#[cfg(feature = "colors")]
use syntect::dumps::from_binary;

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
    pub enum OutputFormat {
        yaml,
        json
    }
}

struct Highlighter<'a, W> {
    _hl: HighlightLines<'a>,
    writer: W,
}

impl<'a, W> Write for Highlighter<'a, W>
    where W: Write
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(feature = "colors")]
pub fn output_result<W: Write>(out: &mut W, res: &Demo, fmt: OutputFormat) -> Result<()> {
    let ss: SyntaxSet = from_binary(include_bytes!("../../packs/syntax.newlines.packdump"));
    let ts: ThemeSet = from_binary(include_bytes!("../../packs/themes.themedump"));
    // TODO: allow to chose theme from a preselected list
    let theme = ts.themes
        .get("Solarized (dark)")
        .expect("theme to exist");

    match fmt {
        OutputFormat::json => {
            let mut hl = Highlighter {
                _hl: HighlightLines::new(ss.find_syntax_by_extension("yaml")
                                             .expect("yaml syntax to be compiled in"),
                                         theme),
                writer: out,
            };
            ::serde_json::ser::to_writer_pretty(&mut hl, res)?
        }
        OutputFormat::yaml => {
            let mut hl = Highlighter {
                _hl: HighlightLines::new(ss.find_syntax_by_extension("json")
                                             .expect("json syntax to be compiled in"),
                                         theme),
                writer: out,
            };
            ::serde_yaml::to_writer(&mut hl, res)?
        }
    }
    Ok(())
}

#[cfg(not(feature = "colors"))]
pub fn output_result<W: Write>(out: &mut W, res: &Demo, fmt: OutputFormat) -> Result<()> {
    match fmt {
        OutputFormat::json => ::serde_json::ser::to_writer_pretty(out, res)?,
        OutputFormat::yaml => ::serde_yaml::to_writer(out, res)?,
    }
    Ok(())
}
