use super::super::errors::*;
use tokio_cassandra::codec::header::Header;
#[cfg(not(feature = "colors"))]
use std::io::Write;


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

#[cfg(not(feature = "colors"))]
pub fn output_result<W: Write>(out: &mut W, res: &Demo, fmt: OutputFormat) -> Result<()> {
    match fmt {
        OutputFormat::json => ::serde_json::ser::to_writer_pretty(out, res)?,
        OutputFormat::yaml => ::serde_yaml::to_writer(out, res)?,
    }
    Ok(())
}
#[cfg(feature = "colors")]
pub use self::highlighting::output_result;

#[cfg(feature = "colors")]
mod highlighting {
    use std::io::{self, Write, Cursor, BufRead};
    use syntect::easy::HighlightLines;
    use syntect::util::as_24_bit_terminal_escaped;
    use syntect::highlighting::ThemeSet;
    use syntect::parsing::SyntaxSet;
    use syntect::dumps::from_binary;
    use super::{Demo, OutputFormat, Result};

    struct Highlighter<'a, W> {
        hl: HighlightLines<'a>,
        cursor: Cursor<Vec<u8>>,
        writer: W,
    }

    impl<'a, W> Write for Highlighter<'a, W>
        where W: Write
    {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.cursor.write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            //        self.cursor.seek(SeekFrom::Start(0)).map(|e| ())?;
            let mut line = String::new();
            while let Ok(_) = self.cursor.read_line(&mut line) {
                let escaped = {
                    let ranges = self.hl.highlight(&line);
                    as_24_bit_terminal_escaped(&ranges[..], true)
                };
                self.writer.write(escaped.as_bytes())?;
                line.clear();
            }
            Ok(())
        }
    }

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
                    hl: HighlightLines::new(ss.find_syntax_by_extension("yaml")
                                                .expect("yaml syntax to be compiled in"),
                                            theme),
                    writer: out,
                    cursor: Cursor::new(Vec::new()),
                };
                ::serde_json::ser::to_writer_pretty(&mut hl, res)?
            }
            OutputFormat::yaml => {
                let mut hl = Highlighter {
                    hl: HighlightLines::new(ss.find_syntax_by_extension("json")
                                                .expect("json syntax to be compiled in"),
                                            theme),
                    writer: out,
                    cursor: Cursor::new(Vec::new()),
                };
                ::serde_yaml::to_writer(&mut hl, res)?
            }
        }
        Ok(())
    }

}
