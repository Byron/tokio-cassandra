use super::super::errors::*;
use std::io::Write;
use tokio_cassandra::codec::header::Header;

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

pub fn output_result<W: Write>(out: &mut W, res: &Demo, fmt: OutputFormat) -> Result<()> {
    match fmt {
        OutputFormat::json => ::serde_json::ser::to_writer_pretty(out, res)?,
        OutputFormat::yaml => ::serde_yaml::to_writer(out, res)?,
    }
    Ok(())
}
