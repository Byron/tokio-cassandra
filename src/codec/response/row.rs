use codec::primitives::{CqlFrom, CqlString, CqlBytes};
use codec::primitives::datatypes::CqlSerializable;
use codec::header::ProtocolVersion;
use codec::primitives::decode;
use bytes::BytesMut;

use super::*;

pub struct Row {
    raw_cols: Vec<CqlBytes>,
}

impl Row {
    pub fn decode(buf: BytesMut, header: &RowsMetadata) -> Result<(BytesMut, Option<Row>)> {
        let mut v = Vec::new();
        let clen = header.column_spec.len();

        let mut b = buf;
        for _ in 0..clen {
            let (buf, bytes) = decode::bytes(b)?;
            v.push(bytes);
            b = buf
        }

        Ok((b, Some(Row { raw_cols: v })))
    }
}

pub struct Row2<H, T>
    where H: CqlSerializable,
          T: ColumnTraverse
{
    head: Cell<H>,
    tail: T,
}

pub struct Cell<T>
    where T: CqlSerializable
{
    column_spec: ColumnSpec,
    value: T,
}

struct Empty;

pub trait ColumnTraverse {
    fn traverse(self);
}

impl ColumnTraverse for Empty {
    fn traverse(self) {}
}

#[cfg(test)]
mod test {
    use super::*;
    use super::super::*;
    use codec::primitives::datatypes::*;

    #[test]
    fn row_traverse() {
        let row = Row2 {
            head: Cell {
                column_spec: ColumnSpec::WithGlobalSpec {
                    name: cql_string!("number"),
                    column_type: ColumnType::Int,
                },
                value: Int::new(1),
            },
            tail: Empty,
        };
    }
}
