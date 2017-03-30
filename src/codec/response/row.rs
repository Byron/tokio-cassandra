use codec::primitives::datatypes::{self, CqlSerializable};
use codec::primitives::decode;
use bytes::BytesMut;
use codec::response::ColumnType;

use super::*;

pub struct Row {
    raw_cols: Vec<Option<BytesMut>>,
}

pub trait ValueAt<T> {
    ///
    /// panics on out of bounds
    ///
    fn value_at(&self, i: usize) -> Result<T>;
}

impl Row {
    pub fn decode(buf: BytesMut, header: &RowsMetadata) -> Result<(BytesMut, Option<Row>)> {
        let clen = header.column_spec.len();
        let mut v = Vec::with_capacity(clen);

        let mut b = buf;
        for _ in 0..clen {
            let (buf, bytes) = decode::bytes(b)?;
            v.push(bytes.as_option());
            b = buf
        }

        Ok((b, Some(Row { raw_cols: v })))
    }

    pub fn col_iter<'a>(&'a self, meta: &'a RowsMetadata) -> RowIterator<'a> {
        RowIterator {
            meta: meta,
            row: self,
            pos: 0,
            max: self.raw_cols.len(),
        }
    }
}

impl<T: CqlSerializable> ValueAt<T> for Row {
    fn value_at(&self, i: usize) -> Result<T> {
        // TODO: no clone, maybe?
        Ok(T::deserialize(self.raw_cols[i]
                              .clone()
                              .expect("Caller expected non-optional value"))?)
    }
}

impl<U: CqlSerializable> ValueAt<Option<U>> for Row {
    fn value_at(&self, i: usize) -> Result<Option<U>> {
        Ok(match self.raw_cols[i].clone() {
               Some(b) => Some(U::deserialize(b)?),
               None => None,
           })
    }
}

pub struct RowIterator<'a> {
    meta: &'a RowsMetadata,
    row: &'a Row,
    pos: usize,
    max: usize,
}

macro_rules! row_iter {
    ($($s : pat => $t : ident ), *) => {
        impl<'a> RowIterator<'a> {
            fn as_string(&self, i: usize) -> Result<String> {
                let coltype = self.meta.column_spec[i].coltype();
                Ok(match *coltype {
                        $(
                            $s => format!("{}", ValueAt::<datatypes::$t>::value_at(self.row, i)?),
                        ) *
                                      _ => String::from("undefined"),
                })
            }
        }
    };
}

// TODO: remove undefined once every case is implemented
row_iter!(
    ColumnType::Bigint => Bigint,
    ColumnType::Blob => Blob,
    ColumnType::Boolean => Boolean,
//    ColumnType::List(_) => List,
    ColumnType::Double => Double,
    ColumnType::Float => Float,
    ColumnType::Int => Int,
    ColumnType::Varchar => Varchar,
    ColumnType::Ascii => Ascii
);

impl<'a> Iterator for RowIterator<'a> {
    type Item = Result<(&'a ColumnSpec, String)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.max {
            None
        } else {
            let i = self.pos;
            self.pos += 1;
            let s = self.as_string(i).map(|v| (&self.meta.column_spec[i], v));
            Some(s)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use codec::primitives::datatypes::*;
    use codec::primitives::{CqlFrom, CqlString};
    use bytes::BytesMut;
    use super::super::{ColumnSpec, RowsMetadata, ColumnType, TableSpec};

    fn as_bytes<T: CqlSerializable>(data: &T) -> Option<BytesMut> {
        let mut bytes = BytesMut::with_capacity(128);
        data.serialize(&mut bytes);
        Some(bytes)
    }

    #[test]
    fn row_value_at() {
        #[derive(PartialEq, Debug)]
        struct TestStruct {
            a: Int,
            b: Double,
            c: Text,
        }

        let from = TestStruct {
            a: Int::new(123),
            b: Double::new(1.2345),
            c: Text::try_from("foo").unwrap(),
        };

        let row = Row { raw_cols: vec![as_bytes(&from.a), as_bytes(&from.b), as_bytes(&from.c)] };

        let to = TestStruct {
            a: row.value_at(0).unwrap(),
            b: row.value_at(1).unwrap(),
            c: row.value_at(2).unwrap(),
        };

        assert_eq!(from, to);
    }

    #[test]
    fn row_value_at_with_option() {
        #[derive(PartialEq, Debug)]
        struct TestStruct {
            a: Option<Int>,
            b: Option<Double>,
            c: Text,
        }

        let from = TestStruct {
            a: Some(Int::new(123)),
            b: None,
            c: Text::try_from("foo").unwrap(),
        };

        let row = Row { raw_cols: vec![as_bytes(&Int::new(123)), None, as_bytes(&Text::try_from("foo").unwrap())] };

        let to = TestStruct {
            a: row.value_at(0).unwrap(),
            b: row.value_at(1).unwrap(),
            c: row.value_at(2).unwrap(),
        };

        assert_eq!(from, to);
    }

    #[test]
    fn row_iterator() {

        let row_metadata = RowsMetadata {
            global_tables_spec: None,
            paging_state: None,
            no_metadata: false,
            column_spec: vec![ColumnSpec::WithoutGlobalSpec {
                                  table_spec: TableSpec::new("ks", "testtable"),
                                  name: cql_string!("col1"),
                                  column_type: ColumnType::Int,
                              },
                              ColumnSpec::WithoutGlobalSpec {
                                  table_spec: TableSpec::new("ks", "testtable"),
                                  name: cql_string!("col2"),
                                  column_type: ColumnType::Varchar,
                              },
                              ColumnSpec::WithoutGlobalSpec {
                                  table_spec: TableSpec::new("ks", "testtable"),
                                  name: cql_string!("col3"),
                                  column_type: ColumnType::Double,
                              }],
            rows_count: 1,
        };

        let row = Row {
            raw_cols: vec![as_bytes(&Int::new(123)),
                           as_bytes(&Varchar::try_from("hello world").unwrap()),
                           as_bytes(&Double::new(1.243))],
        };

        for result in row.col_iter(&row_metadata) {
            //            let (spec, string) = result?;
        }


        //        assert_eq!(from, to);
    }

    //                TODO: Test for Errorcase
}
