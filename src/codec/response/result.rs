use codec::primitives::{CqlFrom, CqlString, CqlBytes};
use codec::header::ProtocolVersion;
use codec::primitives::decode;
use bytes::BytesMut;

use super::*;

#[derive(Debug, PartialEq, Eq)]
pub enum ResultHeader {
    Void,
    SetKeyspace(CqlString),
    SchemaChange(SchemaChangePayload),
    Rows(RowsMetadata),
}

#[derive(Debug, PartialEq, Eq)]
pub struct SchemaChangePayload {
    change_type: CqlString,
    target: CqlString,
    options: CqlString,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RowsMetadata {
    pub global_tables_spec: Option<TableSpec>,
    pub paging_state: Option<CqlBytes>,
    pub no_metadata: bool,
    pub column_spec: Vec<ColumnSpec>,
    pub rows_count: i32,
}

impl Default for RowsMetadata {
    fn default() -> RowsMetadata {
        RowsMetadata {
            global_tables_spec: None,
            paging_state: None,
            no_metadata: false,
            column_spec: Vec::new(),
            rows_count: 0,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct TableSpec {
    keyspace: CqlString,
    table: CqlString,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ColumnSpec {
    WithoutGlobalSpec {
        table_spec: TableSpec,
        name: CqlString,
        column_type: ColumnType,
    },
    WithGlobalSpec {
        name: CqlString,
        column_type: ColumnType,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum ColumnType {
    Custom(CqlString),
    Ascii,
    Bigint,
    Blob,
    Boolean,
    Counter,
    Decimal,
    Double,
    Float,
    Int,
    Timestamp,
    Uuid,
    Varchar,
    Varint,
    Timeuuid,
    Inet,
    List(Box<ColumnType>),
    Map(Box<ColumnType>, Box<ColumnType>),
    Set(Box<ColumnType>),
    Udt(UdtDefinition),
    Tuple(TupleDefinition),
}


#[derive(Debug, PartialEq, Eq)]
pub struct TupleDefinition(Vec<ColumnType>);

#[derive(Debug, PartialEq, Eq)]
pub struct UdtDefinition {
    keyspace: CqlString,
    name: CqlString,
    fields: Vec<UdtField>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct UdtField(CqlString, ColumnType);

impl ColumnType {
    pub fn decode(buf: BytesMut) -> decode::ParseResult<Option<ColumnType>> {
        Ok(if buf.len() < 2 {
               (buf, None)
           } else {
               let (buf, id) = decode::short(buf)?;
               match id {
                   0x0000 => {
                let (buf, s) = decode::string(buf)?;
                (buf, Some(ColumnType::Custom(s)))
            }
                   0x0001 => (buf, Some(ColumnType::Ascii)),
                   0x0002 => (buf, Some(ColumnType::Bigint)),
                   0x0003 => (buf, Some(ColumnType::Blob)),
                   0x0004 => (buf, Some(ColumnType::Boolean)),
                   0x0005 => (buf, Some(ColumnType::Counter)),
                   0x0006 => (buf, Some(ColumnType::Decimal)),
                   0x0007 => (buf, Some(ColumnType::Double)),
                   0x0008 => (buf, Some(ColumnType::Float)),
                   0x0009 => (buf, Some(ColumnType::Int)),
                   0x000B => (buf, Some(ColumnType::Timestamp)),
                   0x000C => (buf, Some(ColumnType::Uuid)),
                   0x000D => (buf, Some(ColumnType::Varchar)),
                   0x000E => (buf, Some(ColumnType::Varint)),
                   0x000F => (buf, Some(ColumnType::Timeuuid)),
                   0x0010 => (buf, Some(ColumnType::Inet)),
                   0x0020 => {
                let (buf, inner) = Self::decode(buf)?;
                (buf, inner.map(|v| ColumnType::List(Box::new(v))))
            }
                   0x0021 => {
                let (buf, inner_key) = Self::decode(buf)?;
                let (buf, inner_value) = Self::decode(buf)?;
                let map = inner_key.and_then(|k| inner_value.map(|v| (ColumnType::Map(Box::new(k), Box::new(v)))));
                (buf, map)
            }
                   0x0022 => {
                let (buf, inner) = Self::decode(buf)?;
                (buf, inner.map(|v| ColumnType::Set(Box::new(v))))
            }
                   0x0030 => {
                //                       TODO: looks a bit complicated, see if there is potential for optimization
                let (buf, ks) = decode::string(buf)?;
                let (buf, name) = decode::string(buf)?;
                let (buf, n) = decode::short(buf)?;

                let mut fields = Vec::new();
                let mut b = buf;
                for _ in 0..n {
                    let (buf, fname) = decode::string(b)?;
                    let (buf, ctype) = Self::decode(buf)?;
                    if let Some(ctype) = ctype {
                        fields.push(UdtField(fname, ctype));
                    } else {
                        return Ok((buf, None));
                    }
                    b = buf
                }
                (b,
                 Some(ColumnType::Udt(UdtDefinition {
                                          keyspace: ks,
                                          name: name,
                                          fields: fields,
                                      })))
            }

                   0x0031 => {
                let (buf, n) = decode::short(buf)?;

                let mut fields = Vec::new();
                let mut b = buf;
                for _ in 0..n {
                    let (buf, ctype) = Self::decode(b)?;
                    if let Some(ctype) = ctype {
                        fields.push(ctype);
                    } else {
                        return Ok((buf, None));
                    }
                    b = buf
                }
                (b, Some(ColumnType::Tuple(TupleDefinition(fields))))
            }

                   _ => unimplemented!(),
               }
           })
    }
}

impl ResultHeader {
    pub fn decode(_v: ProtocolVersion, buf: BytesMut) -> Result<(BytesMut, Option<ResultHeader>)> {
        if buf.len() < 4 {
            Err(ErrorKind::Incomplete(format!("Need 4 bytes for length")).into())
        } else {
            let (buf, t) = decode::int(buf)?;
            match t {
                0x0001 => Ok((buf, Some(ResultHeader::Void))),
                0x0002 => Self::match_decode(Self::decode_rows_metadata(buf), |d| ResultHeader::Rows(d)),
                0x0003 => Self::match_decode(decode::string(buf), |s| ResultHeader::SetKeyspace(s)),
                0x0005 => {
                    Self::match_decode(Self::decode_schema_change(buf),
                                       |c| ResultHeader::SchemaChange(c))
                }
                // TODO:
                // 0x0004    Prepared: result to a PREPARE message.
                _ => Ok((buf, None)),
            }
        }
    }

    fn match_decode<T, F>(decoded: decode::ParseResult<T>, f: F) -> Result<(BytesMut, Option<ResultHeader>)>
        where F: Fn(T) -> ResultHeader
    {
        match decoded {
            Ok((buf, s)) => Ok((buf, Some(f(s)))),
            Err(a) => Err(a.into()),
        }
    }

    fn decode_schema_change(buf: BytesMut) -> decode::ParseResult<SchemaChangePayload> {
        let (buf, change_type) = decode::string(buf)?;
        let (buf, target) = decode::string(buf)?;
        let (buf, options) = decode::string(buf)?;

        Ok((buf,
            SchemaChangePayload {
                change_type: change_type,
                target: target,
                options: options,
            }))
    }

    fn decode_rows_metadata(buf: BytesMut) -> decode::ParseResult<RowsMetadata> {
        let (buf, flags) = decode::int(buf)?;
        let (buf, col_count) = decode::int(buf)?;

        let mut rows_metadata = RowsMetadata::default();

        if (flags & 0x0002) == 0x0002 {
            rows_metadata.paging_state = Some(cql_bytes!(1, 2, 3));
        }

        let buf = if (flags & 0x0001) == 0x0001 {
            let (buf, keyspace) = decode::string(buf)?;
            let (buf, table) = decode::string(buf)?;
            rows_metadata.global_tables_spec = Some(TableSpec {
                                                        keyspace: keyspace,
                                                        table: table,
                                                    });
            buf
        } else {
            buf
        };

        rows_metadata.no_metadata = (flags & 0x0004) == 0x0004;

        let mut columns = Vec::new();
        let mut b = buf;
        for _ in 0..col_count {
            let (buf, table_spec) = {
                if rows_metadata.global_tables_spec.is_none() {
                    let (buf, keyspace) = decode::string(b)?;
                    let (buf, table) = decode::string(buf)?;

                    (buf,
                     Some(TableSpec {
                              keyspace: keyspace,
                              table: table,
                          }))
                } else {
                    (b, None)
                }
            };
            let (buf, name) = decode::string(buf)?;
            let (buf, ctype) = ColumnType::decode(buf)?;

            if let Some(ctype) = ctype {
                columns.push(if let Some(tspec) = table_spec {
                                 ColumnSpec::WithoutGlobalSpec {
                                     table_spec: tspec,
                                     name: name,
                                     column_type: ctype,
                                 }
                             } else {
                                 ColumnSpec::WithGlobalSpec {
                                     name: name,
                                     column_type: ctype,
                                 }
                             });
            } else {
                return Err(decode::Error::Incomplete(decode::Needed::Unknown));
            }
            b = buf;
        }

        rows_metadata.column_spec = columns;

        let (b, rows_count) = decode::int(b)?;
        rows_metadata.rows_count = rows_count;

        Ok((b, rows_metadata))
    }
}

#[cfg(test)]
mod test {
    use codec::header::Header;
    use codec::header::ProtocolVersion::*;
    use codec::primitives::CqlString;
    use super::*;

    fn skip_header(b: &[u8]) -> &[u8] {
        &b[Header::encoded_len()..]
    }

    #[test]
    fn decode_result_header_rows() {
        let msg = include_bytes!("../../../tests/fixtures/v3/responses/result_rows.msg");
        let buf = Vec::from(skip_header(&msg[..]));

        let res = ResultHeader::decode(Version3, Vec::from(&buf[0..5]).into());
        assert!(res.is_err());

        let rexpected = RowsMetadata {
            global_tables_spec: Some(TableSpec {
                                         keyspace: cql_string!("system"),
                                         table: cql_string!("local"),
                                     }),
            paging_state: None,
            no_metadata: false,
            column_spec: vec![ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("key"),
                                  column_type: ColumnType::Varchar,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("bootstrapped"),
                                  column_type: ColumnType::Varchar,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("broadcast_address"),
                                  column_type: ColumnType::Inet,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("cluster_name"),
                                  column_type: ColumnType::Varchar,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("cql_version"),
                                  column_type: ColumnType::Varchar,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("data_center"),
                                  column_type: ColumnType::Varchar,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("gossip_generation"),
                                  column_type: ColumnType::Int,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("host_id"),
                                  column_type: ColumnType::Uuid,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("listen_address"),
                                  column_type: ColumnType::Inet,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("native_protocol_version"),
                                  column_type: ColumnType::Varchar,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("partitioner"),
                                  column_type: ColumnType::Varchar,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("rack"),
                                  column_type: ColumnType::Varchar,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("release_version"),
                                  column_type: ColumnType::Varchar,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("rpc_address"),
                                  column_type: ColumnType::Inet,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("schema_version"),
                                  column_type: ColumnType::Uuid,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("thrift_version"),
                                  column_type: ColumnType::Varchar,
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("tokens"),
                                  column_type: ColumnType::Set(Box::new(ColumnType::Varchar)),
                              },
                              ColumnSpec::WithGlobalSpec {
                                  name: cql_string!("truncated_at"),
                                  column_type: ColumnType::Map(Box::new(ColumnType::Uuid), Box::new(ColumnType::Blob)),
                              }],
            rows_count: 1,
        };

        let res = ResultHeader::decode(Version3, buf.into()).unwrap();
        assert_eq!(res.1, Some(ResultHeader::Rows(rexpected)));
    }

    #[test]
    fn decode_result_body() {
        let msg = include_bytes!("../../../tests/fixtures/v3/responses/result_rows.msg");
        let buf = Vec::from(skip_header(&msg[..])).into();

        let (buf, result_header) = ResultHeader::decode(Version3, buf).unwrap();

        if let ResultHeader::Rows(rows_metadata) = result_header.unwrap() {
            let (buf, row) = Row::decode(buf, &rows_metadata).unwrap();
            //            let t = row.get_type(0, rows_metadata);
            //            let t = row.get_name(0, rows_metadata);
            //            let t = row.get_as_string(0, rows_metadata);
        } else {
            panic!("Expected to have rows metadata");
        }
    }

    // TODO: write test with chunking of result!!! random chunking?

    #[test]
    fn decode_result_header_rows_non_global_spec() {
        let msg = include_bytes!("../../../tests/fixtures/v3/responses/result_rows_non_global_spec.msg");
        let buf = Vec::from(skip_header(&msg[..]));

        let res = ResultHeader::decode(Version3, Vec::from(&buf[0..5]).into());
        assert!(res.is_err());

        let rexpected = RowsMetadata {
            global_tables_spec: None,
            paging_state: None,
            no_metadata: false,
            column_spec: vec![ColumnSpec::WithoutGlobalSpec {
                                  table_spec: TableSpec {
                                      keyspace: cql_string!("system"),
                                      table: cql_string!("local"),
                                  },
                                  name: cql_string!("key"),
                                  column_type: ColumnType::Varchar,
                              },
                              ColumnSpec::WithoutGlobalSpec {
                                  table_spec: TableSpec {
                                      keyspace: cql_string!("system"),
                                      table: cql_string!("l0cal"),
                                  },
                                  name: cql_string!("bootstrapped"),
                                  column_type: ColumnType::Varchar,
                              }],
            rows_count: 1,
        };

        let buf = buf.into();
        let res = ResultHeader::decode(Version3, buf).unwrap();

        assert_eq!(res.1, Some(ResultHeader::Rows(rexpected)));
        // TODO: rest of drained buf should be used for streaming results after that
    }

    #[test]
    fn decode_result_header_void() {
        let msg = include_bytes!("../../../tests/fixtures/v3/responses/result_void.msg");
        let buf = Vec::from(skip_header(&msg[..]));

        let res = ResultHeader::decode(Version3, Vec::from(&buf[0..1]).into());
        assert!(res.is_err());

        let res = ResultHeader::decode(Version3, buf.into()).unwrap();
        assert_eq!(res.1, Some(ResultHeader::Void));
    }

    #[test]
    fn decode_result_header_set_keyspace() {
        let msg = include_bytes!("../../../tests/fixtures/v3/responses/result_set_keyspace.msg");
        let buf = Vec::from(skip_header(&msg[..]));

        // Ok(None) Ok(Some()), Err()
        let res = ResultHeader::decode(Version3, Vec::from(&buf[0..6]).into());
        assert!(res.is_err());

        let res = ResultHeader::decode(Version3, Vec::from(&buf[0..9]).into());
        assert!(res.is_err());

        let res = ResultHeader::decode(Version3, buf.into()).unwrap();
        assert_eq!(res.1, Some(ResultHeader::SetKeyspace(cql_string!("abcd"))));
    }

    #[test]
    fn decode_result_header_schema_change() {
        let msg = include_bytes!("../../../tests/fixtures/v3/responses/result_schema_change.msg");
        let buf = Vec::from(skip_header(&msg[..]));

        // Ok(None) Ok(Some()), Err()
        let res = ResultHeader::decode(Version3, Vec::from(&buf[0..6]).into());
        assert!(res.is_err());

        let res = ResultHeader::decode(Version3, buf.into()).unwrap();
        assert_eq!(res.1,
                   Some(ResultHeader::SchemaChange(SchemaChangePayload {
                                                       change_type: cql_string!("change_type"),
                                                       target: cql_string!("target"),
                                                       options: cql_string!("options"),
                                                   })));
    }

    #[test]
    fn decode_column_type_custom() {
        let buf = vec![0x00, 0x00, 0x00, 0x02, 0x61, 0x62];
        let res = ColumnType::decode(buf.into()).unwrap();

        let expected = ColumnType::Custom(cql_string!("ab"));

        assert_eq!(res.1, Some(expected));
    }

    #[test]
    fn decode_column_type_ascii() {
        let res = ColumnType::decode((vec![0x00, 0x01]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Ascii));
    }

    #[test]
    fn decode_column_type_bigint() {
        let res = ColumnType::decode((vec![0x00, 0x02]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Bigint));
    }

    #[test]
    fn decode_column_type_blob() {
        let res = ColumnType::decode((vec![0x00, 0x03]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Blob));
    }

    #[test]
    fn decode_column_type_boolean() {
        let res = ColumnType::decode((vec![0x00, 0x04]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Boolean));
    }

    #[test]
    fn decode_column_type_counter() {
        let res = ColumnType::decode((vec![0x00, 0x05]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Counter));
    }

    #[test]
    fn decode_column_type_decimal() {
        let res = ColumnType::decode((vec![0x00, 0x06]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Decimal));
    }

    #[test]
    fn decode_column_type_double() {
        let res = ColumnType::decode((vec![0x00, 0x07]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Double));
    }

    #[test]
    fn decode_column_type_float() {
        let res = ColumnType::decode((vec![0x00, 0x08]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Float));
    }

    #[test]
    fn decode_column_type_int() {
        let res = ColumnType::decode((vec![0x00, 0x09]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Int));
    }

    #[test]
    fn decode_column_type_timestamp() {
        let res = ColumnType::decode((vec![0x00, 0x0b]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Timestamp));
    }

    #[test]
    fn decode_column_type_uuid() {
        let res = ColumnType::decode((vec![0x00, 0x0c]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Uuid));
    }

    #[test]
    fn decode_column_type_varchar() {
        let res = ColumnType::decode((vec![0x00, 0x0d]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Varchar));
    }

    #[test]
    fn decode_column_type_varint() {
        let res = ColumnType::decode((vec![0x00, 0x0e]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Varint));
    }

    #[test]
    fn decode_column_type_timeuuid() {
        let res = ColumnType::decode((vec![0x00, 0x0f]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Timeuuid));
    }

    #[test]
    fn decode_column_type_inet() {
        let res = ColumnType::decode((vec![0x00, 0x10]).into()).unwrap();
        assert_eq!(res.1, Some(ColumnType::Inet));
    }

    #[test]
    fn decode_column_type_list() {
        let buf = vec![0x00, 0x20, 0x00, 0x10];
        let res = ColumnType::decode(buf.into()).unwrap();
        let exp = ColumnType::List(Box::new(ColumnType::Inet));
        assert_eq!(res.1, Some(exp));
    }

    #[test]
    fn decode_column_type_list_nested() {
        let buf = vec![0x00, 0x20, 0x00, 0x20, 0x00, 0x06];
        let res = ColumnType::decode(buf.into()).unwrap();
        let exp = ColumnType::List(Box::new(ColumnType::List(Box::new(ColumnType::Decimal))));
        assert_eq!(res.1, Some(exp));
    }

    #[test]
    fn decode_column_type_map() {
        let buf = vec![0x00, 0x21, 0x00, 0x0D, 0x00, 0x06];
        let res = ColumnType::decode(buf.into()).unwrap();
        let exp = ColumnType::Map(Box::new(ColumnType::Varchar), Box::new(ColumnType::Decimal));
        assert_eq!(res.1, Some(exp));
    }

    #[test]
    fn decode_column_type_map_nested() {
        let buf = vec![0x00, 0x21, 0x00, 0x20, 0x00, 0x0D, 0x00, 0x06];
        let res = ColumnType::decode(buf.into()).unwrap();
        let exp = ColumnType::Map(Box::new(ColumnType::List(Box::new(ColumnType::Varchar))),
                                  Box::new(ColumnType::Decimal));
        assert_eq!(res.1, Some(exp));
    }

    #[test]
    fn decode_column_type_set() {
        let buf = vec![0x00, 0x22, 0x00, 0x0D];
        let res = ColumnType::decode(buf.into()).unwrap();
        let exp = ColumnType::Set(Box::new(ColumnType::Varchar));
        assert_eq!(res.1, Some(exp));
    }

    #[test]
    fn decode_column_type_udt() {
        let buf = vec![0x00, 0x30, 0x00, 0x02, 0x6B, 0x73, 0x00, 0x03, 0x75, 0x64, 0x74, 0x00, 0x02, 0x00, 0x02, 0x66,
                       0x31, 0x00, 0x06, 0x00, 0x02, 0x66, 0x32, 0x00, 0x0D];
        let res = ColumnType::decode(buf.into()).unwrap();

        let fields = vec![UdtField(cql_string!("f1"), ColumnType::Decimal),
                          UdtField(cql_string!("f2"), ColumnType::Varchar)];

        let exp = ColumnType::Udt(UdtDefinition {
                                      keyspace: cql_string!("ks"),
                                      name: cql_string!("udt"),
                                      fields: fields,
                                  });
        assert_eq!(res.1, Some(exp));
    }

    #[test]
    fn decode_column_type_tuple() {
        let buf = vec![0x00, 0x31, 0x00, 0x02, 0x00, 0x0D, 0x00, 0x06];
        let res = ColumnType::decode(buf.into()).unwrap();
        let exp = ColumnType::Tuple(TupleDefinition(vec![ColumnType::Varchar, ColumnType::Decimal]));
        assert_eq!(res.1, Some(exp));
    }
}
