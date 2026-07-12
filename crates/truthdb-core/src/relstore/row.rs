//! Row codec: `null bitmap | fixed section | var end-offsets | var data`.
//!
//! Fixed-width columns occupy their slot in schema order whether or not they
//! are NULL (zeroed when NULL); variable-width columns store u16 end offsets
//! relative to the start of the var-data section, so value `i` spans
//! `[end[i-1], end[i])`. Rows are capped at [`MAX_ROW_BYTES`] until overflow
//! pages arrive (Stage 14).

use crate::relstore::types::{ColumnType, Datum, TypeError};

/// In-row size cap (bytes), leaving page-header/slot overhead inside a 4 KiB
/// page.
pub const MAX_ROW_BYTES: usize = 3900;

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
    /// Collation name for character columns (`None` = database default). Used
    /// for comparison/sort/key-encoding; irrelevant to the row byte codec.
    pub collation: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    fn null_bitmap_len(&self) -> usize {
        self.columns.len().div_ceil(8)
    }

    fn fixed_section_len(&self) -> usize {
        self.columns
            .iter()
            .filter_map(|c| c.column_type.fixed_size())
            .sum()
    }

    fn var_column_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| c.column_type.fixed_size().is_none())
            .count()
    }
}

/// Checks that a datum's variant matches the column type (or is NULL).
fn datum_matches(column_type: &ColumnType, datum: &Datum) -> bool {
    matches!(
        (column_type, datum),
        (_, Datum::Null)
            | (ColumnType::TinyInt, Datum::TinyInt(_))
            | (ColumnType::SmallInt, Datum::SmallInt(_))
            | (ColumnType::Int, Datum::Int(_))
            | (ColumnType::BigInt, Datum::BigInt(_))
            | (ColumnType::Bit, Datum::Bit(_))
            | (ColumnType::Real, Datum::Real(_))
            | (ColumnType::Float, Datum::Float(_))
            | (ColumnType::Decimal { .. }, Datum::Decimal(_))
            | (ColumnType::Date, Datum::Date(_))
            | (ColumnType::Time, Datum::Time(_))
            | (ColumnType::DateTime2, Datum::DateTime2(_, _))
            | (ColumnType::UniqueIdentifier, Datum::UniqueIdentifier(_))
            | (ColumnType::VarChar { .. }, Datum::VarChar(_))
            | (ColumnType::NVarChar { .. }, Datum::NVarChar(_))
            | (ColumnType::VarBinary { .. }, Datum::VarBinary(_))
    )
}

pub fn encode_row(schema: &Schema, values: &[Datum]) -> Result<Vec<u8>, TypeError> {
    if values.len() != schema.columns.len() {
        return Err(TypeError(format!(
            "row arity mismatch: {} values for {} columns",
            values.len(),
            schema.columns.len()
        )));
    }
    for (column, value) in schema.columns.iter().zip(values) {
        if !datum_matches(&column.column_type, value) {
            return Err(TypeError(format!(
                "type mismatch for column '{}' ({})",
                column.name,
                column.column_type.name()
            )));
        }
    }

    let bitmap_len = schema.null_bitmap_len();
    let fixed_len = schema.fixed_section_len();
    let var_count = schema.var_column_count();
    let mut out = vec![0u8; bitmap_len];
    out.reserve(fixed_len + var_count * 2);

    // Null bitmap: bit set = NULL.
    for (index, value) in values.iter().enumerate() {
        if value.is_null() {
            out[index / 8] |= 1 << (index % 8);
        }
    }

    // Fixed section (zeroes for NULL fixed columns).
    for (column, value) in schema.columns.iter().zip(values) {
        if let Some(size) = column.column_type.fixed_size() {
            if value.is_null() {
                out.extend(std::iter::repeat_n(0u8, size));
            } else {
                value.encode_fixed(&mut out);
            }
        }
    }

    // Var end-offsets then var data. NULL var values are zero-length (the
    // bitmap distinguishes NULL from empty).
    let mut var_payloads: Vec<Vec<u8>> = Vec::with_capacity(var_count);
    for (column, value) in schema.columns.iter().zip(values) {
        if column.column_type.fixed_size().is_none() {
            var_payloads.push(if value.is_null() {
                Vec::new()
            } else {
                value.encode_var()
            });
        }
    }
    let mut end = 0usize;
    for payload in &var_payloads {
        end += payload.len();
        if end > u16::MAX as usize {
            return Err(TypeError("var section exceeds 64 KiB".to_string()));
        }
        out.extend_from_slice(&(end as u16).to_le_bytes());
    }
    for payload in &var_payloads {
        out.extend_from_slice(payload);
    }

    if out.len() > MAX_ROW_BYTES {
        return Err(TypeError(format!(
            "row of {} bytes exceeds the in-row cap of {MAX_ROW_BYTES}",
            out.len()
        )));
    }
    Ok(out)
}

pub fn decode_row(schema: &Schema, bytes: &[u8]) -> Result<Vec<Datum>, TypeError> {
    let bitmap_len = schema.null_bitmap_len();
    let fixed_len = schema.fixed_section_len();
    let var_count = schema.var_column_count();
    let header_len = bitmap_len + fixed_len + var_count * 2;
    if bytes.len() < header_len {
        return Err(TypeError(format!(
            "row too short: {} bytes, header needs {header_len}",
            bytes.len()
        )));
    }
    let bitmap = &bytes[..bitmap_len];
    let is_null = |index: usize| bitmap[index / 8] & (1 << (index % 8)) != 0;

    let var_offsets_start = bitmap_len + fixed_len;
    let var_data_start = var_offsets_start + var_count * 2;
    let var_data = &bytes[var_data_start..];
    let mut var_ends = Vec::with_capacity(var_count);
    for i in 0..var_count {
        let at = var_offsets_start + i * 2;
        var_ends.push(u16::from_le_bytes([bytes[at], bytes[at + 1]]) as usize);
    }
    if let Some(&last) = var_ends.last()
        && last != var_data.len()
    {
        return Err(TypeError("var section length mismatch".to_string()));
    }

    let mut values = Vec::with_capacity(schema.columns.len());
    let mut fixed_cursor = bitmap_len;
    let mut var_index = 0usize;
    for (index, column) in schema.columns.iter().enumerate() {
        match column.column_type.fixed_size() {
            Some(size) => {
                let raw = &bytes[fixed_cursor..fixed_cursor + size];
                fixed_cursor += size;
                values.push(if is_null(index) {
                    Datum::Null
                } else {
                    Datum::decode_fixed(&column.column_type, raw)?
                });
            }
            None => {
                let start = if var_index == 0 {
                    0
                } else {
                    var_ends[var_index - 1]
                };
                let end = var_ends[var_index];
                var_index += 1;
                if start > end || end > var_data.len() {
                    return Err(TypeError("corrupt var offsets".to_string()));
                }
                values.push(if is_null(index) {
                    Datum::Null
                } else {
                    Datum::decode_var(&column.column_type, &var_data[start..end])?
                });
            }
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Schema {
        Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    column_type: ColumnType::Int,
                    nullable: false,
                    collation: None,
                },
                Column {
                    name: "name".to_string(),
                    column_type: ColumnType::NVarChar { max_len: 50 },
                    nullable: true,
                    collation: None,
                },
                Column {
                    name: "price".to_string(),
                    column_type: ColumnType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                    nullable: true,
                    collation: None,
                },
                Column {
                    name: "blob".to_string(),
                    column_type: ColumnType::VarBinary { max_len: 100 },
                    nullable: true,
                    collation: None,
                },
                Column {
                    name: "flag".to_string(),
                    column_type: ColumnType::Bit,
                    nullable: true,
                    collation: None,
                },
            ],
        }
    }

    #[test]
    fn row_round_trip() {
        let schema = schema();
        let values = vec![
            Datum::Int(42),
            Datum::NVarChar("åäö".to_string()),
            Datum::Decimal(12345),
            Datum::VarBinary(vec![1, 2, 3]),
            Datum::Bit(true),
        ];
        let bytes = encode_row(&schema, &values).expect("encode");
        assert_eq!(decode_row(&schema, &bytes).expect("decode"), values);
    }

    #[test]
    fn nulls_round_trip_and_empty_string_is_not_null() {
        let schema = schema();
        let values = vec![
            Datum::Int(1),
            Datum::Null,
            Datum::Null,
            Datum::VarBinary(Vec::new()),
            Datum::Null,
        ];
        let bytes = encode_row(&schema, &values).expect("encode");
        let decoded = decode_row(&schema, &bytes).expect("decode");
        assert_eq!(decoded, values);
        assert_eq!(decoded[3], Datum::VarBinary(Vec::new()), "empty != NULL");
    }

    #[test]
    fn arity_and_type_mismatches_error() {
        let schema = schema();
        assert!(encode_row(&schema, &[Datum::Int(1)]).is_err());
        let values = vec![
            Datum::BigInt(1), // wrong variant for INT column
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ];
        assert!(encode_row(&schema, &values).is_err());
    }

    #[test]
    fn oversized_row_is_rejected() {
        let schema = Schema {
            columns: vec![Column {
                name: "big".to_string(),
                column_type: ColumnType::VarBinary { max_len: u16::MAX },
                nullable: false,
                collation: None,
            }],
        };
        let values = vec![Datum::VarBinary(vec![0u8; MAX_ROW_BYTES + 1])];
        assert!(encode_row(&schema, &values).is_err());
    }
}
