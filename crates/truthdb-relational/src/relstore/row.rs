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
            | (ColumnType::VarCharMax, Datum::VarChar(_))
            | (ColumnType::NVarCharMax, Datum::NVarChar(_))
            | (ColumnType::VarBinaryMax, Datum::VarBinary(_))
            | (ColumnType::VarCharMax, Datum::OverflowRef { .. })
            | (ColumnType::NVarCharMax, Datum::OverflowRef { .. })
            | (ColumnType::VarBinaryMax, Datum::OverflowRef { .. })
    )
}

/// The var payload of a (MAX) column: a tag byte, then either the inline
/// base encoding (0) or a 16-byte overflow-chain reference (1).
fn encode_max_var(value: &Datum) -> Vec<u8> {
    match value {
        Datum::OverflowRef {
            total_len,
            first_page,
        } => {
            let mut out = Vec::with_capacity(17);
            out.push(1);
            out.extend_from_slice(&total_len.to_le_bytes());
            out.extend_from_slice(&first_page.to_le_bytes());
            out
        }
        other => {
            let mut out = vec![0u8];
            out.extend(other.encode_var());
            out
        }
    }
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
            } else if column.column_type.is_max() {
                encode_max_var(value)
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
    decode_columns(schema, bytes, None)
}

/// Decodes only `projection`'s columns — schema indices, **ascending and
/// distinct** — returning them in that order, so the result is indexed by
/// position within `projection` rather than by schema index.
///
/// Every column's position is derived from the schema, not from the data: a
/// fixed column's slot is the running sum of the fixed sizes before it, and a
/// variable column's span is `[var_ends[k-1], var_ends[k])`, a direct index. So
/// the columns to skip cost only their offset arithmetic, and what is saved is
/// the decode itself — which for a character column is a `String` allocation.
///
/// The row's *structure* is validated exactly as [`decode_row`] validates it —
/// the header's length, the var section's, and every column's offsets, skipped
/// or not — so the columns that are read cannot be shifted by damage elsewhere
/// in the row.
///
/// A skipped column's *content* is not validated, because validating it is the
/// decode this exists to avoid: a row whose VARBINARY holds bytes that are not
/// the UTF-8 its column claims is rejected by [`decode_row`] and accepted here
/// by a query that does not select it. That is the deal projection pruning
/// makes — what is not read is not looked at — and it is why the structural
/// checks above are not part of it.
pub fn decode_row_projected(
    schema: &Schema,
    bytes: &[u8],
    projection: &[usize],
) -> Result<Vec<Datum>, TypeError> {
    debug_assert!(
        projection.windows(2).all(|w| w[0] < w[1]),
        "projection must be ascending and distinct"
    );
    decode_columns(schema, bytes, Some(projection))
}

/// [`decode_row`] and [`decode_row_projected`] over one body: `None` decodes
/// every column, `Some` only the listed ones.
fn decode_columns(
    schema: &Schema,
    bytes: &[u8],
    projection: Option<&[usize]>,
) -> Result<Vec<Datum>, TypeError> {
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

    let wanted_count = projection.map_or(schema.columns.len(), <[usize]>::len);
    let mut values = Vec::with_capacity(wanted_count);
    // The next projected index still to be reached. `projection` is ascending,
    // so one cursor decides every column in a single pass.
    let mut next_wanted = 0usize;
    let mut fixed_cursor = bitmap_len;
    let mut var_index = 0usize;
    for (index, column) in schema.columns.iter().enumerate() {
        let wanted = match projection {
            None => true,
            Some(projection) => {
                let hit = projection.get(next_wanted) == Some(&index);
                if hit {
                    next_wanted += 1;
                }
                hit
            }
        };
        match column.column_type.fixed_size() {
            Some(size) => {
                let raw = &bytes[fixed_cursor..fixed_cursor + size];
                fixed_cursor += size;
                if wanted {
                    values.push(if is_null(index) {
                        Datum::Null
                    } else {
                        Datum::decode_fixed(&column.column_type, raw)?
                    });
                }
            }
            None => {
                let start = if var_index == 0 {
                    0
                } else {
                    var_ends[var_index - 1]
                };
                let end = var_ends[var_index];
                var_index += 1;
                // Checked for every column, wanted or not: a corrupt row is
                // corrupt whichever columns the query happens to read.
                if start > end || end > var_data.len() {
                    return Err(TypeError("corrupt var offsets".to_string()));
                }
                if wanted {
                    values.push(if is_null(index) {
                        Datum::Null
                    } else {
                        Datum::decode_var(&column.column_type, &var_data[start..end])?
                    });
                }
            }
        }
    }
    if next_wanted != wanted_count && projection.is_some() {
        return Err(TypeError(format!(
            "projection names a column outside the schema's {} columns",
            schema.columns.len()
        )));
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
    fn a_projected_decode_reads_exactly_the_whole_decode_would_have() {
        // The oracle is the whole decode: every subset must equal the same
        // positions of it. Every subset, because the interesting cases are
        // structural — skipping a variable-width column must not disturb the
        // next one's offset, and skipping a fixed one must not disturb the
        // fixed cursor. The schema mixes both, and NULLs sit in both sections.
        let schema = schema();
        for values in [
            vec![
                Datum::Int(42),
                Datum::NVarChar("åäö".to_string()),
                Datum::Decimal(12345),
                Datum::VarBinary(vec![1, 2, 3]),
                Datum::Bit(true),
            ],
            // NULLs everywhere they are allowed: a NULL var column still owns
            // its offset slot, which is exactly what a skip could get wrong.
            vec![
                Datum::Int(7),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ],
            // Empty (not NULL) variable-width values: zero-length spans.
            vec![
                Datum::Int(0),
                Datum::NVarChar(String::new()),
                Datum::Decimal(0),
                Datum::VarBinary(Vec::new()),
                Datum::Bit(false),
            ],
        ] {
            let bytes = encode_row(&schema, &values).expect("encode");
            let whole = decode_row(&schema, &bytes).expect("decode");
            // Every subset of the five columns, in ascending order.
            for mask in 0u32..(1 << 5) {
                let projection: Vec<usize> = (0..5).filter(|i| mask & (1 << i) != 0).collect();
                let expected: Vec<Datum> = projection.iter().map(|&i| whole[i].clone()).collect();
                assert_eq!(
                    decode_row_projected(&schema, &bytes, &projection).expect("decode"),
                    expected,
                    "projection {projection:?} of {values:?}"
                );
            }
        }
    }

    #[test]
    fn a_projected_decode_still_rejects_a_structurally_corrupt_row() {
        // Structural damage shifts the columns that ARE read, so it must be
        // caught whatever the projection. The offset check inside the decode
        // loop is the one a `wanted` guard could plausibly skip, so the row here
        // is corrupted to reach *that* check specifically: a truncated row trips
        // the header-length check before the loop even starts, which would leave
        // the interesting branch untested.
        let schema = schema();
        let values = vec![
            Datum::Int(42),
            Datum::NVarChar("abc".to_string()),
            Datum::Decimal(1),
            Datum::VarBinary(vec![9]),
            Datum::Bit(true),
        ];
        let bytes = encode_row(&schema, &values).expect("encode");

        // Push the first variable column's end offset past the var section. The
        // *last* end still matches, so the pre-loop length check passes and only
        // the per-column check can catch this.
        let mut corrupt = bytes.clone();
        let var_offsets_start = schema.null_bitmap_len() + schema.fixed_section_len();
        corrupt[var_offsets_start] = 0xff;
        corrupt[var_offsets_start + 1] = 0xff;

        assert!(
            decode_row(&schema, &corrupt).is_err(),
            "the whole decode rejects it"
        );
        // Column 0 is the fixed INT — the projection never reads the damaged
        // column, and must still refuse the row.
        assert!(
            decode_row_projected(&schema, &corrupt, &[0]).is_err(),
            "a projection that skips the damaged column must still reject it"
        );
        // The same row, uncorrupted, decodes — so the assertions above are about
        // the damage and not about a projection that always errors.
        assert!(decode_row_projected(&schema, &bytes, &[0]).is_ok());
    }

    #[test]
    fn a_projected_decode_does_not_validate_what_it_does_not_read() {
        // The other half of the deal, and the point of pruning: a skipped
        // column's *content* is never looked at, so a row the whole decode
        // rejects is answered by a query that does not select the bad column.
        // Recorded as a test because it is a real behaviour change, not an
        // oversight — validating the column would be the decode being skipped.
        let schema = Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    column_type: ColumnType::Int,
                    nullable: false,
                    collation: None,
                },
                Column {
                    name: "text".to_string(),
                    column_type: ColumnType::VarChar { max_len: 20 },
                    nullable: true,
                    collation: None,
                },
            ],
        };
        // Encode through a VARBINARY-shaped schema so the bytes are laid out
        // legally, then read them back as the VARCHAR schema above: the row is
        // structurally perfect and its second column is not valid UTF-8.
        let binary_schema = Schema {
            columns: vec![
                schema.columns[0].clone(),
                Column {
                    column_type: ColumnType::VarBinary { max_len: 20 },
                    ..schema.columns[1].clone()
                },
            ],
        };
        let bytes = encode_row(
            &binary_schema,
            &[Datum::Int(7), Datum::VarBinary(vec![0xff, 0xfe, 0xff])],
        )
        .expect("encode");

        assert!(
            decode_row(&schema, &bytes).is_err(),
            "the whole decode reads the bad column and rejects it"
        );
        assert_eq!(
            decode_row_projected(&schema, &bytes, &[0]).expect("id decodes"),
            vec![Datum::Int(7)],
            "a projection that skips it answers with the column it was asked for"
        );
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
