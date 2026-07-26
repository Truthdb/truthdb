use super::*;

impl StorageFile {
    /// Spills every (MAX) value above the inline threshold to an overflow
    /// chain, replacing the datum with a reference. Runs inside statement
    /// closures, before the row is encoded; chain pages are WAL-imaged, so
    /// they are crash-durable with the statement (and leak if it fails —
    /// the drop-table posture).
    pub(in crate::storage) fn spill_max_values(
        ctx: &mut RelCtx<'_>,
        schema: &Schema,
        values: &mut [Datum],
    ) -> Result<(), StorageError> {
        for (column, value) in schema.columns.iter().zip(values.iter_mut()) {
            if !column.column_type.is_max() || value.is_null() {
                continue;
            }
            let bytes = match value {
                Datum::VarChar(_) | Datum::NVarChar(_) | Datum::VarBinary(_) => value.encode_var(),
                _ => continue,
            };
            if bytes.len() <= OVERFLOW_INLINE_MAX {
                continue;
            }
            let first_page = overflow::write_chain(ctx, &bytes)?;
            *value = Datum::OverflowRef {
                total_len: bytes.len() as u64,
                first_page,
            };
        }
        Ok(())
    }

    /// Resolves overflow references in decoded rows back to their values.
    /// `types` must align with the rows' columns (the projection's types for
    /// projected reads).
    pub(in crate::storage) fn resolve_overflow_rows(
        &mut self,
        types: &[ColumnType],
        rows: &mut [Vec<Datum>],
    ) -> Result<(), StorageError> {
        if !types.iter().any(ColumnType::is_max) {
            return Ok(());
        }
        let mut ctx = self.rel_ctx();
        for row in rows.iter_mut() {
            for (column_type, value) in types.iter().zip(row.iter_mut()) {
                if let Datum::OverflowRef {
                    total_len,
                    first_page,
                } = *value
                {
                    let bytes = overflow::read_chain(&mut ctx, first_page, total_len)?;
                    let base = match column_type {
                        ColumnType::VarCharMax => ColumnType::VarChar { max_len: u16::MAX },
                        ColumnType::NVarCharMax => ColumnType::NVarChar { max_len: u16::MAX },
                        ColumnType::VarBinaryMax => ColumnType::VarBinary { max_len: u16::MAX },
                        other => {
                            return Err(StorageError::InvalidFile(format!(
                                "overflow reference under non-MAX column type {}",
                                other.name()
                            )));
                        }
                    };
                    *value = Datum::decode_var(&base, &bytes)?;
                }
            }
        }
        Ok(())
    }

    /// The column types a projection selects (`None` = every column).
    pub(in crate::storage) fn projected_types(
        schema: &Schema,
        projection: Option<&[usize]>,
    ) -> Vec<ColumnType> {
        match projection {
            None => schema.columns.iter().map(|c| c.column_type).collect(),
            Some(projection) => projection
                .iter()
                .map(|&i| schema.columns[i].column_type)
                .collect(),
        }
    }
}
