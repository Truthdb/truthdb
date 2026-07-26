use super::super::prelude::*;

// ---- SELECT -------------------------------------------------------------

/// Rows a table scan reads per slice before dropping the storage lock and
/// letting another session in. Large enough that the per-slice overhead (a lock
/// acquisition and a catalog lookup) is noise against decoding the rows, small
/// enough that a big scan yields often.
pub(in crate::engine::relational) const SCAN_SLICE_ROWS: usize = 1024;

pub(in crate::engine::relational) struct Source {
    pub(in crate::engine::relational) columns: Vec<ResultColumn>,
    /// Per-column table qualifier (alias or table name; `None` = virtual/
    /// constant source), parallel to `columns`. Drives multi-table resolution.
    pub(in crate::engine::relational) qualifiers: Vec<Option<String>>,
    /// Per-column collation names (parallel to `columns`; `None` = database
    /// default). Used by ORDER BY on character columns.
    pub(in crate::engine::relational) collations: Vec<Option<String>>,
    /// Rows of typed values (real-table Datums; virtual sources build them).
    pub(in crate::engine::relational) rows: SourceRows,
}

/// A source's rows: already whole, or pulled slice-by-slice from a base-table
/// scan as the consumer iterates (Stage 8 streaming scans, the input side). A
/// consumer that filters or folds row-at-a-time holds one slice, not the
/// table; one that needs the whole input calls [`SourceRows::materialize`].
pub(in crate::engine::relational) enum SourceRows {
    Materialized(Vec<Vec<Datum>>),
    Scan(ScanStream),
}

/// A base-table scan not yet read: full-width rows, [`SCAN_SLICE_ROWS`] at a
/// time on the resumable cursor. The storage lock is taken per slice, as
/// everywhere since #96; under every isolation level that takes read locks
/// the table's S lock spans the whole batch, so lazy pulling changes no
/// isolation semantics — and READ UNCOMMITTED took no read lock before
/// either (the cursor's per-page object check safe-stops on a recycled
/// page, as the B+ tree layer documents).
pub(in crate::engine::relational) struct ScanStream {
    pub(super) db_id: u32,
    pub(super) table: String,
    pub(super) cursor: ScanCursor,
}

impl ScanStream {
    pub(super) fn next_slice(
        &mut self,
        storage: &Storage,
    ) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        let mut slice = Vec::new();
        while !self.cursor.done() && slice.is_empty() {
            check_cancelled()?;
            self.cursor = storage
                .rel_scan_slice(
                    self.db_id,
                    &self.table,
                    self.cursor,
                    SCAN_SLICE_ROWS,
                    None,
                    &mut slice,
                )
                .map_err(|err| map_storage_err(err, &self.table))?;
        }
        Ok(if slice.is_empty() { None } else { Some(slice) })
    }
}

/// A [`Source`] with its rows fully in hand — the join operators' BUILD side,
/// which is walked repeatedly (nested loop) or hashed whole (the grace-hash
/// spill bounds it past the memory budget). The probe side never takes this
/// form: it streams via [`SourceRows::next_slice`].
pub(in crate::engine::relational) struct MaterializedSource {
    pub(super) columns: Vec<ResultColumn>,
    pub(super) collations: Vec<Option<String>>,
    pub(super) rows: Vec<Vec<Datum>>,
}

impl MaterializedSource {
    pub(super) fn from(source: Source, storage: &Storage) -> Result<Self, SqlError> {
        let Source {
            columns,
            collations,
            rows,
            ..
        } = source;
        Ok(MaterializedSource {
            columns,
            collations,
            rows: rows.materialize(storage)?,
        })
    }
}

impl SourceRows {
    /// Pulls the next batch of rows, for a consumer that walks the source
    /// exactly once (a join's probe side). A scan hands over its next slice; a
    /// materialized source hands everything in one batch.
    pub(super) fn next_slice(
        &mut self,
        storage: &Storage,
    ) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        match self {
            SourceRows::Materialized(rows) => {
                if rows.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(std::mem::take(rows)))
                }
            }
            SourceRows::Scan(stream) => stream.next_slice(storage),
        }
    }

    /// The whole input, for consumers that need it at once. A scan drains its
    /// remaining slices; a materialized source hands its rows over as-is.
    pub(super) fn materialize(self, storage: &Storage) -> Result<Vec<Vec<Datum>>, SqlError> {
        match self {
            SourceRows::Materialized(rows) => Ok(rows),
            SourceRows::Scan(mut stream) => {
                #[cfg(test)]
                storage.count_scan_materialization();
                let mut rows = Vec::new();
                while let Some(mut slice) = stream.next_slice(storage)? {
                    rows.append(&mut slice);
                }
                Ok(rows)
            }
        }
    }
}

impl Source {
    pub(super) fn types(&self) -> Vec<ColumnType> {
        self.columns.iter().map(|c| c.column_type).collect()
    }

    pub(super) fn scope(&self) -> JoinScope {
        JoinScope {
            columns: self
                .qualifiers
                .iter()
                .zip(&self.columns)
                .map(|(qualifier, column)| (qualifier.clone(), column.name.clone()))
                .collect(),
            collations: self.collations.clone(),
        }
    }
}

/// Resolves column references against a (possibly multi-table) row source. A
/// dotted `t.col` matches by qualifier + name; a bare `col` matches a unique
/// column (ambiguous or unknown → `None`, surfaced by eval as an invalid-
/// column error).
pub(in crate::engine::relational) struct JoinScope {
    /// (qualifier, bare column name) per source column.
    pub(super) columns: Vec<(Option<String>, String)>,
    /// Per-column collation names, parallel to `columns` (`None` = database
    /// default). Empty for correlation-only scopes that never drive comparison.
    pub(super) collations: Vec<Option<String>>,
}

/// Resolver over an output RowSet's columns. Output columns are unqualified,
/// so a qualified `t.col` reference (e.g. in a grouped query's ORDER BY)
/// resolves by its bare name.
///
/// It does not carry per-column collation, so an *embedded equality* in an
/// ORDER BY expression over a `_CS`/`_BIN` output column (e.g.
/// `ORDER BY CASE WHEN code = 'ABC' THEN 0 ELSE 1 END`) folds case
/// (case-insensitive default). The sort key itself is collation-correct — the
/// non-aggregated path orders via `sort_collators` (real per-column collation)
/// and the aggregated/DISTINCT path via `order_key_cmp` — so this only affects a
/// nested `=` inside an ORDER BY expression on a case-sensitive column: a narrow,
/// documented limitation.
pub(in crate::engine::relational) struct OutputScope {
    pub(super) names: Vec<String>,
}

impl truthdb_sql::eval::ColumnResolver for OutputScope {
    fn resolve(&self, name: &str) -> Option<usize> {
        let bare = name.rsplit('.').next().unwrap_or(name);
        self.names.iter().position(|n| n.eq_ignore_ascii_case(bare))
    }
}

impl JoinScope {
    /// True if any column matches `name` — even ambiguously (>1 match), where
    /// [`ColumnResolver::resolve`] returns `None`. Correlation analysis uses this
    /// to tell "the inner scope has this name (bind/error here)" from "the name
    /// is absent (it is an outer reference)": an ambiguous inner column must NOT
    /// be rebound to a same-named outer column.
    pub(super) fn matches_any(&self, name: &str) -> bool {
        self.columns.iter().any(|(qualifier, column)| {
            if let Some((q, c)) = name.rsplit_once('.') {
                qualifier
                    .as_deref()
                    .is_some_and(|qq| qq.eq_ignore_ascii_case(q))
                    && column.eq_ignore_ascii_case(c)
            } else {
                column.eq_ignore_ascii_case(name)
            }
        })
    }

    /// Source-column indices belonging to a table qualifier (for `t.*`).
    pub(super) fn indices_for_qualifier(&self, qualifier: &str) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, (q, _))| {
                q.as_deref()
                    .is_some_and(|q| q.eq_ignore_ascii_case(qualifier))
            })
            .map(|(index, _)| index)
            .collect()
    }
}

impl truthdb_sql::eval::ColumnResolver for JoinScope {
    fn resolve(&self, name: &str) -> Option<usize> {
        match self.resolve_detail(name) {
            truthdb_sql::eval::Resolution::Found(index) => Some(index),
            // Ambiguous and not-found both fail to bind a single column.
            _ => None,
        }
    }

    fn resolve_detail(&self, name: &str) -> truthdb_sql::eval::Resolution {
        use truthdb_sql::eval::Resolution;
        let matches = |q: &Option<String>, c: &str| -> bool {
            if let Some((qualifier, column)) = name.rsplit_once('.') {
                q.as_deref()
                    .is_some_and(|q| q.eq_ignore_ascii_case(qualifier))
                    && c.eq_ignore_ascii_case(column)
            } else {
                c.eq_ignore_ascii_case(name)
            }
        };
        let mut found = None;
        for (index, (qualifier, column)) in self.columns.iter().enumerate() {
            if matches(qualifier, column) {
                if found.is_some() {
                    return Resolution::Ambiguous; // more than one match
                }
                found = Some(index);
            }
        }
        match found {
            Some(index) => Resolution::Found(index),
            None => Resolution::NotFound,
        }
    }

    fn collation(&self, index: usize) -> truthdb_sql::collation::CollationSensitivity {
        truthdb_sql::collation::CollationSensitivity::from_optional(
            self.collations.get(index).and_then(|c| c.as_deref()),
        )
    }
}

/// SqlValues of a row, for expression evaluation. `types` (parallel to `row`)
/// restores each value's exact type (e.g. a DECIMAL's scale).
pub(in crate::engine::relational) fn row_values(
    row: &[Datum],
    types: &[ColumnType],
) -> Vec<SqlValue> {
    row.iter()
        .zip(types)
        .map(|(d, t)| value::datum_to_sql(d, t))
        .collect()
}
