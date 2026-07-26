use super::prelude::*;

/// Whether a statement can make a durable commit that the batch must fsync: any
/// write/DDL (its own autocommit, or an identity reservation's mini-commit even
/// inside a transaction) or a `COMMIT`. Conservative by design — it flags by
/// kind, not by transaction state, so a hidden mini-commit (e.g. identity) is
/// never missed. Reads, `BEGIN`, `ROLLBACK`, `SET` and `DECLARE` never commit.
pub(super) fn statement_may_commit(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_)
            | Statement::AlterDatabase(_)
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. }
            | Statement::Exec(_)
            | Statement::Block { .. }
            | Statement::If { .. }
            | Statement::While { .. }
            | Statement::CreateProcedure(_)
            | Statement::DropProcedure { .. }
            | Statement::CreateFunction(_)
            | Statement::DropFunction { .. }
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger { .. }
            | Statement::CreateLogin(_)
            | Statement::DropLogin { .. }
            | Statement::CreateUser(_)
            | Statement::DropUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::AlterRole { .. }
            | Statement::Permission(_)
            | Statement::SetTriggerState { .. }
            | Statement::Commit { .. }
    )
}

/// The table/database locks a batch needs, from its statements and the
/// session isolation level, deduped to the strongest mode per resource. The
/// engine acquires these up front (before running any statement) so a
/// conflicting batch can be parked and restarted cleanly.
///
/// A parse error yields no locks — execution re-parses and surfaces it.
/// `sys.*` views and unresolved tables take no lock (catalog reads are
/// unlocked; missing tables error at execution).
/// Object ids of the parent tables a table's foreign keys reference.
pub(super) fn fk_parent_object_ids(storage: &Storage, def: &TableDef) -> Vec<u32> {
    def.foreign_keys
        .iter()
        .filter_map(|fk| resolve_table(storage, def.database_id, &fk.parent).map(|p| p.object_id))
        .collect()
}

/// Object ids of the tables whose foreign keys reference `parent_name`.
pub(super) fn fk_child_object_ids(storage: &Storage, db_id: u32, parent_name: &str) -> Vec<u32> {
    storage
        .rel_tables()
        .into_iter()
        .filter(|t| {
            t.database_id == db_id
                && t.foreign_keys
                    .iter()
                    .any(|fk| fk.parent.eq_ignore_ascii_case(parent_name))
        })
        .map(|t| t.object_id)
        .collect()
}

/// True if any table has a foreign key referencing `name` — i.e. `name` is an
/// FK parent. Such a table keeps table-granular write locks so an FK
/// existence-read (Table IS on the parent) still serializes against a
/// concurrent change to the referenced row.
pub(super) fn is_fk_parent(storage: &Storage, db_id: u32, name: &str) -> bool {
    !fk_child_object_ids(storage, db_id, name).is_empty()
}

/// Above this many row-lock keys for one statement, `analyze_locks` escalates to
/// a single table lock (SQL Server-style lock escalation) rather than flooding
/// the lock table.
pub(super) const ROW_LOCK_ESCALATION_THRESHOLD: usize = 1000;

/// A key hash for the [`Resource::Row`] lock, from the row's clustered-key bytes.
pub(super) fn row_key_hash(
    schema: &Schema,
    key_columns: &[usize],
    key_values: &[Datum],
) -> Option<u64> {
    let bytes = crate::relstore::key::encode_key(schema, key_columns, key_values).ok()?;
    Some(xxh64(&bytes, 0))
}

/// True if the clustered key can be safely hashed for a row lock: no key column
/// is a floating type. REAL/FLOAT keys are excluded because `-0.0 == 0.0` (and
/// NaN) compare equal in evaluation but encode to distinct key bytes, so two
/// writers to one physical row could get distinct hashes and not serialize.
///
/// Character keys are safe even under a case-insensitive collation: the row-lock
/// hash is taken over the *folded* key (`encode_key` folds character keys by
/// collation, Stage 5), so `WHERE key = 'ABC'` and a concurrent write of `'abc'`
/// hash to the same row resource and serialize.
pub(super) fn key_columns_row_lockable(schema: &Schema, key_columns: &[usize]) -> bool {
    key_columns.iter().all(|&i| {
        !matches!(
            schema.columns[i].column_type,
            ColumnType::Real | ColumnType::Float
        )
    })
}

/// True if a literal may pin a key column for a row lock: the executor's
/// equality must be a direct same-domain match so the lock key equals the stored
/// row's key. The hazard is a **character** key compared to a non-string literal:
/// the executor coerces the stored string to the literal's number (many strings
/// → one number: `'05' = 5`), while the lock key coerces the number to one
/// canonical string — opposite directions that disagree. So a character key
/// column requires a string literal; other domains coerce unambiguously (or
/// `sql_to_datum` errors and the caller falls back).
pub(super) fn literal_pins_key(value: &SqlValue, column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::VarChar { .. } | ColumnType::NVarChar { .. } => {
            matches!(value, SqlValue::Str(_))
        }
        _ => true,
    }
}

/// True if the table has a secondary UNIQUE index. Such a table keeps
/// table-granular locks for INSERT/UPDATE: a Row X on the clustered key alone
/// would not serialize two writers colliding on the *unique index* key.
pub(super) fn has_secondary_unique_index(def: &TableDef) -> bool {
    def.indexes.iter().any(|ix| ix.unique)
}

/// Evaluates a constant literal expression (`5`, `'x'`, `-3`, NULL, …) to a
/// value. Returns `None` for anything that is not a self-contained literal —
/// a column reference, variable, function call, or subquery — so the caller
/// falls back to a coarser (table) lock rather than a wrong row key.
pub(super) fn eval_literal_const(expr: &Expr) -> Option<SqlValue> {
    if !is_literal_const(expr) {
        return None;
    }
    let empty: Vec<String> = Vec::new();
    eval::eval(expr, &[], &empty, &EvalContext::default()).ok()
}

/// True if `expr` is a self-contained literal (no columns/vars/functions/
/// subqueries): a literal, or a unary +/- over one.
pub(super) fn is_literal_const(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Null
        | ExprKind::Literal(_) => true,
        ExprKind::Unary { expr: inner, .. } => is_literal_const(inner),
        _ => false,
    }
}

/// True if `expr` contains any subquery node (scalar, EXISTS, or IN (SELECT)).
pub(super) fn expr_has_subquery(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Subquery(_) | ExprKind::Exists(_) | ExprKind::InSubquery { .. } => true,
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => false,
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. } => expr_has_subquery(e),
        ExprKind::Binary { left, right, .. } => expr_has_subquery(left) || expr_has_subquery(right),
        ExprKind::Like {
            expr: e, pattern, ..
        } => expr_has_subquery(e) || expr_has_subquery(pattern),
        ExprKind::InList { expr: e, list, .. } => {
            expr_has_subquery(e) || list.iter().any(expr_has_subquery)
        }
        ExprKind::Between {
            expr: e, low, high, ..
        } => expr_has_subquery(e) || expr_has_subquery(low) || expr_has_subquery(high),
        ExprKind::Function { args, .. } => args.iter().any(expr_has_subquery),
        ExprKind::Aggregate { arg, .. } => arg.as_deref().is_some_and(expr_has_subquery),
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            operand.as_deref().is_some_and(expr_has_subquery)
                || branches
                    .iter()
                    .any(|(w, r)| expr_has_subquery(w) || expr_has_subquery(r))
                || else_result.as_deref().is_some_and(expr_has_subquery)
        }
    }
}

/// The row-lock keys for an INSERT: `Some(hashes)` when the target is a
/// clustered table and every row supplies all key columns as constant literals
/// (so two concurrent inserters of *different* keys need not serialize).
/// `None` — fall back to a table lock — for a heap, an IDENTITY/defaulted key
/// (value is server-generated, unknown here), `INSERT ... SELECT`, a
/// non-constant key expression, or more keys than the escalation threshold.
pub(super) fn insert_row_key_hashes(def: &TableDef, insert: &Insert) -> Option<Vec<u64>> {
    if def.key_columns.is_empty() {
        return None;
    }
    let InsertSource::Values(value_rows) = &insert.source else {
        return None;
    };
    let schema = def.schema().ok()?;
    if !key_columns_row_lockable(&schema, &def.key_columns) {
        return None;
    }
    let ncols = schema.columns.len();
    let identity_col = def.identity.map(|s| s.column);
    // Column index for each value position (explicit list, else all non-identity
    // columns in order — matching `exec_insert`).
    let target: Vec<usize> = match &insert.columns {
        Some(names) => names
            .iter()
            .map(|n| column_index(&schema, &n.value))
            .collect::<Option<Vec<_>>>()?,
        None => (0..ncols).filter(|i| Some(*i) != identity_col).collect(),
    };
    let mut hashes = Vec::with_capacity(value_rows.len());
    for row in value_rows {
        if row.len() != target.len() {
            return None; // arity mismatch — executor will error; table-lock it
        }
        let mut key_values = vec![Datum::Null; ncols];
        for &kc in &def.key_columns {
            if Some(kc) == identity_col {
                return None; // server-generated key value
            }
            let pos = target.iter().position(|&t| t == kc)?; // key not supplied
            let value = eval_literal_const(&row[pos])?;
            let column = &schema.columns[kc];
            if !literal_pins_key(&value, &column.column_type) {
                return None;
            }
            key_values[kc] = value::sql_to_datum(&value, &column.column_type, &column.name).ok()?;
        }
        hashes.push(row_key_hash(&schema, &def.key_columns, &key_values)?);
        if hashes.len() > ROW_LOCK_ESCALATION_THRESHOLD {
            return None;
        }
    }
    Some(hashes)
}

/// The single row-lock key for a point UPDATE/DELETE: `Some(hash)` when the
/// WHERE clause is a subquery-free conjunction that pins *every* clustered-key
/// column to a constant literal. `None` — fall back to a table lock — otherwise
/// (heap, partial/absent key predicate, range/OR/subquery predicate).
pub(super) fn where_point_key_hash(def: &TableDef, where_clause: &Option<Expr>) -> Option<u64> {
    if def.key_columns.is_empty() {
        return None;
    }
    let where_clause = where_clause.as_ref()?;
    if expr_has_subquery(where_clause) {
        return None;
    }
    let schema = def.schema().ok()?;
    if !key_columns_row_lockable(&schema, &def.key_columns) {
        return None;
    }
    let mut conjuncts = Vec::new();
    flatten_and(where_clause, &mut conjuncts);
    let mut key_values = vec![Datum::Null; schema.columns.len()];
    let mut bound: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for conjunct in conjuncts {
        let ExprKind::Binary {
            op: ast::BinaryOp::Eq,
            left,
            right,
        } = &conjunct.kind
        else {
            continue;
        };
        let (name, value_expr) = match (&left.kind, &right.kind) {
            (ExprKind::Column(n), _) => (n, right.as_ref()),
            (_, ExprKind::Column(n)) => (n, left.as_ref()),
            _ => continue,
        };
        let Some(ci) = column_index(&schema, &name.value) else {
            continue;
        };
        if !def.key_columns.contains(&ci) {
            continue;
        }
        let Some(value) = eval_literal_const(value_expr) else {
            continue;
        };
        let column = &schema.columns[ci];
        if !literal_pins_key(&value, &column.column_type) {
            continue;
        }
        if let Ok(datum) = value::sql_to_datum(&value, &column.column_type, &column.name) {
            key_values[ci] = datum;
            bound.insert(ci);
        }
    }
    if def.key_columns.iter().any(|kc| !bound.contains(kc)) {
        return None; // not every key column pinned
    }
    row_key_hash(&schema, &def.key_columns, &key_values)
}

/// The row-lock key for a point UPDATE: as [`where_point_key_hash`], but only
/// when no assignment targets a key column (a key change moves the row, touching
/// two keys) and no assignment value contains a subquery (which would read rows
/// the single row lock does not cover).
pub(super) fn update_row_key_hash(def: &TableDef, update: &Update) -> Option<u64> {
    let schema = def.schema().ok()?;
    for assignment in &update.assignments {
        let ci = column_index(&schema, &assignment.column.value)?;
        if def.key_columns.contains(&ci) || expr_has_subquery(&assignment.value) {
            return None;
        }
    }
    where_point_key_hash(def, &update.where_clause)
}

/// Collects the database ids every `USE` in the batch -- including one hidden
/// in LITERAL `sp_executesql` text, whose inner statements execute under it --
/// can switch to. Bounded like the analysis recursion; a non-literal dynamic
/// batch contributes nothing here (its analysis arm already locks the
/// database exclusively). Procedure bodies cannot contain USE (parser 154).
pub(super) fn collect_use_targets(
    storage: &Storage,
    statements: &[Statement],
    depth: u32,
    dbs: &mut Vec<u32>,
) {
    if depth > 32 {
        return;
    }
    let mut flat = Vec::new();
    flatten_statements(statements, &mut flat);
    for statement in &flat {
        match statement {
            Statement::Use { database, .. } => {
                if let Some(id) = storage.rel_database_id_by_name(&database.value)
                    && !dbs.contains(&id)
                {
                    dbs.push(id);
                }
            }
            Statement::Exec(exec) => {
                if let Some(inner) = exec_literal_sql(exec)
                    && let Ok(parsed) = truthdb_sql::parse(&inner)
                {
                    collect_use_targets(storage, &parsed, depth + 1, dbs);
                }
            }
            _ => {}
        }
    }
}

pub fn analyze_locks(
    storage: &Storage,
    db_id: u32,
    sql: &str,
    isolation: Isolation,
) -> Vec<(Resource, LockMode)> {
    let Ok(parsed) = truthdb_sql::parse(sql) else {
        return Vec::new();
    };
    // A batch that switches databases mid-stream (`USE`) executes later
    // statements in the new context, but this analysis runs once, up front.
    // Resolve under EVERY database context the batch can reach and take the
    // union: over-locking is safe, under-locking is the 2PL hole. (A failed
    // USE leaves the old context — also covered, it is in the set.)
    let mut dbs = vec![db_id];
    collect_use_targets(storage, &parsed, 0, &mut dbs);
    if dbs.len() > 1 {
        let mut out: Vec<(Resource, LockMode)> = Vec::new();
        for db in dbs {
            let mut visited = std::collections::HashSet::new();
            let mut trigger_visited = std::collections::HashSet::new();
            for lock in analyze_statements_locks(
                storage,
                db,
                &parsed,
                isolation,
                &mut visited,
                &mut trigger_visited,
            ) {
                if !out.contains(&lock) {
                    out.push(lock);
                }
            }
        }
        return out;
    }
    // The visited set terminates recursive procedures. Keyed on (procedure,
    // effective analysis regime), NOT the name alone: a body's lock
    // contribution is ISOLATION-DEPENDENT (versioned RC contributes Database
    // IS; an escalated re-entry needs Table S), so a body re-entered under a
    // different regime must re-analyze — the review's HIGH showed a shared
    // body analyzed versioned first and then skipped under SERIALIZABLE,
    // executing with no Table S. The regime lattice is finite, so
    // termination survives.
    let mut visited = std::collections::HashSet::new();
    let mut trigger_visited = std::collections::HashSet::new();
    analyze_statements_locks(
        storage,
        db_id,
        &parsed,
        isolation,
        &mut visited,
        &mut trigger_visited,
    )
}

pub(super) fn analyze_statements_locks(
    storage: &Storage,
    db_id: u32,
    parsed: &[Statement],
    isolation: Isolation,
    visited: &mut std::collections::HashSet<(String, Isolation)>,
    trigger_visited: &mut std::collections::HashSet<(u32, Isolation)>,
) -> Vec<(Resource, LockMode)> {
    // Flatten TRY/CATCH so the locks a batch needs are pre-acquired for the
    // statements inside its try/catch blocks too, not just the top level.
    let mut statements: Vec<&Statement> = Vec::new();
    flatten_statements(parsed, &mut statements);
    // Reads take shared locks except under READ UNCOMMITTED, which takes none.
    // A batch that raises the isolation level (e.g. `SET ISOLATION LEVEL
    // SERIALIZABLE; SELECT ...`) must lock its reads even if the session was
    // READ UNCOMMITTED on entry — otherwise the post-SET read would run
    // unlocked. We therefore take read locks unless the whole batch is READ
    // UNCOMMITTED: the session is RU and no SET raises it above RU.
    // SNAPSHOT is a versioned level, not a raise: a SET to it must not force
    // lock-based analysis (its whole point is to read without Table S).
    let escalates_reads = statements.iter().any(|s| {
        matches!(
            s,
            Statement::Set(SetStatement::IsolationLevel(level))
                if !matches!(level, IsolationLevel::ReadUncommitted | IsolationLevel::Snapshot)
        )
    });
    // A batch that SETs SNAPSHOT mid-stream still read-locks (statements
    // before the SET run at the session level, and batch analysis cannot see
    // the boundary) — but it must at least hold the Database IS fence, so an
    // RU session's `SET SNAPSHOT; SELECT` is not entirely lock-free.
    let sets_snapshot = statements.iter().any(|s| {
        matches!(
            s,
            Statement::Set(SetStatement::IsolationLevel(IsolationLevel::Snapshot))
        )
    });
    let reads_lock =
        !matches!(isolation, Isolation::ReadUncommitted) || escalates_reads || sets_snapshot;
    // Versioned reads take Database IS only — the DDL fence for the batch's
    // duration — and no Table S: READ COMMITTED under RCSI (per-statement
    // snapshots) and SNAPSHOT isolation (the transaction's snapshot). A batch
    // whose SET raises the level is analyzed lock-based (conservative: the
    // raise is seen here, the exact statement boundary is not).
    let versioned_reads = !escalates_reads
        && (matches!(isolation, Isolation::Snapshot)
            || (matches!(isolation, Isolation::ReadCommitted) && storage.rcsi_enabled()));
    // The isolation a fired trigger body (and any EXEC it makes) must be analyzed
    // under: an in-line SET that raises the level locks the body's reads too, so
    // forward a lock-based level whenever this batch locks reads — the SAME
    // correction the EXEC path applies. Without it a trigger body under a
    // versioned session (Snapshot / RCSI) would recompute versioned_reads=true
    // and drop the Table S it actually reads lock-based at runtime (a dirty read,
    // the Stage-13 seam class).
    let nested_isolation = if reads_lock {
        if matches!(isolation, Isolation::ReadCommitted | Isolation::Snapshot) && !escalates_reads {
            isolation
        } else {
            Isolation::RepeatableRead
        }
    } else {
        isolation
    };
    let mut needs: std::collections::HashMap<Resource, LockMode> = std::collections::HashMap::new();
    let mut add = |resource: Resource, mode: LockMode| {
        needs
            .entry(resource)
            .and_modify(|m| *m = m.combine(mode))
            .or_insert(mode);
    };
    for statement in statements.iter().copied() {
        match statement {
            Statement::Select(select) => {
                if !reads_lock {
                    continue;
                }
                // Lock every base table the query reads — the FROM clause AND
                // any subqueries in its expressions (WHERE/SELECT/HAVING/...).
                // CTEs are inlined first so their base tables are counted.
                let expanded = expand_ctes(select);
                let mut tables = Vec::new();
                collect_locked_tables(&expanded, &mut tables);
                for name in tables {
                    for oid in read_lock_object_ids(storage, db_id, &name.value) {
                        add(Resource::Database, LockMode::IntentShared);
                        if !versioned_reads {
                            add(Resource::Table(oid), LockMode::Shared);
                        }
                    }
                }
                // A scalar function the query calls reads tables through its
                // body; lock those up front too (2PL), or the body would read
                // with no lock held. read_lock_object_ids recurses the body.
                for oid in select_function_read_ids(storage, db_id, &expanded) {
                    add(Resource::Database, LockMode::IntentShared);
                    if !versioned_reads {
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                }
            }
            Statement::Insert(insert) => {
                if let Some(def) = resolve_table(storage, db_id, &insert.table.value) {
                    // Row X locks on each inserted key (two inserters of
                    // different keys then run concurrently under Table IX); a
                    // heap / IDENTITY / non-literal key falls back to Table X.
                    // A table referenced as an FK parent keeps Table X so an FK
                    // existence-read (Table IS) still serializes against it; a
                    // secondary UNIQUE index likewise needs table-granular
                    // serialization (the PK Row X does not cover its key).
                    let hashes =
                        if is_fk_parent(storage, def.database_id, &def.name) || has_secondary_unique_index(&def) {
                            None
                        } else {
                            insert_row_key_hashes(&def, insert)
                        };
                    match hashes {
                        Some(hashes) => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::IntentExclusive);
                            for hash in hashes {
                                add(Resource::Row(def.object_id, hash), LockMode::Exclusive);
                            }
                        }
                        None => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::Exclusive);
                        }
                    }
                    // A child INSERT reads its FK parents (integrity read).
                    for oid in fk_parent_object_ids(storage, &def) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                    // A firing AFTER-INSERT trigger's body reads/writes further
                    // tables; hold those locks up front too (strict 2PL).
                    add_trigger_locks(
                        db_id,
                        storage,
                        def.object_id,
                        catalog::TriggerEvent::Insert,
                        nested_isolation,
                        visited,
                        trigger_visited,
                        &mut add,
                    );
                }
                // INSERT ... SELECT also reads its source tables (and any
                // subqueries in the SELECT); lock them like a SELECT so it
                // cannot read another txn's uncommitted rows (they combine to
                // SIX on the target if it is a source).
                if reads_lock && let InsertSource::Select(select) = &insert.source {
                    let expanded = expand_ctes(select);
                    let mut tables = Vec::new();
                    collect_locked_tables(&expanded, &mut tables);
                    for name in tables {
                        for oid in read_lock_object_ids(storage, db_id, &name.value) {
                            add(Resource::Database, LockMode::IntentShared);
                            add(Resource::Table(oid), LockMode::Shared);
                        }
                    }
                    for oid in select_function_read_ids(storage, db_id, &expanded) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                }
            }
            Statement::Update(update) => {
                if let Some(def) = resolve_table(storage, db_id, &update.table.value) {
                    // A point UPDATE (WHERE pins the whole key, no key-column
                    // write, no subquery) takes Table IX + a single Row X. An FK
                    // parent or a secondary UNIQUE index keeps Table X (see INSERT).
                    let hash =
                        if is_fk_parent(storage, def.database_id, &def.name) || has_secondary_unique_index(&def) {
                            None
                        } else {
                            update_row_key_hash(&def, update)
                        };
                    match hash {
                        Some(hash) => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::IntentExclusive);
                            add(Resource::Row(def.object_id, hash), LockMode::Exclusive);
                        }
                        None => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::Exclusive);
                        }
                    }
                    // UPDATE reads FK parents (new values) and referencing
                    // children (a changed PK must not orphan them).
                    for oid in fk_parent_object_ids(storage, &def) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                    for oid in fk_child_object_ids(storage, def.database_id, &def.name) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                    add_trigger_locks(
                        db_id,
                        storage,
                        def.object_id,
                        catalog::TriggerEvent::Update,
                        nested_isolation,
                        visited,
                        trigger_visited,
                        &mut add,
                    );
                }
            }
            Statement::Delete(delete) => {
                if let Some(def) = resolve_table(storage, db_id, &delete.table.value) {
                    // A point DELETE (WHERE pins the whole key, no subquery)
                    // takes Table IX + a single Row X. A table referenced as an
                    // FK parent keeps Table X (see INSERT).
                    let hash = if is_fk_parent(storage, def.database_id, &def.name) {
                        None
                    } else {
                        where_point_key_hash(&def, &delete.where_clause)
                    };
                    match hash {
                        Some(hash) => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::IntentExclusive);
                            add(Resource::Row(def.object_id, hash), LockMode::Exclusive);
                        }
                        None => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::Exclusive);
                        }
                    }
                    // DELETE reads referencing children (NO ACTION check).
                    for oid in fk_child_object_ids(storage, def.database_id, &def.name) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                    add_trigger_locks(
                        db_id,
                        storage,
                        def.object_id,
                        catalog::TriggerEvent::Delete,
                        nested_isolation,
                        visited,
                        trigger_visited,
                        &mut add,
                    );
                }
            }
            // DDL serializes against every active transaction via a
            // database-exclusive lock (it is disallowed inside a txn anyway).
            Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_)
            // ALTER DATABASE quiesces the database: no snapshot may be live
            // and no writer mid-transaction while the options flip.
            | Statement::AlterDatabase(_)
            // CREATE/DROP DATABASE rewrite the catalog's database list; the
            // same quiesce keeps every in-flight resolution coherent.
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. } => {
                add(Resource::Database, LockMode::Exclusive);
            }
            // EXEC sp_executesql with a LITERAL statement is analyzable up
            // front: recurse into the inner text. Anything else (a variable
            // statement, an unknown procedure) cannot be analyzed before it
            // runs — lock the database exclusively rather than under-lock
            // (2PL acquires the full set up front).
            Statement::Exec(exec) => {
                // A user procedure: its stored body analyzes like literal
                // inner text, parsed with the IN-PROCEDURE grammar — a plain
                // parse would reject `RETURN <value>` (178), yield no locks,
                // and the body would run UNLOCKED (the 2PL hole class).
                if let Some(def) = resolve_table(storage, db_id, &exec.proc.value)
                    && let Some(procedure) = &def.procedure
                {
                    let inner_isolation = if reads_lock {
                        if matches!(isolation, Isolation::ReadCommitted | Isolation::Snapshot)
                            && !escalates_reads
                        {
                            isolation
                        } else {
                            Isolation::RepeatableRead
                        }
                    } else {
                        isolation
                    };
                    if visited.insert((def.name.clone(), inner_isolation))
                        && let Ok(body) = truthdb_sql::parse_procedure_body(&procedure.body)
                    {
                        // The body executes in the procedure's HOME database
                        // (run_user_procedure sets it; the body cannot USE) —
                        // analyze it there, or the two derivations diverge.
                        for (resource, mode) in analyze_statements_locks(
                            storage,
                            def.database_id,
                            &body,
                            inner_isolation,
                            visited,
                            trigger_visited,
                        ) {
                            add(resource, mode);
                        }
                    }
                    continue;
                }
                match exec_literal_sql(exec) {
                Some(inner) => {
                    // The inner text runs under the batch's EFFECTIVE
                    // isolation: a `SET ... SERIALIZABLE` before the EXEC
                    // must lock the inner reads too, so the recursion gets a
                    // read-locking level whenever this batch locks reads.
                    // (An inner SET raising isolation is seen by the
                    // recursion's own scan; it cannot outlive the EXEC — SET
                    // options revert at scope exit.)
                    //
                    // That level must be one the versioned-read path can
                    // never claim: under RCSI the recursion's own
                    // `versioned_reads` would drop Table S for a plain
                    // READ COMMITTED, while at runtime the inner statement
                    // executes under the OUTER effective level and reads
                    // lock-based — a reachable dirty read at SERIALIZABLE
                    // (caught by the adversarial review). READ COMMITTED is
                    // passed only when it truly is the effective level.
                    let inner_isolation = if reads_lock {
                        if matches!(
                            isolation,
                            Isolation::ReadCommitted | Isolation::Snapshot
                        ) && !escalates_reads
                        {
                            // Both survive the recursion faithfully: the
                            // inner analysis reaches the same versioned/
                            // lock-based decision execution will.
                            isolation
                        } else {
                            Isolation::RepeatableRead
                        }
                    } else {
                        isolation
                    };
                    if let Ok(parsed) = truthdb_sql::parse(&inner) {
                        for (resource, mode) in analyze_statements_locks(
                            storage,
                            db_id,
                            &parsed,
                            inner_isolation,
                            visited,
                            trigger_visited,
                        ) {
                            add(resource, mode);
                        }
                    }
                }
                None => add(Resource::Database, LockMode::Exclusive),
                }
            }
            // Procedure DDL rewrites the catalog: Database X, like other DDL.
            Statement::CreateProcedure(_)
            | Statement::DropProcedure { .. }
            | Statement::CreateFunction(_)
            | Statement::DropFunction { .. }
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger { .. }
            | Statement::CreateLogin(_)
            | Statement::DropLogin { .. }
            | Statement::CreateUser(_)
            | Statement::DropUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::AlterRole { .. }
            | Statement::Permission(_)
            | Statement::SetTriggerState { .. } => {
                add(Resource::Database, LockMode::Exclusive);
            }
            // IF/WHILE conditions read tables through their subqueries —
            // locked exactly like a SELECT's tables (their bodies were
            // flattened into this list and analyze as themselves).
            Statement::If { condition, .. } | Statement::While { condition, .. } => {
                if !reads_lock {
                    continue;
                }
                let mut tables = Vec::new();
                collect_expr_tables(condition, &mut tables);
                for name in tables {
                    for oid in read_lock_object_ids(storage, db_id, &name.value) {
                        add(Resource::Database, LockMode::IntentShared);
                        if !versioned_reads {
                            add(Resource::Table(oid), LockMode::Shared);
                        }
                    }
                }
                for oid in expr_function_read_ids(storage, db_id, condition) {
                    add(Resource::Database, LockMode::IntentShared);
                    if !versioned_reads {
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                }
            }
            // Transaction control, SET, and DECLARE take no data locks.
            // TRY/CATCH and plain blocks were flattened away by
            // `flatten_statements`, so their contained statements appear here
            // directly; BREAK/CONTINUE/RETURN touch nothing.
            Statement::Block { .. }
            | Statement::Break { .. }
            | Statement::Continue { .. }
            | Statement::Return { .. }
            | Statement::Goto { .. }
            | Statement::Label { .. }
            | Statement::BeginTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::SaveTransaction { .. }
            | Statement::Set(_)
            | Statement::Declare(_)
            | Statement::DeclareTableVar { .. }
            | Statement::Use { .. }
            | Statement::Throw(_)
            | Statement::RaiseError(_)
            | Statement::TryCatch { .. }
            // BACKUP takes no batch lock: it is online and manages its own
            // per-chunk storage locking. A Database X here would serialize it
            // against every writer and defeat the fuzzy design.
            | Statement::BackupDatabase { .. }
            | Statement::BackupLog { .. }
            // RESTORE VERIFYONLY/HEADERONLY/FILELISTONLY only read a backup file;
            // they touch no database object, so they take no lock.
            | Statement::Restore { .. }
            // Cursor statements take no batch lock. OPEN executes its query,
            // whose scans take their own per-slice storage locks (as every read
            // does); DECLARE/FETCH/CLOSE/DEALLOCATE touch only session state.
            | Statement::DeclareCursor { .. }
            | Statement::OpenCursor { .. }
            | Statement::FetchCursor { .. }
            | Statement::CloseCursor { .. }
            | Statement::DeallocateCursor { .. } => {}
        }
    }
    // Batch-level lock escalation: if a table accumulated more than the
    // threshold of row locks across the whole batch (many literal-key INSERTs,
    // a loop, or several point statements), replace them all with one Table X.
    // Bounds the lock set a batch can request regardless of per-statement caps.
    let mut row_counts: std::collections::HashMap<u32, usize> = std::collections::HashMap::new();
    for resource in needs.keys() {
        if let Resource::Row(oid, _) = resource {
            *row_counts.entry(*oid).or_default() += 1;
        }
    }
    let escalate: std::collections::HashSet<u32> = row_counts
        .into_iter()
        .filter(|(_, count)| *count > ROW_LOCK_ESCALATION_THRESHOLD)
        .map(|(oid, _)| oid)
        .collect();
    if !escalate.is_empty() {
        needs.retain(
            |resource, _| !matches!(resource, Resource::Row(oid, _) if escalate.contains(oid)),
        );
        for oid in escalate {
            needs
                .entry(Resource::Table(oid))
                .and_modify(|m| *m = m.combine(LockMode::Exclusive))
                .or_insert(LockMode::Exclusive);
            needs
                .entry(Resource::Database)
                .and_modify(|m| *m = m.combine(LockMode::IntentExclusive))
                .or_insert(LockMode::IntentExclusive);
        }
    }
    needs.into_iter().collect()
}
