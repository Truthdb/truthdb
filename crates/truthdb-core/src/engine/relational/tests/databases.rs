use super::*;

#[test]
fn use_switches_context_with_canonical_casing_and_db_functions_track() {
    let path = unique_temp_path("multidb-use-canonical");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let first_row = |out: &BatchOutcome| -> Vec<Datum> {
        for result in &out.results {
            if let StatementResult::Rows(rowset) = result {
                return rowset.rows[0].clone();
            }
        }
        panic!("expected a rowset: {:?}", out.error);
    };
    assert!(
        batch(&engine, &mut ctx, "CREATE DATABASE Hr")
            .error
            .is_none(),
        "create"
    );
    // A case-variant USE lands in the catalog's spelling.
    assert!(batch(&engine, &mut ctx, "USE HR").error.is_none(), "use");
    let row = first_row(&batch(&engine, &mut ctx, "SELECT DB_NAME(), DB_ID()"));
    assert_eq!(row[0], Datum::NVarChar("Hr".into()), "canonical casing");
    assert_eq!(row[1], Datum::BigInt(2));
    // Objects created here live here; the default database has none.
    assert!(
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)"
        )
        .error
        .is_none()
    );
    assert!(batch(&engine, &mut ctx, "USE truthdb").error.is_none());
    let row = first_row(&batch(&engine, &mut ctx, "SELECT COUNT(*) FROM sys.tables"));
    assert_eq!(row[0], Datum::BigInt(0));
    let row = first_row(&batch(&engine, &mut ctx, "SELECT COUNT(*) FROM Hr.dbo.t"));
    assert_eq!(row[0], Datum::BigInt(0), "cross-db read");
    let _ = std::fs::remove_file(path);
}

#[test]
fn use_is_scoped_to_dynamic_sql_and_refused_in_stored_bodies() {
    let path = unique_temp_path("multidb-use-scope");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    assert!(
        batch(&engine, &mut ctx, "CREATE DATABASE hr")
            .error
            .is_none()
    );
    assert!(
        batch(
            &engine,
            &mut ctx,
            "USE hr; CREATE TABLE t (id INT NOT NULL PRIMARY KEY); USE truthdb"
        )
        .error
        .is_none()
    );
    // A USE inside sp_executesql is scoped to the dynamic batch: the
    // inner insert lands in hr, and the caller's context comes back.
    let out = batch(
        &engine,
        &mut ctx,
        "EXEC sp_executesql N'USE hr; INSERT INTO t VALUES (1)'",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let first_row = |out: &BatchOutcome| -> Vec<Datum> {
        for result in &out.results {
            if let StatementResult::Rows(rowset) = result {
                return rowset.rows[0].clone();
            }
        }
        panic!("expected a rowset: {:?}", out.error);
    };
    let row = first_row(&batch(&engine, &mut ctx, "SELECT DB_NAME()"));
    assert_eq!(
        row[0],
        Datum::NVarChar("truthdb".into()),
        "the context change must not outlive the dynamic batch"
    );
    let row = first_row(&batch(&engine, &mut ctx, "SELECT COUNT(*) FROM hr.dbo.t"));
    assert_eq!(row[0], Datum::BigInt(1), "the inner insert landed in hr");

    // The lock-analysis union sees the USE hidden in the literal dynamic
    // batch: the batch pre-acquires locks for hr's table (the 2PL hole
    // the review found).
    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "EXEC sp_executesql N'USE hr; INSERT INTO t VALUES (2)'",
        crate::engine::Isolation::ReadCommitted,
    );
    assert!(
        !locks.is_empty(),
        "a USE inside literal dynamic SQL must contribute its database's locks"
    );

    // USE is refused inside stored bodies at CREATE, like SQL Server 154.
    let err = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE p AS BEGIN USE hr; SELECT 1 END",
    )
    .error
    .expect("USE in a procedure body must be refused");
    assert_eq!(err.number, 154);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_dropped_database_errors_stale_sessions_and_never_reuses_its_id() {
    let path = unique_temp_path("multidb-drop-stale");
    let engine = new_engine(&path);
    let mut stale = TxnContext::default();
    let mut admin = TxnContext::default();
    assert!(
        batch(&engine, &mut admin, "CREATE DATABASE hr")
            .error
            .is_none()
    );
    assert!(batch(&engine, &mut stale, "USE hr").error.is_none());
    assert!(
        batch(&engine, &mut admin, "DROP DATABASE hr")
            .error
            .is_none()
    );

    // The stale session errors on its next statement (loud degradation)...
    let err = batch(&engine, &mut stale, "SELECT 1")
        .error
        .expect("a statement in a dropped database must error");
    assert_eq!(err.number, 911);
    // ...but USE is its way out.
    assert!(batch(&engine, &mut stale, "USE truthdb").error.is_none());
    assert!(batch(&engine, &mut stale, "SELECT 1").error.is_none());

    // The dropped id is tombstoned: the next CREATE DATABASE gets a fresh
    // id, so nothing can rebind a stale context into the new database.
    let first_row = |out: &BatchOutcome| -> Vec<Datum> {
        for result in &out.results {
            if let StatementResult::Rows(rowset) = result {
                return rowset.rows[0].clone();
            }
        }
        panic!("expected a rowset: {:?}", out.error);
    };
    assert!(
        batch(&engine, &mut admin, "CREATE DATABASE payroll")
            .error
            .is_none()
    );
    let row = first_row(&batch(&engine, &mut admin, "SELECT DB_ID('payroll')"));
    assert_eq!(
        row[0],
        Datum::BigInt(3),
        "hr's id 2 is retired, never reused"
    );

    // Reserved names are refused.
    for reserved in ["sys", "dbo", "master", "model", "msdb", "tempdb"] {
        assert!(
            batch(&engine, &mut admin, &format!("CREATE DATABASE {reserved}"))
                .error
                .is_some(),
            "{reserved} must be reserved"
        );
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn three_part_drop_and_cross_database_view_resolve_in_their_home_database() {
    let path = unique_temp_path("multidb-home");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let first_row = |out: &BatchOutcome| -> Vec<Datum> {
        for result in &out.results {
            if let StatementResult::Rows(rowset) = result {
                return rowset.rows[0].clone();
            }
        }
        panic!("expected a rowset: {:?}", out.error);
    };
    assert!(
        batch(&engine, &mut ctx, "CREATE DATABASE hr")
            .error
            .is_none()
    );
    assert!(
            batch(
                &engine,
                &mut ctx,
                "USE hr; CREATE TABLE t (id INT NOT NULL PRIMARY KEY);                  INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3);                  CREATE VIEW v AS SELECT id FROM t; USE truthdb"
            )
            .error
            .is_none()
        );
    // Same-named table in the session's database, with different contents.
    assert!(
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)"
        )
        .error
        .is_none()
    );
    // A cross-database view reads its HOME database's t (3 rows), not the
    // caller's empty one.
    let row = first_row(&batch(&engine, &mut ctx, "SELECT COUNT(*) FROM hr.dbo.v"));
    assert_eq!(
        row[0],
        Datum::BigInt(3),
        "view body resolves in its home database"
    );

    // DROP TABLE with a three-part name drops the NAMED database's table.
    assert!(
        batch(&engine, &mut ctx, "DROP TABLE hr.dbo.t")
            .error
            .is_none()
    );
    let row = first_row(&batch(&engine, &mut ctx, "SELECT COUNT(*) FROM t"));
    assert_eq!(row[0], Datum::BigInt(0), "the session's t survives");
    assert!(
        batch(&engine, &mut ctx, "SELECT COUNT(*) FROM hr.dbo.t")
            .error
            .is_some(),
        "hr's t is gone"
    );

    // A quoted identifier containing dots is one name (regression: it must
    // not be re-split as db.schema.object).
    assert!(
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE [dbo].[my.table] (id INT NOT NULL PRIMARY KEY)"
        )
        .error
        .is_none()
    );
    assert!(
        batch(&engine, &mut ctx, "INSERT INTO [my.table] VALUES (1)")
            .error
            .is_none()
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn wal_page_records_carry_their_database_as_the_container_tag() {
    use crate::wal::records::{
        REL_KIND_PAGE_IMAGE, REL_KIND_PAGE_IMAGES, REL_KIND_PAGE_OP, REL_KIND_TXN_BEGIN,
        REL_KIND_TXN_COMMIT,
    };
    let path = unique_temp_path("multidb-wal-tag");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    assert!(
        batch(&engine, &mut ctx, "CREATE DATABASE hr")
            .error
            .is_none()
    );
    assert!(
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY); INSERT INTO t1 VALUES (1); \
                 USE hr; CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY); INSERT INTO t2 VALUES (1)"
        )
        .error
        .is_none()
    );
    // A cross-database transaction rolled back: its CLRs compensate both
    // databases' pages under one context — they must all be tag 0.
    assert!(
        batch(
            &engine,
            &mut ctx,
            "USE truthdb; BEGIN TRANSACTION; INSERT INTO t1 VALUES (2); \
                 INSERT INTO hr.dbo.t2 VALUES (2); ROLLBACK TRANSACTION"
        )
        .error
        .is_none()
    );
    // rel_wal_records reads the replay cache scanned at OPEN — reopen the
    // file so the appended ring is visible.
    drop(engine);
    let storage = Storage::open(path.clone()).expect("reopen");
    let records = storage.rel_wal_records().expect("wal records");
    let tags: Vec<u16> = records
        .iter()
        .filter(|(_, r)| {
            matches!(
                r.kind,
                REL_KIND_PAGE_OP | REL_KIND_PAGE_IMAGE | REL_KIND_PAGE_IMAGES
            )
        })
        .map(|(_, r)| r.flags)
        .collect();
    // Both databases' page traffic is present and attributed.
    assert!(
        tags.contains(&1),
        "default-database pages tagged 1: {tags:?}"
    );
    assert!(tags.contains(&2), "hr's pages tagged 2: {tags:?}");
    // Transaction control stays global (a transaction can span containers).
    assert!(
        records
            .iter()
            .filter(|(_, r)| matches!(r.kind, REL_KIND_TXN_BEGIN | REL_KIND_TXN_COMMIT))
            .all(|(_, r)| r.flags == 0),
        "txn control must stay untagged"
    );
    // CLRs are never tagged — a rollback can span databases, and the
    // include-0-in-every-subscription rule is what keeps filtered copies
    // convergent through compensation.
    let clr_tags: Vec<u16> = records
        .iter()
        .filter(|(_, r)| r.kind == crate::wal::records::REL_KIND_CLR)
        .map(|(_, r)| r.flags)
        .collect();
    assert!(!clr_tags.is_empty(), "the rolled-back txn produced CLRs");
    assert!(
        clr_tags.iter().all(|&f| f == 0),
        "CLRs must stay untagged: {clr_tags:?}"
    );
    let _ = std::fs::remove_file(path);
}
