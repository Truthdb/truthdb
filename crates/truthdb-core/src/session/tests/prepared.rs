use super::*;

#[tokio::test]
async fn prepare_execute_unprepare_roundtrip() {
    let h = start(LOCK_WAIT_TIMEOUT);
    let s = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    fill(&h, s, 5).await;

    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::Prepare {
            decls: "@p1 int".into(),
            stmt: "SELECT id FROM t WHERE id = @p1".into(),
        },
        no_cancel(),
    ))
    .await;
    assert_eq!(event_error(&events), None, "{events:?}");
    let handle = handle_of(&events).expect("prepare returns a handle");

    // Execute twice with different values: the same handle re-binds each
    // time, and the unnamed wire value takes the declaration's name.
    for wanted in [3, 5] {
        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Execute {
                handle,
                values: vec![int_param(wanted)],
            },
            no_cancel(),
        ))
        .await;
        assert_eq!(event_error(&events), None, "{events:?}");
        assert_eq!(rows_of(&events), vec![vec![Datum::Int(wanted)]]);
    }

    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::Unprepare { handle },
        no_cancel(),
    ))
    .await;
    assert_eq!(event_error(&events), None, "{events:?}");

    // The dropped handle is gone: 8179, SQL Server's number for it.
    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::Execute {
            handle,
            values: vec![int_param(1)],
        },
        no_cancel(),
    ))
    .await;
    assert_eq!(event_error(&events), Some(8179), "{events:?}");
}

#[tokio::test]
async fn an_unknown_handle_answers_8179() {
    let h = start(LOCK_WAIT_TIMEOUT);
    let s = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::Execute {
            handle: 42,
            values: Vec::new(),
        },
        no_cancel(),
    ))
    .await;
    assert_eq!(event_error(&events), Some(8179));
    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::Unprepare { handle: 42 },
        no_cancel(),
    ))
    .await;
    assert_eq!(event_error(&events), Some(8179));
}

#[tokio::test]
async fn a_parse_error_at_prepare_allocates_no_handle() {
    let h = start(LOCK_WAIT_TIMEOUT);
    let s = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::Prepare {
            decls: String::new(),
            stmt: "SELEC oops".into(),
        },
        no_cancel(),
    ))
    .await;
    assert!(event_error(&events).is_some(), "{events:?}");
    assert_eq!(handle_of(&events), None, "no handle on a failed prepare");

    // The failed prepare consumed nothing: the next handle is still 1.
    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::Prepare {
            decls: String::new(),
            stmt: "SELECT 1 AS one".into(),
        },
        no_cancel(),
    ))
    .await;
    assert_eq!(handle_of(&events), Some(1));
}

#[tokio::test]
async fn ddl_between_prepare_and_execute_sees_the_new_schema() {
    // There is no cached plan: every execute re-binds against the live
    // catalog, so DDL between prepare and execute needs no invalidation —
    // the same handle simply sees the new schema (the plan's
    // catalog_version/rebind machinery is moot by design).
    let h = start(LOCK_WAIT_TIMEOUT);
    let s = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    fill(&h, s, 3).await;
    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::Prepare {
            decls: String::new(),
            stmt: "SELECT * FROM t ORDER BY id".into(),
        },
        no_cancel(),
    ))
    .await;
    let handle = handle_of(&events).expect("prepare returns a handle");

    h.handle
        .run_batch(
            s,
            "DROP TABLE t; CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL); INSERT INTO t VALUES (7, 70)".into(),
        )
        .await
        .unwrap();

    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::Execute {
            handle,
            values: Vec::new(),
        },
        no_cancel(),
    ))
    .await;
    assert_eq!(event_error(&events), None, "{events:?}");
    // The wildcard expands against the NEW table: two columns, new row.
    assert_eq!(rows_of(&events), vec![vec![Datum::Int(7), Datum::Int(70)]]);
}

#[tokio::test]
async fn prepexec_reports_the_handle_after_the_results() {
    let h = start(LOCK_WAIT_TIMEOUT);
    let s = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    fill(&h, s, 4).await;
    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::PrepExec {
            decls: "@p1 int".into(),
            stmt: "SELECT id FROM t WHERE id > @p1 ORDER BY id".into(),
            values: vec![int_param(2)],
        },
        no_cancel(),
    ))
    .await;
    assert_eq!(event_error(&events), None, "{events:?}");
    assert_eq!(
        rows_of(&events),
        vec![vec![Datum::Int(3)], vec![Datum::Int(4)]]
    );
    // Return values follow every result set: the handle event comes after
    // the statement's DONE, immediately before Complete.
    let positions: Vec<&str> = events
        .iter()
        .map(|e| match e {
            BatchEvent::Columns(_) => "columns",
            BatchEvent::Rows(_) => "rows",
            BatchEvent::StatementDone { .. } => "done",
            BatchEvent::PreparedHandle(_) => "handle",
            BatchEvent::Complete { .. } => "complete",
            _ => "other",
        })
        .collect();
    assert_eq!(
        positions.iter().rev().take(2).copied().collect::<Vec<_>>(),
        ["complete", "handle"],
        "{positions:?}"
    );
    // And the stored handle is executable afterwards.
    let handle = handle_of(&events).expect("prepexec returns a handle");
    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::Execute {
            handle,
            values: vec![int_param(3)],
        },
        no_cancel(),
    ))
    .await;
    assert_eq!(rows_of(&events), vec![vec![Datum::Int(4)]]);
}

#[tokio::test]
async fn describe_first_result_set_covers_static_shapes_only() {
    let h = start(LOCK_WAIT_TIMEOUT);
    let s = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    h.handle
        .run_batch(
            s,
            "CREATE TABLE d (id INT NOT NULL PRIMARY KEY, name NVARCHAR(40))".into(),
        )
        .await
        .unwrap();

    // A parameterized single-table SELECT describes without executing.
    // The @p1 is unresolvable in describe's default context — the planner
    // swallows that and falls back to a scan (non-sargable predicate);
    // the columns come from the table schema either way.
    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::Describe {
            tsql: "SELECT id, name FROM d WHERE id = @p1".into(),
        },
        no_cancel(),
    ))
    .await;
    assert_eq!(event_error(&events), None, "{events:?}");
    let rows = rows_of(&events);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Datum::Int(1)); // column_ordinal
    assert_eq!(rows[0][2], Datum::NVarChar("id".into()));
    assert_eq!(rows[0][4], Datum::Int(56)); // system_type_id: int
    assert_eq!(rows[1][2], Datum::NVarChar("name".into()));
    assert_eq!(rows[1][5], Datum::NVarChar("nvarchar(40)".into()));

    // A statement producing no result set describes as zero rows — and
    // describing it executes nothing: the table stays empty.
    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::Describe {
            tsql: "INSERT INTO d VALUES (1, 'x')".into(),
        },
        no_cancel(),
    ))
    .await;
    assert_eq!(event_error(&events), None, "{events:?}");
    assert_eq!(rows_of(&events), Vec::<Vec<Datum>>::new());
    let reply = h
        .handle
        .run_batch(s, "SELECT COUNT(*) FROM d".into())
        .await
        .unwrap();
    match &reply.outcome.results[0] {
        StatementResult::Rows(rowset) => assert_eq!(
            rowset.rows[0][0],
            Datum::BigInt(0),
            "describe must not execute the INSERT"
        ),
        other => panic!("expected rows, got {other:?}"),
    }

    // The contract is the first RESULT SET, not the first statement:
    // `INSERT; SELECT` (and a SELECT inside a TRY block) describe the
    // SELECT, and `TOP 0` — the standard metadata-discovery idiom —
    // describes like any other TOP.
    for tsql in [
        "INSERT INTO d VALUES (9, 'y'); SELECT id FROM d",
        "BEGIN TRY SELECT id FROM d END TRY BEGIN CATCH END CATCH",
        "SELECT TOP 0 id FROM d",
    ] {
        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Describe { tsql: tsql.into() },
            no_cancel(),
        ))
        .await;
        assert_eq!(event_error(&events), None, "{tsql}: {events:?}");
        let rows = rows_of(&events);
        assert_eq!(rows.len(), 1, "{tsql}");
        assert_eq!(rows[0][2], Datum::NVarChar("id".into()), "{tsql}");
    }

    // A shape whose types are only known by executing answers 11514.
    let events = drain_events(h.handle.stream_prepared(
        s,
        PreparedRpc::Describe {
            tsql: "SELECT a.id FROM d a JOIN d b ON a.id = b.id".into(),
        },
        no_cancel(),
    ))
    .await;
    assert_eq!(event_error(&events), Some(11514), "{events:?}");
}

#[test]
fn decl_names_splits_top_level_commas_only() {
    assert_eq!(
        crate::engine::decl_names("@p1 int, @p2 nvarchar(10), @p3 decimal(10,2)"),
        ["@p1", "@p2", "@p3"]
    );
    assert_eq!(crate::engine::decl_names(""), Vec::<String>::new());
    assert_eq!(crate::engine::decl_names("@a int"), ["@a"]);
    // A quoted default may contain commas and parens; a doubled ''
    // escape stays inside the string.
    assert_eq!(
        crate::engine::decl_names("@p1 varchar(10) = 'a,b', @p2 int, @p3 varchar(5) = 'it''s, ok'"),
        ["@p1", "@p2", "@p3"]
    );
}

#[test]
fn bind_decl_names_keeps_existing_names() {
    let mut named = int_param(1);
    named.name = "@mine".into();
    let bound = bind_decl_names("@p1 int, @p2 int", vec![named, int_param(2)]).expect("bind");
    assert_eq!(bound[0].name, "@mine");
    assert_eq!(bound[1].name, "@p2");
}

#[test]
fn more_values_than_declarations_is_8144() {
    // SQL Server rejects extra arguments rather than silently ignoring
    // them; without this an unmatched value seeded nothing and vanished.
    let err = bind_decl_names("@p1 int", vec![int_param(1), int_param(2)])
        .expect_err("extra value must be rejected");
    assert_eq!(err.number, 8144);
    let err = bind_decl_names("", vec![int_param(1)])
        .expect_err("values against an empty declaration list must be rejected");
    assert_eq!(err.number, 8144);
    // Fewer values than declarations stays legal (an unread declared
    // parameter goes unmissed).
    assert!(bind_decl_names("@p1 int, @p2 int", vec![int_param(1)]).is_ok());
    // Extra NAMED values pass through — they seed variables by their own
    // names (the run_rpc wrappers' contract).
    let mut named = int_param(9);
    named.name = "@extra".into();
    assert!(bind_decl_names("", vec![named]).is_ok());
}
