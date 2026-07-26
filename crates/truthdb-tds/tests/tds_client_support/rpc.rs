use super::*;

/// A multi-RPC request (mssql-jdbc's default flow batches sp_unprepare with
/// the next sp_prepexec this way): each RPC answers its own DONEPROC-framed
/// reply — DONE_MORE on every DONEPROC but the last — inside one response.
#[tokio::test]
async fn a_multi_rpc_request_answers_each_rpc_in_one_response() {
    let path = temp_path("multirpc");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let mut body = sp_executesql_rpc("SELECT 1 AS a");
    body.push(0xff); // batch separator
    body.extend(sp_executesql_rpc("SELECT 2 AS b"));
    let tokens = client.rpc(&body).await;

    let ints: Vec<i64> = row_ints(&tokens);
    assert_eq!(ints, [1, 2], "tokens: {tokens:?}");
    let procs: Vec<(bool, bool)> = tokens
        .iter()
        .filter_map(|t| match t {
            Token::DoneProc { more, error, .. } => Some((*more, *error)),
            _ => None,
        })
        .collect();
    assert_eq!(
        procs,
        [(true, false), (false, false)],
        "every DONEPROC but the last carries DONE_MORE: {tokens:?}"
    );
    assert_eq!(
        tokens
            .iter()
            .filter(|t| matches!(t, Token::ReturnStatus(0)))
            .count(),
        2,
        "one RETURNSTATUS per RPC: {tokens:?}"
    );

    // An erroring RPC does not take the rest of the request with it: the
    // second RPC still runs and the response stays framed.
    let mut body = Vec::new();
    body.extend_from_slice(&0xffffu16.to_le_bytes());
    body.extend_from_slice(&15u16.to_le_bytes()); // sp_unprepare
    body.extend_from_slice(&0u16.to_le_bytes());
    body.push(0); // empty param name
    body.push(0); // status
    b_int(&mut body, 42); // a handle that was never prepared
    body.push(0xff);
    body.extend(sp_executesql_rpc("SELECT 3 AS c"));
    let tokens = client.rpc(&body).await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 8179, .. })),
        "tokens: {tokens:?}"
    );
    assert_eq!(row_ints(&tokens), [3], "tokens: {tokens:?}");
    let procs: Vec<(bool, bool)> = tokens
        .iter()
        .filter_map(|t| match t {
            Token::DoneProc { more, error, .. } => Some((*more, *error)),
            _ => None,
        })
        .collect();
    assert_eq!(procs, [(true, true), (false, false)], "tokens: {tokens:?}");

    // A decode-level error mid-request (an unknown procedure never reaches
    // the engine) renders in-frame and the RPCs after it still run.
    let mut body = Vec::new();
    body.extend_from_slice(&5u16.to_le_bytes()); // name length in chars
    body.extend(ucs2le("sp_no"));
    body.extend_from_slice(&0u16.to_le_bytes()); // option flags
    body.push(0xff);
    body.extend(sp_executesql_rpc("SELECT 4 AS d"));
    let tokens = client.rpc(&body).await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 2812, .. })),
        "tokens: {tokens:?}"
    );
    assert_eq!(row_ints(&tokens), [4], "tokens: {tokens:?}");

    let _ = std::fs::remove_file(&path);
}

/// An RPC-by-name call of a user procedure with a named OUTPUT parameter and a
/// RETURN carries the real status and the OUTPUT value back as RETURNSTATUS and
/// a typed RETURNVALUE.
#[tokio::test]
async fn rpc_by_name_returns_status_and_named_output() {
    let path = temp_path("rpc-named-output");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE PROCEDURE addone @x INT, @out INT OUTPUT AS BEGIN SET @out = @x + 1; RETURN 7; END")
        .await;

    let body = proc_rpc(
        "addone",
        &[
            ("@x", RpcArg::Int(10), false),
            ("@out", RpcArg::IntNull, true),
        ],
    );
    let tokens = client.rpc(&body).await;

    assert_eq!(return_status(&tokens), Some(7), "tokens: {tokens:?}");
    assert_eq!(
        return_values(&tokens),
        vec![(1, "@out".to_string(), Cell::Int(11))],
        "tokens: {tokens:?}"
    );
    // The reply frames as a procedure reply and ends clean.
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::DoneProc { error: false, .. })),
        "tokens: {tokens:?}"
    );
    let _ = std::fs::remove_file(&path);
}

/// A positional (unnamed-parameter) call — the JDBC `{call p(?, ?)}` shape —
/// binds by position and returns the OUTPUT value as a nameless RETURNVALUE the
/// driver matches by ordinal.
#[tokio::test]
async fn rpc_by_name_positional_output() {
    let path = temp_path("rpc-positional");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE PROCEDURE double2 @a INT, @b INT OUTPUT AS BEGIN SET @b = @a * 2; RETURN 3; END")
        .await;

    let body = proc_rpc(
        "double2",
        &[("", RpcArg::Int(21), false), ("", RpcArg::IntNull, true)],
    );
    let tokens = client.rpc(&body).await;

    assert_eq!(return_status(&tokens), Some(3), "tokens: {tokens:?}");
    assert_eq!(
        return_values(&tokens),
        vec![(1, String::new(), Cell::Int(42))],
        "tokens: {tokens:?}"
    );
    let _ = std::fs::remove_file(&path);
}

/// A typed OUTPUT parameter other than INT round-trips through the RETURNVALUE
/// encoder (exercises the TYPE_INFO path for a variable-length string).
#[tokio::test]
async fn rpc_by_name_nvarchar_output() {
    let path = temp_path("rpc-nvarchar");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch(
            "CREATE PROCEDURE greet @who NVARCHAR(50), @msg NVARCHAR(80) OUTPUT AS \
             BEGIN SET @msg = N'hello ' + @who; END",
        )
        .await;

    let body = proc_rpc(
        "greet",
        &[
            ("@who", RpcArg::NVarChar("world".to_string()), false),
            ("@msg", RpcArg::NVarChar(String::new()), true),
        ],
    );
    let tokens = client.rpc(&body).await;

    // No RETURN: the status defaults to 0, still emitted.
    assert_eq!(return_status(&tokens), Some(0), "tokens: {tokens:?}");
    assert_eq!(
        return_values(&tokens),
        vec![(1, "@msg".to_string(), Cell::Str("hello world".to_string()))],
        "tokens: {tokens:?}"
    );
    let _ = std::fs::remove_file(&path);
}

/// A NULL result in an OUTPUT parameter is returned as a NULL RETURNVALUE.
#[tokio::test]
async fn rpc_by_name_null_output() {
    let path = temp_path("rpc-null-output");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE PROCEDURE clear1 @out INT OUTPUT AS BEGIN SET @out = NULL; END")
        .await;

    let body = proc_rpc("clear1", &[("@out", RpcArg::Int(5), true)]);
    let tokens = client.rpc(&body).await;

    assert_eq!(
        return_values(&tokens),
        vec![(0, "@out".to_string(), Cell::Null)],
        "tokens: {tokens:?}"
    );
    let _ = std::fs::remove_file(&path);
}

/// An RPC-by-name call of a procedure that does not exist is error 2812, the
/// same as SQL Server, and no RETURNSTATUS or RETURNVALUE is emitted.
#[tokio::test]
async fn rpc_by_name_unknown_proc_is_2812() {
    let path = temp_path("rpc-unknown");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let body = proc_rpc("no_such_proc", &[("@x", RpcArg::Int(1), false)]);
    let tokens = client.rpc(&body).await;

    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 2812, .. })),
        "tokens: {tokens:?}"
    );
    assert!(
        return_values(&tokens).is_empty(),
        "a failed procedure returns no OUTPUT values: {tokens:?}"
    );
    let _ = std::fs::remove_file(&path);
}

/// Two procedure RPCs in one request each carry their own RETURNSTATUS and
/// RETURNVALUE tail — the tails do not bleed across the multi-RPC boundary.
#[tokio::test]
async fn multi_rpc_proc_calls_each_carry_their_own_tail() {
    let path = temp_path("rpc-multi-tail");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE PROCEDURE addk @x INT, @out INT OUTPUT AS BEGIN SET @out = @x + 100; RETURN @x; END")
        .await;

    let mut body = proc_rpc(
        "addk",
        &[
            ("@x", RpcArg::Int(1), false),
            ("@out", RpcArg::IntNull, true),
        ],
    );
    body.push(0xff); // batch separator
    body.extend(proc_rpc(
        "addk",
        &[
            ("@x", RpcArg::Int(2), false),
            ("@out", RpcArg::IntNull, true),
        ],
    ));
    let tokens = client.rpc(&body).await;

    let statuses: Vec<i32> = tokens
        .iter()
        .filter_map(|t| match t {
            Token::ReturnStatus(v) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(statuses, [1, 2], "one status per RPC, in order: {tokens:?}");
    assert_eq!(
        return_values(&tokens),
        vec![
            (1, "@out".to_string(), Cell::Int(101)),
            (1, "@out".to_string(), Cell::Int(102)),
        ],
        "each RPC's OUTPUT in order: {tokens:?}"
    );
    // Every DONEPROC but the last carries DONE_MORE.
    let procs: Vec<(bool, bool)> = tokens
        .iter()
        .filter_map(|t| match t {
            Token::DoneProc { more, error, .. } => Some((*more, *error)),
            _ => None,
        })
        .collect();
    assert_eq!(procs, [(true, false), (false, false)], "tokens: {tokens:?}");
    let _ = std::fs::remove_file(&path);
}

/// A procedure that raises a non-fatal error (severity 11-16) and then RETURNs
/// still *completes*, so its RETURN status and OUTPUT parameters are transmitted
/// alongside the error — as SQL Server does. The batch surfacing a continued
/// error must not suppress the tail (the emission gate and the copy-back gate
/// have to agree; they are unified on the procedure-completion signal).
#[tokio::test]
async fn rpc_by_name_completed_proc_with_warning_still_returns_tail() {
    let path = temp_path("rpc-warn-tail");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch(
            "CREATE PROCEDURE warnproc @x INT, @out INT OUTPUT AS \
             BEGIN SET @out = @x + 1; RAISERROR('warn', 16, 1); RETURN 7; END",
        )
        .await;

    let body = proc_rpc(
        "warnproc",
        &[
            ("@x", RpcArg::Int(10), false),
            ("@out", RpcArg::IntNull, true),
        ],
    );
    let tokens = client.rpc(&body).await;

    // The warning is delivered as an ERROR token and DONEPROC keeps DONE_ERROR,
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 50000, .. })),
        "tokens: {tokens:?}"
    );
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::DoneProc { error: true, .. })),
        "tokens: {tokens:?}"
    );
    // ...yet the completed procedure's status and OUTPUT are still returned.
    assert_eq!(return_status(&tokens), Some(7), "tokens: {tokens:?}");
    assert_eq!(
        return_values(&tokens),
        vec![(1, "@out".to_string(), Cell::Int(11))],
        "tokens: {tokens:?}"
    );
    let _ = std::fs::remove_file(&path);
}

/// Two OUTPUT parameters each carry their own 0-based ParamOrdinal — pytds
/// places OUTPUT values into the caller's list by this field, so a hardcoded 0
/// would collide both at index 0 and lose the first. Positional call.
#[tokio::test]
async fn rpc_by_name_multiple_outputs_carry_distinct_ordinals() {
    let path = temp_path("rpc-ordinals");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE PROCEDURE two @x INT OUTPUT, @y INT OUTPUT AS BEGIN SET @x = 1; SET @y = 2; END")
        .await;

    let body = proc_rpc(
        "two",
        &[("", RpcArg::IntNull, true), ("", RpcArg::IntNull, true)],
    );
    let tokens = client.rpc(&body).await;

    assert_eq!(
        return_values(&tokens),
        vec![
            (0, String::new(), Cell::Int(1)),
            (1, String::new(), Cell::Int(2)),
        ],
        "each OUTPUT carries its 0-based position, not a shared 0: {tokens:?}"
    );
    let _ = std::fs::remove_file(&path);
}

/// A RETURN value outside the int32 range overflows (error 8115) like SQL
/// Server, rather than being silently reported as a clean status 0.
#[tokio::test]
async fn rpc_by_name_out_of_range_return_is_8115() {
    let path = temp_path("rpc-bigrc");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE PROCEDURE bigrc AS BEGIN RETURN 3000000000; END")
        .await;

    let tokens = client.rpc(&proc_rpc("bigrc", &[])).await;

    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 8115, .. })),
        "tokens: {tokens:?}"
    );
    // The procedure aborted, so no clean status and no OUTPUT are reported.
    assert_eq!(return_status(&tokens), None, "tokens: {tokens:?}");
    assert!(return_values(&tokens).is_empty(), "tokens: {tokens:?}");
    let _ = std::fs::remove_file(&path);
}
