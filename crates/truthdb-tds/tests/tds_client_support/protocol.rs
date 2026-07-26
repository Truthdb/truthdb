use super::*;

#[tokio::test]
async fn full_handshake_query_and_error() {
    let path = temp_path("e2e");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;

    client.prelogin().await;
    let login = client.login("sa", "secret").await;
    assert!(login.contains(&Token::LoginAck), "login tokens: {login:?}");
    assert!(!login.iter().any(|t| matches!(t, Token::Error { .. })));

    // DDL + insert.
    client
        .batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(50), active BIT)")
        .await;
    let insert = client
        .batch("INSERT INTO t VALUES (1, 'Skor', 1), (2, 'Kangor', 0), (3, NULL, NULL)")
        .await;
    assert!(
        insert
            .iter()
            .any(|t| matches!(t, Token::Done { count: Some(3), .. })),
        "insert tokens: {insert:?}"
    );

    // SELECT: typed COLMETADATA + ROWs.
    let select = client
        .batch("SELECT id, name, active FROM t ORDER BY id")
        .await;
    let rows: Vec<&Vec<Cell>> = select
        .iter()
        .filter_map(|t| match t {
            Token::Row(cells) => Some(cells),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 3, "tokens: {select:?}");
    assert_eq!(
        *rows[0],
        vec![Cell::Int(1), Cell::Str("Skor".into()), Cell::Bool(true)]
    );
    assert_eq!(
        *rows[1],
        vec![Cell::Int(2), Cell::Str("Kangor".into()), Cell::Bool(false)]
    );
    assert_eq!(*rows[2], vec![Cell::Int(3), Cell::Null, Cell::Null]);

    // Error path: duplicate PK -> 2627 in the token stream.
    let dup = client.batch("INSERT INTO t VALUES (1, 'x', 1)").await;
    assert!(
        dup.iter()
            .any(|t| matches!(t, Token::Error { number: 2627, .. })),
        "dup tokens: {dup:?}"
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn computed_columns_and_constant_select() {
    let path = temp_path("computed");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    client
        .batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .await;
    client.batch("INSERT INTO t VALUES (10), (20)").await;
    let select = client.batch("SELECT id, id * 2 FROM t ORDER BY id").await;
    let rows: Vec<&Vec<Cell>> = select
        .iter()
        .filter_map(|t| match t {
            Token::Row(cells) => Some(cells),
            _ => None,
        })
        .collect();
    assert_eq!(*rows[0], vec![Cell::Int(10), Cell::Int(20)]);
    assert_eq!(*rows[1], vec![Cell::Int(20), Cell::Int(40)]);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn malformed_all_headers_is_rejected() {
    // A TotalLength that runs past the payload must be a protocol error. It was
    // previously treated as "no headers", handing the header bytes to the SQL
    // decoder as if they were the query.
    for bad in [
        // TotalLength beyond the payload.
        {
            let mut p = 9999u32.to_le_bytes().to_vec();
            p.extend(ucs2le("SELECT 1"));
            p
        },
        // TotalLength smaller than the field itself.
        {
            let mut p = 2u32.to_le_bytes().to_vec();
            p.extend(ucs2le("SELECT 1"));
            p
        },
        // A header whose HeaderLength is 0 (would stall the walk).
        {
            let mut h = 0u32.to_le_bytes().to_vec();
            h.extend_from_slice(&2u16.to_le_bytes());
            headers_block(&h)
        },
        // A header whose HeaderLength overruns the block.
        {
            let mut h = 999u32.to_le_bytes().to_vec();
            h.extend_from_slice(&2u16.to_le_bytes());
            headers_block(&h)
        },
        // A transaction-descriptor header with truncated data.
        {
            let mut h = (4u32 + 2 + 3).to_le_bytes().to_vec();
            h.extend_from_slice(&2u16.to_le_bytes());
            h.extend_from_slice(&[0, 0, 0]);
            headers_block(&h)
        },
    ] {
        let path = temp_path("bad-headers");
        let engine = engine(&path);
        let mut client = connect(engine).await;
        client.prelogin().await;
        client.login("sa", "secret").await;
        client.write_packet(PKT_SQL_BATCH, &bad).await;
        assert!(
            client.try_read_message().await.is_none(),
            "malformed ALL_HEADERS must close the connection, not answer"
        );
        let _ = std::fs::remove_file(path);
    }
}
