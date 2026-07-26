use super::*;
use crate::packet::{MIN_PACKET_SIZE, read_message};
use truthdb_core::engine::{BatchOutcome, ResultColumn, RowSet, StatementResult};
use truthdb_core::relstore::types::{ColumnType, Datum};
use truthdb_sql::error::SqlError;

/// The pre-streaming renderer, verbatim. Do not "fix" this to agree with
/// the new code: it is the oracle, and its job is to disagree.
fn build_batch_tokens(outcome: &BatchOutcome, in_transaction: bool) -> Vec<u8> {
    let mut out = Vec::new();
    let has_error = outcome.error.is_some();
    let last_index = outcome.results.len().saturating_sub(1);
    for (index, result) in outcome.results.iter().enumerate() {
        let more = index != last_index || has_error;
        match result {
            StatementResult::Rows(rowset) => {
                token::colmetadata(&mut out, &rowset.columns);
                for row in &rowset.rows {
                    token::row(&mut out, row, &rowset.columns);
                }
                token::done(
                    &mut out,
                    more,
                    false,
                    in_transaction,
                    Some(rowset.rows.len() as u64),
                    token::CMD_SELECT,
                );
            }
            StatementResult::RowsAffected(n) => {
                token::done(&mut out, more, false, in_transaction, Some(*n), 0);
            }
            StatementResult::Done => {
                token::done(&mut out, more, false, in_transaction, None, 0);
            }
        }
    }
    if let Some(error) = &outcome.error {
        token::error(
            &mut out,
            error.number,
            error.state,
            error.level,
            &error.message,
        );
        token::done(&mut out, false, true, in_transaction, None, 0);
    } else if outcome.results.is_empty() {
        token::done(&mut out, false, false, in_transaction, None, 0);
    }
    out
}

/// Renders through the real write path and reassembles the message.
/// Renders `outcome` through the real path: as the event stream a worker
/// would produce, drained by `stream_reply` exactly as a connection drains
/// it. Nothing is short-circuited — the events go through a real channel and
/// the deferred DONE has to settle `DONE_MORE` by lookahead, which is the
/// part most likely to disagree with the oracle.
async fn rendered(outcome: &BatchOutcome, in_xact: bool, packet_size: usize) -> Vec<u8> {
    rendered_events(events_for(outcome, in_xact), packet_size).await
}

/// Renders an event stream through the real `stream_reply` and reassembles
/// the message payload.
async fn rendered_events(
    events: mpsc::UnboundedReceiver<BatchEvent>,
    packet_size: usize,
) -> Vec<u8> {
    rendered_events_framed(events, packet_size, false).await
}

async fn rendered_events_framed(
    events: mpsc::UnboundedReceiver<BatchEvent>,
    packet_size: usize,
    rpc: bool,
) -> Vec<u8> {
    let mut wire = Duplex {
        read: std::io::Cursor::new(Vec::new()),
        written: Vec::new(),
    };
    let kept = stream_reply(
        &mut wire,
        ReplySource::Single {
            events: Some(events),
            rpc,
        },
        Arc::new(AtomicBool::new(false)),
        packet_size,
    )
    .await
    .expect("stream");
    assert!(kept, "the client did not disconnect");
    let mut cursor = std::io::Cursor::new(wire.written);
    read_message(&mut cursor).await.expect("message").payload
}

/// The events a worker's `send_outcome` would produce for `outcome`, in a
/// closed channel ready to drain.
fn events_for(outcome: &BatchOutcome, in_xact: bool) -> mpsc::UnboundedReceiver<BatchEvent> {
    let (tx, rx) = mpsc::unbounded_channel();
    for result in &outcome.results {
        match result {
            StatementResult::Rows(rowset) => {
                tx.send(BatchEvent::Columns(rowset.columns.clone()))
                    .unwrap();
                // Chunked exactly as the sink chunks, so a boundary landing
                // mid-result is exercised here too.
                for chunk in rowset.rows.chunks(256) {
                    tx.send(BatchEvent::Rows(chunk.to_vec())).unwrap();
                }
                tx.send(BatchEvent::StatementDone {
                    count: Some(rowset.rows.len() as u64),
                    in_transaction: in_xact,
                    command: truthdb_core::engine::DoneCommand::Select,
                })
                .unwrap();
            }
            StatementResult::RowsAffected(n) => tx
                .send(BatchEvent::StatementDone {
                    count: Some(*n),
                    in_transaction: in_xact,
                    command: truthdb_core::engine::DoneCommand::Other,
                })
                .unwrap(),
            StatementResult::Done => tx
                .send(BatchEvent::StatementDone {
                    count: None,
                    in_transaction: in_xact,
                    command: truthdb_core::engine::DoneCommand::Other,
                })
                .unwrap(),
        }
    }
    if let Some(error) = &outcome.error {
        tx.send(BatchEvent::Error(error.clone())).unwrap();
    }
    tx.send(BatchEvent::Complete {
        in_transaction: in_xact,
    })
    .unwrap();
    rx
}

/// An RPC response frames as SQL Server does: per-statement DONEINPROC
/// (DONE_MORE kept on the last), then RETURNSTATUS, then RETURNVALUE (the
/// prepared handle, when there is one), then a final DONEPROC. A SQL
/// batch's plain-DONE framing is untouched — the oracle pins those bytes.
#[tokio::test]
async fn a_prepared_handle_renders_as_a_returnvalue_token() {
    let (tx, rx) = mpsc::unbounded_channel();
    tx.send(BatchEvent::StatementDone {
        count: Some(1),
        in_transaction: false,
        command: truthdb_core::engine::DoneCommand::Other,
    })
    .unwrap();
    tx.send(BatchEvent::PreparedHandle(7)).unwrap();
    tx.send(BatchEvent::Complete {
        in_transaction: false,
    })
    .unwrap();
    drop(tx);

    let mut expected = Vec::new();
    token::done_in_proc(&mut expected, true, false, false, Some(1), 0);
    token::return_status(&mut expected, 0);
    token::return_value_int(&mut expected, "handle", 7);
    token::done_proc(&mut expected, false, false, false, None);
    assert_eq!(rendered_events_framed(rx, 4096, true).await, expected);

    // Without a handle (sp_executesql, sp_execute): DONEINPROC,
    // RETURNSTATUS, DONEPROC.
    let (tx, rx) = mpsc::unbounded_channel();
    tx.send(BatchEvent::StatementDone {
        count: Some(3),
        in_transaction: false,
        command: truthdb_core::engine::DoneCommand::Other,
    })
    .unwrap();
    tx.send(BatchEvent::Complete {
        in_transaction: false,
    })
    .unwrap();
    drop(tx);
    let mut expected = Vec::new();
    token::done_in_proc(&mut expected, true, false, false, Some(3), 0);
    token::return_status(&mut expected, 0);
    token::done_proc(&mut expected, false, false, false, None);
    assert_eq!(rendered_events_framed(rx, 4096, true).await, expected);

    // A failed RPC returns no status: ERROR, DONEINPROC for what ran,
    // then DONEPROC with DONE_ERROR.
    let (tx, rx) = mpsc::unbounded_channel();
    tx.send(BatchEvent::Error(truthdb_sql::error::SqlError::new(
        8179,
        16,
        1,
        "Could not find prepared statement with handle 42.",
    )))
    .unwrap();
    tx.send(BatchEvent::Complete {
        in_transaction: false,
    })
    .unwrap();
    drop(tx);
    let mut expected = Vec::new();
    token::error(
        &mut expected,
        8179,
        1,
        16,
        "Could not find prepared statement with handle 42.",
    );
    token::done_proc(&mut expected, false, true, false, None);
    assert_eq!(rendered_events_framed(rx, 4096, true).await, expected);

    // The token's own bytes, pinned: 0xAC, ordinal 0, B_VARCHAR name,
    // status 0x01 (output param), UserType 0, Flags 0, INTN(4), value.
    let mut token_bytes = Vec::new();
    token::return_value_int(&mut token_bytes, "h", 7);
    assert_eq!(
        token_bytes,
        [
            0xac, 0x00, 0x00, // token, ParamOrdinal
            0x01, b'h' as u8, 0x00, // B_VARCHAR "h"
            0x01, // Status: output parameter
            0x00, 0x00, 0x00, 0x00, // UserType
            0x00, 0x00, // Flags
            0x26, 0x04, // TYPE_INFO: INTN, max 4
            0x04, 0x07, 0x00, 0x00, 0x00, // 4-byte value 7
        ]
    );
}

/// Each DONE carries the transaction state of *its own* statement on the
/// wire — `BEGIN TRAN; SELECT ...; COMMIT` reads INXACT 1, 1, 0 — instead
/// of the batch's final state stamped on all of them retroactively.
#[tokio::test]
async fn done_inxact_is_per_statement() {
    let (tx, rx) = mpsc::unbounded_channel();
    for (count, in_transaction) in [(None, true), (Some(1), true), (None, false)] {
        tx.send(BatchEvent::StatementDone {
            count,
            in_transaction,
            command: truthdb_core::engine::DoneCommand::Other,
        })
        .unwrap();
    }
    tx.send(BatchEvent::Complete {
        in_transaction: false,
    })
    .unwrap();
    drop(tx);

    let mut expected = Vec::new();
    token::done(&mut expected, true, false, true, None, 0);
    token::done(&mut expected, true, false, true, Some(1), 0);
    token::done(&mut expected, false, false, false, None, 0);
    assert_eq!(rendered_events(rx, 4096).await, expected);
}

/// A statement that fails after its result set began streaming closes the
/// set with a CLEAN done — never `DONE_ERROR` without an ERROR token,
/// which pytds and go-mssqldb both turn into a synthesized "request
/// failed" error that strands every result set behind it — and the stream
/// stays framed for what follows, here a CATCH block's own result set.
#[tokio::test]
async fn an_aborted_statement_closes_its_rowset_with_a_clean_done() {
    let (tx, rx) = mpsc::unbounded_channel();
    tx.send(BatchEvent::Columns(columns())).unwrap();
    tx.send(BatchEvent::Rows(vec![vec![Datum::Int(1)]]))
        .unwrap();
    tx.send(BatchEvent::StatementAborted {
        in_transaction: false,
    })
    .unwrap();
    tx.send(BatchEvent::Columns(columns())).unwrap();
    tx.send(BatchEvent::Rows(vec![vec![Datum::Int(9)]]))
        .unwrap();
    tx.send(BatchEvent::StatementDone {
        count: Some(1),
        in_transaction: false,
        command: truthdb_core::engine::DoneCommand::Other,
    })
    .unwrap();
    tx.send(BatchEvent::Complete {
        in_transaction: false,
    })
    .unwrap();
    drop(tx);

    let mut expected = Vec::new();
    token::colmetadata(&mut expected, &columns());
    token::row(&mut expected, &[Datum::Int(1)], &columns());
    token::done(&mut expected, true, false, false, None, 0);
    token::colmetadata(&mut expected, &columns());
    token::row(&mut expected, &[Datum::Int(9)], &columns());
    token::done(&mut expected, false, false, false, Some(1), 0);
    assert_eq!(rendered_events(rx, 4096).await, expected);
}

/// An abort as the batch's LAST statement event (an empty CATCH at the end
/// of the batch): its pending DONE becomes the batch-final DONE and must
/// be clean — the batch succeeded, its one error was caught. The buffered
/// path sent a single clean final DONE for this batch; a final
/// `DONE_ERROR` with no ERROR token would read as a failed batch.
#[tokio::test]
async fn an_abort_ending_the_batch_leaves_the_final_done_clean() {
    let (tx, rx) = mpsc::unbounded_channel();
    tx.send(BatchEvent::Columns(columns())).unwrap();
    tx.send(BatchEvent::Rows(vec![vec![Datum::Int(1)]]))
        .unwrap();
    tx.send(BatchEvent::StatementAborted {
        in_transaction: false,
    })
    .unwrap();
    tx.send(BatchEvent::Complete {
        in_transaction: false,
    })
    .unwrap();
    drop(tx);

    let mut expected = Vec::new();
    token::colmetadata(&mut expected, &columns());
    token::row(&mut expected, &[Datum::Int(1)], &columns());
    token::done(&mut expected, false, false, false, None, 0);
    assert_eq!(rendered_events(rx, 4096).await, expected);
}

/// A write half that fails on the first socket write, read half pending
/// forever — a client that died while a result was streaming to it.
struct FailingWrite;

impl AsyncRead for FailingWrite {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Pending
    }
}

impl AsyncWrite for FailingWrite {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::task::Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// A socket write failure mid-stream must cancel the batch and wait for
/// it to end before returning — the caller closes the session the moment
/// `stream_reply` returns, which releases the batch's locks, so returning
/// while the batch still runs would let another session read its
/// uncommitted writes. `stream_reply` returning at all proves the drain
/// ran (it waits for the channel to close), and the sender task below
/// only closes the channel once it observes the cancel flag.
#[tokio::test]
async fn a_write_error_mid_stream_cancels_and_drains_the_batch() {
    let (tx, rx) = mpsc::unbounded_channel();
    // Enough rows that rendering must write a packet to the (dead) socket.
    tx.send(BatchEvent::Columns(columns())).unwrap();
    tx.send(BatchEvent::Rows(
        (0..200).map(|i| vec![Datum::Int(i)]).collect(),
    ))
    .unwrap();

    let cancel = Arc::new(AtomicBool::new(false));
    let observed = cancel.clone();
    // The "worker": keeps the batch open until it sees the cancel, then
    // ends it — as the real executor's check_cancelled poll does.
    let worker = tokio::spawn(async move {
        while !observed.load(Ordering::Relaxed) {
            tokio::task::yield_now().await;
        }
        let _ = tx.send(BatchEvent::Complete {
            in_transaction: false,
        });
        drop(tx);
    });

    let mut wire = FailingWrite;
    let result = stream_reply(
        &mut wire,
        ReplySource::Single {
            events: Some(rx),
            rpc: false,
        },
        cancel.clone(),
        MIN_PACKET_SIZE,
    )
    .await;
    assert!(result.is_err(), "the write error surfaces");
    assert!(
        cancel.load(Ordering::Relaxed),
        "the running batch was cancelled before stream_reply returned"
    );
    worker.await.expect("worker");
}

/// A stream whose read half never yields (no Attention ever arrives) and
/// whose write half collects the bytes. `stream_reply` splits it, so it
/// needs both halves on one object.
struct Duplex {
    read: std::io::Cursor<Vec<u8>>,
    written: Vec<u8>,
}

impl AsyncRead for Duplex {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        // Never ready: a client that sends nothing during its batch. Not
        // `Ok(())` with zero bytes, which `stream_reply` reads as a
        // disconnect.
        std::task::Poll::Pending
    }
}

impl AsyncWrite for Duplex {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        self.written.extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

fn columns() -> Vec<ResultColumn> {
    vec![ResultColumn {
        name: "id".into(),
        column_type: ColumnType::Int,
    }]
}

fn rowset(n: i32) -> RowSet {
    RowSet {
        columns: columns(),
        rows: (0..n).map(|i| vec![Datum::Int(i)]).collect(),
    }
}

fn err() -> SqlError {
    SqlError::new(2627, 14, 1, "Violation of PRIMARY KEY constraint.")
}

fn outcome(results: Vec<StatementResult>, error: Option<SqlError>) -> BatchOutcome {
    BatchOutcome { results, error }
}

#[tokio::test]
async fn every_batch_shape_reaches_the_wire_as_the_buffered_path_did() {
    let cases: Vec<(&str, BatchOutcome)> = vec![
        // A batch with no statements at all: one final DONE.
        ("empty batch", outcome(Vec::new(), None)),
        // A rowset, including the zero-row case whose DONE still carries 0,
        // and one big enough to cross ROW_FLUSH_BYTES mid-result.
        (
            "zero-row rowset",
            outcome(vec![StatementResult::Rows(rowset(0))], None),
        ),
        (
            "small rowset",
            outcome(vec![StatementResult::Rows(rowset(3))], None),
        ),
        (
            "large rowset",
            outcome(vec![StatementResult::Rows(rowset(5000))], None),
        ),
        // A row count (DML) and a bare DONE (DDL).
        (
            "rows affected",
            outcome(vec![StatementResult::RowsAffected(5)], None),
        ),
        ("ddl", outcome(vec![StatementResult::Done], None)),
        // Several statements: every DONE but the last says MORE.
        (
            "three statements",
            outcome(
                vec![
                    StatementResult::Rows(rowset(2)),
                    StatementResult::Rows(rowset(1)),
                    StatementResult::RowsAffected(9),
                ],
                None,
            ),
        ),
        // With an error the last statement's DONE says MORE too, because
        // the error's DONE is the final one.
        (
            "results then error",
            outcome(vec![StatementResult::Rows(rowset(2))], Some(err())),
        ),
        ("error only", outcome(Vec::new(), Some(err()))),
    ];
    for (case, batch) in &cases {
        for in_xact in [false, true] {
            // Small packets so a large result is split many times, and the
            // default so the common path is covered too.
            for packet_size in [MIN_PACKET_SIZE, DEFAULT_PACKET_SIZE] {
                assert_eq!(
                    rendered(batch, in_xact, packet_size).await,
                    build_batch_tokens(batch, in_xact),
                    "{case} (in_transaction={in_xact}, packet_size={packet_size})"
                );
            }
        }
    }
}

#[tokio::test]
async fn a_large_result_is_split_into_many_packets_but_one_message() {
    // The point of the writer: the bytes leave as they are rendered rather
    // than accumulating into one buffer. A 5000-row result at the minimum
    // packet size is hundreds of packets and exactly one EOM.
    let batch = outcome(vec![StatementResult::Rows(rowset(5000))], None);
    let events = events_for(&batch, false);
    let mut duplex = Duplex {
        read: std::io::Cursor::new(Vec::new()),
        written: Vec::new(),
    };
    stream_reply(
        &mut duplex,
        ReplySource::Single {
            events: Some(events),
            rpc: false,
        },
        Arc::new(AtomicBool::new(false)),
        MIN_PACKET_SIZE,
    )
    .await
    .expect("stream");
    let wire = duplex.written;
    let mut packets = 0;
    let mut eom = 0;
    let mut offset = 0;
    while offset < wire.len() {
        let length = u16::from_be_bytes([wire[offset + 2], wire[offset + 3]]) as usize;
        if wire[offset + 1] & 0x01 != 0 {
            eom += 1;
        }
        packets += 1;
        offset += length;
    }
    // 5000 rows of one INT is ~25 KB of ROW tokens, so ~50 packets of 504
    // payload bytes. The bound is deliberately loose — the claim is "many
    // packets, still one message", not an exact encoding size.
    assert!(
        packets > 30,
        "a big result spans many packets, got {packets}"
    );
    assert_eq!(eom, 1, "exactly one packet ends the message");
    assert_eq!(offset, wire.len(), "packet lengths tile the stream exactly");
}
