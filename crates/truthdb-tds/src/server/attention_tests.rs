use super::*;
use tokio::io::AsyncWriteExt;
use truthdb_core::engine::ResultColumn;
use truthdb_core::relstore::types::{ColumnType, Datum};

/// An Attention header-only packet.
const ATTN: [u8; 8] = [PKT_ATTENTION, 0x01, 0x00, 0x08, 0, 0, 0, 0];

fn columns() -> Vec<ResultColumn> {
    vec![ResultColumn {
        name: "id".to_string(),
        column_type: ColumnType::Int,
    }]
}

/// A batch that produces nothing until its cancel flag is set — the
/// executor polling `check_cancelled` before its first row. Emitting
/// nothing up front is what makes the test deterministic: `select!` picks
/// randomly among ready branches, so a batch with rows waiting would
/// sometimes render them before the Attention and sometimes not.
fn cancellable_batch(cancel: Arc<AtomicBool>) -> mpsc::UnboundedReceiver<BatchEvent> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while !cancel.load(Ordering::Relaxed) {
            tokio::task::yield_now().await;
        }
        // A cancelled batch reports the executor's internal cancel error,
        // which the renderer must not put on the wire.
        tx.send(BatchEvent::Error(
            truthdb_sql::error::SqlError::message_only(3617, "The query was canceled."),
        ))
        .ok();
        tx.send(BatchEvent::Complete {
            in_transaction: false,
        })
        .ok();
    });
    rx
}

#[tokio::test]
async fn attention_during_batch_cancels_and_acks() {
    let (mut client, server) = tokio::io::duplex(4096);
    let mut server = server;
    let cancel = Arc::new(AtomicBool::new(false));
    let events = cancellable_batch(cancel.clone());
    // The client sends an Attention (header-only packet) during the batch.
    client.write_all(&ATTN).await.expect("send attention");
    let kept = stream_reply(
        &mut server,
        ReplySource::Single {
            events: Some(events),
            rpc: false,
        },
        cancel.clone(),
        4096,
    )
    .await
    .expect("io ok");
    assert!(kept, "the client is still connected");
    assert!(
        cancel.load(Ordering::Relaxed),
        "the Attention set the cancel flag"
    );

    let mut cursor = std::io::Cursor::new(read_client_bytes(&mut client).await);
    let payload = crate::packet::read_message(&mut cursor)
        .await
        .expect("message")
        .payload;
    // DONE(attention) and nothing else. The executor's internal 3617 is
    // never rendered — the buffered path did not show it, and a client must
    // not see it either — even though the batch reported it.
    let mut expected = Vec::new();
    token::done_attention(&mut expected);
    assert_eq!(payload, expected);
}

#[tokio::test]
async fn no_attention_renders_the_batch_normally() {
    let (mut client, server) = tokio::io::duplex(4096);
    let mut server = server;
    let cancel = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::unbounded_channel();
    tx.send(BatchEvent::Columns(columns())).unwrap();
    tx.send(BatchEvent::Rows(vec![vec![Datum::Int(1)]]))
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

    let kept = stream_reply(
        &mut server,
        ReplySource::Single {
            events: Some(rx),
            rpc: false,
        },
        cancel.clone(),
        4096,
    )
    .await
    .expect("io ok");
    assert!(kept);
    assert!(!cancel.load(Ordering::Relaxed));
    let mut cursor = std::io::Cursor::new(read_client_bytes(&mut client).await);
    let payload = crate::packet::read_message(&mut cursor)
        .await
        .expect("message")
        .payload;
    // One final DONE carrying the row count, and no attention bit.
    let mut expected = Vec::new();
    token::colmetadata(&mut expected, &columns());
    token::row(&mut expected, &[Datum::Int(1)], &columns());
    token::done(&mut expected, false, false, false, Some(1), 0);
    assert_eq!(payload, expected);
}

#[tokio::test]
async fn a_reply_that_ends_without_a_terminal_event_is_a_clean_error() {
    // The worker panicked, or the pool dropped the call at shutdown. Falling
    // through here would emit a message with NO DONE at all, leaving the
    // client waiting on a result that never terminates. The buffered path
    // turned a dead reply channel into a clean 50000.
    let (mut client, server) = tokio::io::duplex(4096);
    let mut server = server;
    let (tx, rx) = mpsc::unbounded_channel();
    tx.send(BatchEvent::Columns(columns())).unwrap();
    tx.send(BatchEvent::Rows(vec![vec![Datum::Int(1)]]))
        .unwrap();
    // A DONE is still deferred here — it must not be lost either.
    tx.send(BatchEvent::StatementDone {
        count: Some(1),
        in_transaction: false,
        command: truthdb_core::engine::DoneCommand::Other,
    })
    .unwrap();
    drop(tx);

    let kept = stream_reply(
        &mut server,
        ReplySource::Single {
            events: Some(rx),
            rpc: false,
        },
        Arc::new(AtomicBool::new(false)),
        4096,
    )
    .await
    .expect("io ok");
    assert!(kept);
    let mut cursor = std::io::Cursor::new(read_client_bytes(&mut client).await);
    let payload = crate::packet::read_message(&mut cursor)
        .await
        .expect("message")
        .payload;
    // The deferred DONE is flushed with DONE_MORE (something does follow
    // it), then the error and its final DONE_ERROR. Without the fix this
    // message has no DONE at all.
    let mut expected = Vec::new();
    token::colmetadata(&mut expected, &columns());
    token::row(&mut expected, &[Datum::Int(1)], &columns());
    token::done(&mut expected, true, false, false, Some(1), 0);
    token::error(
        &mut expected,
        50000,
        1,
        16,
        &EngineError::Unavailable.to_string(),
    );
    token::done(&mut expected, false, true, false, None, 0);
    assert_eq!(payload, expected);
}

#[tokio::test]
async fn an_early_exit_waits_for_the_batch_it_cancelled_to_finish() {
    // `serve_connection` closes the session the moment this returns, and
    // `close_session` releases *the session's* locks. A batch that is still
    // running still holds them and may still be writing, so returning early
    // hands its locks to the next session mid-statement — a dirty read the
    // engine has no defence against, since nothing can abort a running
    // batch. The buffered path awaited the batch here for this reason.
    let (client, server) = tokio::io::duplex(4096);
    let mut server = server;
    drop(client);
    let cancel = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));

    let (tx, rx) = mpsc::unbounded_channel();
    let batch_cancel = cancel.clone();
    let batch_finished = finished.clone();
    tokio::spawn(async move {
        // The batch notices its cancel flag, then still has work to do
        // before it returns and its locks are released.
        while !batch_cancel.load(Ordering::Relaxed) {
            tokio::task::yield_now().await;
        }
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        batch_finished.store(true, Ordering::Relaxed);
        tx.send(BatchEvent::Complete {
            in_transaction: false,
        })
        .ok();
        // `tx` drops here: the sink dying is what says the batch is over.
    });

    let kept = stream_reply(
        &mut server,
        ReplySource::Single {
            events: Some(rx),
            rpc: false,
        },
        cancel.clone(),
        4096,
    )
    .await
    .expect("io ok");
    assert!(!kept, "the client disconnected");
    assert!(
        cancel.load(Ordering::Relaxed),
        "a vanished client cancels the batch it left running"
    );
    assert!(
        finished.load(Ordering::Relaxed),
        "returned while the batch was still running: the caller would now \
             release its locks out from under it"
    );
}

#[tokio::test]
async fn a_disconnect_mid_batch_is_reported_as_such() {
    let (client, server) = tokio::io::duplex(4096);
    let mut server = server;
    drop(client);
    let cancel = Arc::new(AtomicBool::new(false));
    let events = cancellable_batch(cancel.clone());
    let kept = stream_reply(
        &mut server,
        ReplySource::Single {
            events: Some(events),
            rpc: false,
        },
        cancel.clone(),
        4096,
    )
    .await
    .expect("io ok");
    assert!(!kept, "the client disconnected");
    assert!(
        cancel.load(Ordering::Relaxed),
        "a vanished client still cancels the batch it left running"
    );
}

/// Everything the server has written so far.
async fn read_client_bytes(client: &mut tokio::io::DuplexStream) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    // The server half is still open, so read what is buffered and stop.
    loop {
        let mut chunk = [0u8; 4096];
        match tokio::time::timeout(
            std::time::Duration::from_millis(50),
            client.read(&mut chunk),
        )
        .await
        {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            Ok(Err(_)) => break,
        }
    }
    buf
}
