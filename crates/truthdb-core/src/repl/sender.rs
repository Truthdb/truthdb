//! The primary's per-standby log sender: after [`serve_handshake`] accepts a
//! standby, this task registers the standby's replication slot (holding WAL
//! truncation at its resume point) and streams `Storage::read_wal_range` output
//! to it as `LogData` frames — first the catch-up backlog, then live, waking on
//! the group-commit flushed watch as new WAL becomes durable. A concurrent
//! reader half consumes the standby's `FlushAck`s and advances the slot, so the
//! primary reclaims log the standby has made durable. The slot survives a
//! disconnect by design: it keeps holding log for the standby's reconnect, and
//! is reclaimed only by the checkpoint retention reap.
//!
//! [`serve_handshake`]: super::server::serve_handshake

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::framing::{read_repl_frame, write_repl_frame};
use super::server::AcceptedStandby;
use super::{
    FlushAck, Heartbeat, HelloAck, LogData, REPL_PROTOCOL_VERSION, ReplFrame, ReplMsgType,
};
use crate::storage::Storage;

/// Bytes of WAL per `LogData` frame while catching a standby up (well under
/// `REPL_MAX_PAYLOAD`, and each chunk is read under one storage-lock hold).
const SENDER_CHUNK: u64 = 4 * 1024 * 1024;

/// Streams durable WAL to one accepted standby until the connection drops, the
/// storage shuts down, or `shutdown` flips. Errors are connection-fatal only —
/// the standby reconnects and resumes from its persisted watermark.
pub(crate) async fn run_sender<S>(
    stream: S,
    storage: Arc<Storage>,
    standby: AcceptedStandby,
    heartbeat: Duration,
    shutdown: watch::Receiver<bool>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    // Slot ids are u32 (the persisted slot table); the wire's node_id is u64.
    // Configured node ids are u32, so an overflow is a foreign client.
    let mut stream = stream;
    let slot_id = match u32::try_from(standby.node_id) {
        Ok(id) => id,
        Err(_) => {
            let msg = format!(
                "node_id {} exceeds the slot-id range (u32); configure a smaller node id",
                standby.node_id
            );
            return refuse(&mut stream, &storage, &msg).await;
        }
    };
    let sent = standby.last_received_lsn;
    let flushed = storage.wal_flushed_lsn();
    if sent > flushed {
        // The standby has log this primary never wrote (or lost): a diverged
        // timeline. Shipping from `sent` would fabricate continuity.
        let msg = format!(
            "standby received LSN {sent} is ahead of the primary's durable WAL ({flushed}): \
             diverged timelines — reseed the standby from a fresh backup"
        );
        return refuse(&mut stream, &storage, &msg).await;
    }
    // Register the slot before the first read: the registration is fenced
    // against the WAL head under the storage lock, so from here the log at
    // `sent` cannot be truncated out from under the stream.
    if let Err(err) = storage.try_register_repl_slot(slot_id, sent) {
        return refuse(&mut stream, &storage, &err.to_string()).await;
    }

    let (rd, mut wr) = tokio::io::split(stream);
    let mut acks = spawn_ack_reader(rd, Arc::clone(&storage), slot_id);
    let result = ship_loop(&mut wr, &storage, sent, heartbeat, shutdown, &mut acks).await;
    acks.abort();
    result
}

/// Reads the standby's frames (`FlushAck` advances the slot; anything else is
/// ignored) until the connection drops.
fn spawn_ack_reader<R>(mut rd: R, storage: Arc<Storage>, slot_id: u32) -> JoinHandle<io::Result<()>>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        loop {
            let frame = read_repl_frame(&mut rd).await?;
            if frame.msg_type == ReplMsgType::FlushAck {
                let ack: FlushAck = frame.decode().map_err(io::Error::other)?;
                // Advance at the standby's *durable* watermark: log below it is
                // safe to reclaim even if the standby crashes right now.
                storage.advance_repl_slot(slot_id, ack.flushed_lsn);
            }
        }
    })
}

async fn ship_loop<W>(
    wr: &mut W,
    storage: &Arc<Storage>,
    mut sent: u64,
    heartbeat: Duration,
    mut shutdown: watch::Receiver<bool>,
    acks: &mut JoinHandle<io::Result<()>>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut flushed_watch = storage.subscribe_wal_flushed();
    let mut tick = tokio::time::interval(heartbeat);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        // Ship everything durable, then wait for more. The watch value is a
        // hint only — the watermark is re-read every pass (a direct WAL sync
        // advances it without signalling the watch; the tick covers that).
        let flushed = storage.wal_flushed_lsn();
        while sent < flushed {
            let end = flushed.min(sent + SENDER_CHUNK);
            let bytes = storage
                .read_wal_range(sent, end)
                .map_err(io::Error::other)?;
            let frame = ReplFrame::encode(
                ReplMsgType::LogData,
                &LogData {
                    from_lsn: sent,
                    bytes,
                },
            )
            .map_err(io::Error::other)?;
            // A stalled standby (full TCP send buffer) must not be able to
            // hang server shutdown: abandon the write when shutdown flips (the
            // connection is being torn down anyway).
            tokio::select! {
                written = write_repl_frame(wr, &frame) => written?,
                _ = shutdown.changed() => return Ok(()),
            }
            sent = end;
        }
        tokio::select! {
            res = &mut *acks => {
                // The standby hung up (or its stream failed): connection over.
                return match res {
                    Ok(inner) => inner,
                    Err(join) => Err(io::Error::other(join)),
                };
            }
            changed = flushed_watch.changed() => {
                if changed.is_err() {
                    // Storage (and its log-writer) shut down.
                    return Ok(());
                }
            }
            _ = tick.tick() => {
                if storage.wal_flushed_lsn() <= sent {
                    let hb = Heartbeat { time_ms: now_ms() };
                    let frame = ReplFrame::encode(ReplMsgType::Heartbeat, &hb)
                        .map_err(io::Error::other)?;
                    tokio::select! {
                        written = write_repl_frame(wr, &frame) => written?,
                        _ = shutdown.changed() => return Ok(()),
                    }
                }
            }
            _ = shutdown.changed() => return Ok(()),
        }
    }
}

/// Sends a post-handshake rejection notice — a `HelloAck { accepted: false }`
/// status frame naming the reason (the standby surfaces it to its operator,
/// e.g. "reseed from a fresh backup") — then fails the connection.
async fn refuse<S>(stream: &mut S, storage: &Storage, msg: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let notice = HelloAck {
        protocol_version: REPL_PROTOCOL_VERSION,
        accepted: false,
        primary_epoch: storage.epoch(),
        primary_flushed_lsn: storage.wal_flushed_lsn(),
        message: msg.to_string(),
    };
    let frame = ReplFrame::encode(ReplMsgType::HelloAck, &notice).map_err(io::Error::other)?;
    write_repl_frame(stream, &frame).await?;
    stream.flush().await?;
    Err(io::Error::new(io::ErrorKind::InvalidData, msg.to_string()))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageOptions;

    fn temp_storage(label: &str) -> (Arc<Storage>, std::path::PathBuf) {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "truthdb-repl-sender-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let storage = Storage::create(
            path.clone(),
            StorageOptions {
                size_gib: 1,
                wal_ratio: 0.05,
                metadata_ratio: 0.08,
                snapshot_ratio: 0.02,
                allocator_ratio: 0.02,
                reserved_ratio: 0.17,
                default_collation: None,
            },
        )
        .unwrap();
        (Arc::new(storage), path)
    }

    async fn refusal_message(standby: AcceptedStandby) -> (io::Result<()>, HelloAck) {
        let (server_end, mut client_end) = tokio::io::duplex(64 * 1024);
        let (storage, path) = temp_storage("refuse");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let sender = tokio::spawn(run_sender(
            server_end,
            storage,
            standby,
            Duration::from_secs(30),
            shutdown_rx,
        ));
        let notice: HelloAck = read_repl_frame(&mut client_end)
            .await
            .unwrap()
            .decode()
            .unwrap();
        let result = sender.await.unwrap();
        let _ = std::fs::remove_file(path);
        (result, notice)
    }

    // A standby claiming log the primary never durably wrote is a diverged
    // timeline: the sender refuses with a notice instead of fabricating
    // continuity.
    #[tokio::test]
    async fn an_ahead_standby_is_refused_as_diverged() {
        let (result, notice) = refusal_message(AcceptedStandby {
            node_id: 3,
            last_received_lsn: u64::MAX / 2,
        })
        .await;
        assert!(result.is_err());
        assert!(!notice.accepted);
        assert!(
            notice.message.contains("reseed"),
            "the notice names the fix: {}",
            notice.message
        );
    }

    // Slot ids are the persisted u32 table's; a node id that cannot be one is
    // refused up front rather than truncated into a colliding slot.
    #[tokio::test]
    async fn an_oversized_node_id_is_refused() {
        let (result, notice) = refusal_message(AcceptedStandby {
            node_id: u64::from(u32::MAX) + 1,
            last_received_lsn: 0,
        })
        .await;
        assert!(result.is_err());
        assert!(!notice.accepted);
        assert!(
            notice.message.contains("node id"),
            "the notice names the cause: {}",
            notice.message
        );
    }
}
