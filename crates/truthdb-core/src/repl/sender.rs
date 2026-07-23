//! The primary's per-standby log sender: after [`serve_handshake`] accepts a
//! standby, this task registers the standby's replication slot (holding WAL
//! truncation at its resume point) and streams the log to it as `LogData`
//! frames — first the catch-up backlog, then live, waking on the group-commit
//! flushed watch as new WAL becomes durable. Chunks are cut on WAL-ENTRY
//! BOUNDARIES ([`Storage::read_wal_chunk`]): the standby persists each range's
//! end as its applied tail, and a mid-entry end would make every later decode
//! silently no-op — permanent silent divergence. The same primitive fences the
//! read against the WAL head under one lock hold, so a slot reaped by the
//! retention cap turns the NEXT read into a loud reseed error instead of
//! shipping recycled ring bytes from a newer lap.
//!
//! A concurrent reader half consumes the standby's `FlushAck`s and advances
//! the slot — but only ever to a boundary this sender actually shipped, so a
//! malicious or corrupt ack cannot park the slot (and therefore the checkpoint
//! truncation floor, which becomes the WAL head the next restart scans from)
//! in the middle of an entry. The slot survives a disconnect by design: it
//! keeps holding log for the standby's reconnect, and is reclaimed only by the
//! checkpoint retention reap.
//!
//! [`serve_handshake`]: super::server::serve_handshake

use std::collections::BTreeSet;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::framing::{read_repl_frame, write_repl_frame};
use super::listener::PrimaryReplContext;
use super::server::AcceptedStandby;
use super::{
    FlushAck, Heartbeat, HelloAck, LogData, REPL_MAX_PAYLOAD, REPL_PROTOCOL_VERSION, ReplFrame,
    ReplMsgType,
};
use crate::storage::Storage;

/// Default bytes of WAL per `LogData` frame while catching a standby up (well
/// under `REPL_MAX_PAYLOAD`; a single oversized WAL entry may exceed it, the
/// wire cap below is the hard limit).
pub const DEFAULT_CHUNK_BYTES: u64 = 4 * 1024 * 1024;

/// Streams durable WAL to one accepted standby until the connection drops, the
/// storage shuts down, or `shutdown` flips. Errors are connection-fatal only —
/// the standby reconnects and resumes from its persisted watermark.
pub(crate) async fn run_sender<S>(
    stream: S,
    ctx: PrimaryReplContext,
    standby: AcceptedStandby,
    shutdown: watch::Receiver<bool>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    // Slot ids are u32 (the persisted slot table); the wire's node_id is u64.
    // Configured node ids are u32, so an overflow is a foreign client.
    let mut stream = stream;
    let storage = Arc::clone(&ctx.storage);
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
    // One connection per node id: a second stream under the same id would
    // reset the first's slot and its acks would advance it out from under the
    // other stream's shipping position (the shipped default node_id is the
    // same on every node, so this is an easy misconfiguration to hit).
    let _membership = match NodeMembership::acquire(&ctx.active_nodes, slot_id) {
        Some(m) => m,
        None => {
            let msg = format!(
                "node id {slot_id} is already connected; every standby needs a distinct node_id"
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
    // `sent` cannot be truncated out from under the stream (unless the
    // retention-cap reap drops the slot — which the per-chunk head fence in
    // `read_wal_chunk` then reports as a reseed error).
    if let Err(err) = storage.try_register_repl_slot(slot_id, sent) {
        return refuse(&mut stream, &storage, &err.to_string()).await;
    }

    // Boundaries this sender has shipped (or started from): the only LSNs an
    // ack may advance the slot to.
    let shipped = Arc::new(Mutex::new(BTreeSet::from([sent])));
    let (rd, mut wr) = tokio::io::split(stream);
    let mut acks = spawn_ack_reader(
        rd,
        Arc::clone(&storage),
        slot_id,
        Arc::clone(&shipped),
        ctx.stall_timeout,
    );
    let result = ship_loop(&mut wr, &ctx, sent, shutdown, &mut acks, &shipped).await;
    acks.abort();
    result
}

/// RAII membership in the primary's connected-node set.
struct NodeMembership {
    nodes: Arc<Mutex<std::collections::HashSet<u32>>>,
    id: u32,
}

impl NodeMembership {
    fn acquire(nodes: &Arc<Mutex<std::collections::HashSet<u32>>>, id: u32) -> Option<Self> {
        let mut set = nodes.lock().expect("active-node set poisoned");
        if !set.insert(id) {
            return None;
        }
        Some(NodeMembership {
            nodes: Arc::clone(nodes),
            id,
        })
    }
}

impl Drop for NodeMembership {
    fn drop(&mut self) {
        self.nodes
            .lock()
            .expect("active-node set poisoned")
            .remove(&self.id);
    }
}

/// Reads the standby's frames until the connection drops or stays silent past
/// `stall` (a healthy standby acks every heartbeat, so silence means a dead or
/// half-open peer — the connection is torn down; the slot remains). A
/// `FlushAck` advances the slot, clamped to the greatest boundary this sender
/// shipped at or below the acked LSN.
fn spawn_ack_reader<R>(
    mut rd: R,
    storage: Arc<Storage>,
    slot_id: u32,
    shipped: Arc<Mutex<BTreeSet<u64>>>,
    stall: Duration,
) -> JoinHandle<io::Result<()>>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        loop {
            let frame = tokio::time::timeout(stall, read_repl_frame(&mut rd))
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("standby sent nothing for {stall:?}; dropping the connection"),
                    )
                })??;
            if frame.msg_type == ReplMsgType::FlushAck {
                let ack: FlushAck = frame.decode().map_err(io::Error::other)?;
                // Advance at the standby's *durable* watermark — but only to a
                // boundary we shipped: the slot LSN feeds the checkpoint
                // truncation floor (= the restart scan's start), which must
                // never sit mid-entry.
                let bounded = {
                    let mut set = shipped.lock().expect("shipped-boundary set poisoned");
                    let target = set.range(..=ack.flushed_lsn).next_back().copied();
                    if let Some(lsn) = target {
                        // Boundaries at or below the ack are consumed.
                        let keep = set.split_off(&lsn);
                        *set = keep;
                    }
                    target
                };
                if let Some(lsn) = bounded {
                    storage.advance_repl_slot(slot_id, lsn);
                }
            }
        }
    })
}

async fn ship_loop<W>(
    wr: &mut W,
    ctx: &PrimaryReplContext,
    mut sent: u64,
    mut shutdown: watch::Receiver<bool>,
    acks: &mut JoinHandle<io::Result<()>>,
    shipped: &Arc<Mutex<BTreeSet<u64>>>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let storage = &ctx.storage;
    let mut flushed_watch = storage.subscribe_wal_flushed();
    let mut tick = tokio::time::interval(ctx.heartbeat);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        // Ship everything durable, then wait for more. The watch value is a
        // hint only — the watermark is re-read every pass (a direct WAL sync
        // advances it without signalling the watch; the tick covers that).
        let flushed = storage.wal_flushed_lsn();
        while sent < flushed {
            let (bytes, end) = storage
                .read_wal_chunk(sent, flushed, ctx.chunk_bytes)
                .map_err(io::Error::other)?;
            if end == sent {
                // No complete entry fits below the cap (transient boundary
                // state); wait for the watermark to move past it.
                break;
            }
            if bytes.len() + 64 > REPL_MAX_PAYLOAD {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "WAL entry at LSN {sent} is larger than the replication frame cap \
                         ({REPL_MAX_PAYLOAD} bytes); cannot ship it"
                    ),
                ));
            }
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
            shipped
                .lock()
                .expect("shipped-boundary set poisoned")
                .insert(end);
            sent = end;
        }
        tokio::select! {
            res = &mut *acks => {
                // The standby hung up, went silent past the stall deadline, or
                // its stream failed: connection over.
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

    fn test_ctx(storage: Arc<Storage>) -> PrimaryReplContext {
        PrimaryReplContext {
            shared_secret: Arc::new(b"unused".to_vec()),
            cluster_uuid: [0u8; 16],
            storage,
            heartbeat: Duration::from_secs(30),
            stall_timeout: Duration::from_secs(30),
            chunk_bytes: DEFAULT_CHUNK_BYTES,
            active_nodes: Arc::default(),
        }
    }

    async fn refusal_message(standby: AcceptedStandby) -> (io::Result<()>, HelloAck) {
        let (server_end, mut client_end) = tokio::io::duplex(64 * 1024);
        let (storage, path) = temp_storage("refuse");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let sender = tokio::spawn(run_sender(
            server_end,
            test_ctx(storage),
            standby,
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

    // A second connection under an in-use node id is refused: it would reset
    // the live stream's slot and cross-release its truncation pin.
    #[tokio::test]
    async fn a_duplicate_node_id_is_refused() {
        let (storage, path) = temp_storage("dup-node");
        let ctx = test_ctx(storage);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // First connection holds node 5.
        let (server1, _client1) = tokio::io::duplex(64 * 1024);
        let ctx1 = ctx.clone();
        let first = tokio::spawn(run_sender(
            server1,
            ctx1,
            AcceptedStandby {
                node_id: 5,
                last_received_lsn: 0,
            },
            shutdown_rx.clone(),
        ));
        // Give the first sender time to register.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (server2, mut client2) = tokio::io::duplex(64 * 1024);
        let second = tokio::spawn(run_sender(
            server2,
            ctx.clone(),
            AcceptedStandby {
                node_id: 5,
                last_received_lsn: 0,
            },
            shutdown_rx,
        ));
        let notice: HelloAck = read_repl_frame(&mut client2)
            .await
            .unwrap()
            .decode()
            .unwrap();
        assert!(!notice.accepted);
        assert!(
            notice.message.contains("already connected"),
            "{}",
            notice.message
        );
        assert!(second.await.unwrap().is_err());

        first.abort();
        let _ = first.await;
        // The membership guard released the id on abort, so it is reusable.
        assert!(!ctx.active_nodes.lock().unwrap().contains(&5));
        let _ = std::fs::remove_file(path);
    }
}
