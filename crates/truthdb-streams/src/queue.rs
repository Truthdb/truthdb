//! Topics read as queues (the SQL queue's `sys.sp_s3_queue_*`), the way
//! Amazon SQS serves one: a receive hands out up to ten visible messages,
//! each hidden for its visibility timeout and named by a receipt handle new
//! with each receive; an acknowledged message is done; one not acknowledged
//! in time is visible again (delivery is at least once).
//!
//! A partition's frontier is the first offset never received: a message
//! before it is either in flight, with a lease (when it is visible again,
//! how often and since when it has been received) indexed by when it is
//! visible again, or done. So a receive takes the due leases from that
//! index, then fresh messages from the frontier, and an acknowledgement
//! removes one lease: each costs what it hands out or settles, however
//! many messages are in flight or done. A message that retention removed
//! while in flight is dropped when its lease comes due.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::StreamTxn;
use crate::store::{corrupt, partition_key, read_record, u64_at};

/// How long a received message stays hidden at most, from its first
/// receive (SQS's 12 hours).
pub const MAX_VISIBILITY_MS: u64 = 12 * 3_600_000;
/// Due leases of removed messages one receive drops, at most, besides those
/// it hands out.
const GARBAGE_PER_RECEIVE: usize = 64;

/// A received message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueMessage {
    pub partition: u32,
    pub offset: u64,
    /// What acknowledges it, or changes its visibility (this receive's).
    pub receipt: String,
    pub value: Vec<u8>,
    /// When it was appended.
    pub timestamp_ms: u64,
    pub receives: u32,
    pub first_receive_ms: u64,
}

/// Why a queue operation was refused (the storage itself worked).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueError {
    NoSuchTopic,
    /// Not a receipt handle of this queue (or of this receive).
    InvalidReceipt,
    /// The message is not hidden (done, or visible again).
    NotInFlight,
    /// Past 12 hours from its first receive.
    TooLong,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Lease {
    visible_ms: u64,
    receives: u32,
    first_ms: u64,
}

/// `[visible ms u64][receives u32][first receive ms u64]`.
fn encode_lease(lease: &Lease) -> Vec<u8> {
    [
        &lease.visible_ms.to_le_bytes()[..],
        &lease.receives.to_le_bytes(),
        &lease.first_ms.to_le_bytes(),
    ]
    .concat()
}

fn decode_lease(row: &[u8]) -> Result<Lease, StorageError> {
    if row.len() != 20 {
        return Err(corrupt("queue lease"));
    }
    Ok(Lease {
        visible_ms: u64_at(row, 0)?,
        receives: u32::from_le_bytes(row[8..12].try_into().unwrap()),
        first_ms: u64_at(row, 12)?,
    })
}

fn lease_key(topic: u32, partition: u32, offset: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&partition_key(topic, partition));
    key[8..].copy_from_slice(&offset.to_be_bytes());
    key
}

fn time_key(topic: u32, partition: u32, visible_ms: u64, offset: u64) -> [u8; 24] {
    let mut key = [0u8; 24];
    key[..8].copy_from_slice(&partition_key(topic, partition));
    key[8..16].copy_from_slice(&visible_ms.to_be_bytes());
    key[16..].copy_from_slice(&offset.to_be_bytes());
    key
}

/// What a receipt names: the topic id, partition, offset and receive count.
type Receipt = (u32, u32, u64, u32);

/// `topic-partition-offset-receives`.
fn receipt(topic: u32, partition: u32, offset: u64, receives: u32) -> String {
    format!("{topic}-{partition}-{offset}-{receives}")
}

fn parse_receipt(text: &str) -> Option<Receipt> {
    let mut fields = text.split('-');
    let parsed = (
        fields.next()?.parse().ok()?,
        fields.next()?.parse().ok()?,
        fields.next()?.parse().ok()?,
        fields.next()?.parse().ok()?,
    );
    fields.next().is_none().then_some(parsed)
}

impl StreamTxn<'_, '_> {
    /// Up to `max` visible messages of `topic`, each hidden until
    /// `now_ms + visibility_ms`: in each partition, those visible again
    /// first, then those never received, oldest first.
    pub fn queue_receive(
        &mut self,
        topic: &str,
        max: usize,
        visibility_ms: u64,
        now_ms: u64,
    ) -> Result<Result<Vec<QueueMessage>, QueueError>, StorageError> {
        let Some(topic) = self.topic_row(topic)? else {
            return Ok(Err(QueueError::NoSuchTopic));
        };
        let visible_ms = now_ms.saturating_add(visibility_ms);
        let mut out = Vec::new();
        // Starting from a partition the time picks, so a busy one does not
        // keep the others waiting.
        let first = (now_ms % u64::from(topic.partitions)) as u32;
        for partition in (first..topic.partitions).chain(0..first) {
            if out.len() == max {
                break;
            }
            let state = self.partition_row(topic.id, partition)?;
            // Visible again: the due leases, by when they became so.
            let due = self.trees.queue_times().scan_range_limit(
                self.ctx,
                Some(&time_key(topic.id, partition, 0, 0)),
                Some(&time_key(topic.id, partition, now_ms, u64::MAX)),
                max - out.len() + GARBAGE_PER_RECEIVE,
            )?;
            for (key, _) in due {
                if out.len() == max {
                    break;
                }
                let offset = be_u64(&key[16..])?;
                let was_visible = be_u64(&key[8..16])?;
                let lease_key = lease_key(topic.id, partition, offset);
                let mode = &mut OpMode::Txn(self.txn);
                self.trees.queue_times().delete(self.ctx, mode, &key)?;
                let lease = match self.trees.queue_leases().get(self.ctx, &lease_key)? {
                    Some(row) => decode_lease(&row)?,
                    None => return Err(corrupt("queue time without its lease")),
                };
                if lease.visible_ms != was_visible {
                    return Err(corrupt("queue time out of step with its lease"));
                }
                // Removed by retention while in flight.
                if offset < state.start {
                    self.trees
                        .queue_leases()
                        .delete(self.ctx, mode, &lease_key)?;
                    continue;
                }
                let lease = Lease {
                    visible_ms,
                    receives: lease.receives.saturating_add(1),
                    first_ms: lease.first_ms,
                };
                out.push(self.hand_out(topic.id, partition, offset, lease)?);
            }
            // Then those never received.
            let mut frontier = self.queue_frontier(topic.id, partition)?.max(state.start);
            let before = frontier;
            while out.len() < max && frontier < state.next {
                let lease = Lease {
                    visible_ms,
                    receives: 1,
                    first_ms: now_ms,
                };
                out.push(self.hand_out(topic.id, partition, frontier, lease)?);
                frontier += 1;
            }
            if frontier != before {
                let frontiers = self.trees.queue_frontiers();
                let key = partition_key(topic.id, partition);
                let mode = &mut OpMode::Txn(self.txn);
                frontiers.delete(self.ctx, mode, &key)?;
                frontiers.insert_unique(self.ctx, mode, &key, &frontier.to_le_bytes())?;
            }
        }
        Ok(Ok(out))
    }

    /// Leases a message as `lease` and reads it.
    fn hand_out(
        &mut self,
        topic: u32,
        partition: u32,
        offset: u64,
        lease: Lease,
    ) -> Result<QueueMessage, StorageError> {
        let record = read_record(self.ctx, &self.trees.records(), topic, partition, offset)?
            .ok_or_else(|| corrupt("partition missing a record"))?;
        self.put_lease(topic, partition, offset, &lease)?;
        Ok(QueueMessage {
            partition,
            offset,
            receipt: receipt(topic, partition, offset, lease.receives),
            value: record.value,
            timestamp_ms: record.timestamp_ms,
            receives: lease.receives,
            first_receive_ms: lease.first_ms,
        })
    }

    /// Acknowledges the message `receipt` names: it is done. A receipt of
    /// an earlier receive, or of a message already done, changes nothing.
    pub fn queue_ack(
        &mut self,
        topic: &str,
        receipt: &str,
    ) -> Result<Result<(), QueueError>, StorageError> {
        let (topic, partition, offset, receives) = match self.receipt_of(topic, receipt)? {
            Ok(found) => found,
            Err(err) => return Ok(Err(err)),
        };
        let Some(lease) = self.lease(topic, partition, offset)? else {
            // Done, or never received.
            return Ok(if offset < self.queue_frontier(topic, partition)? {
                Ok(())
            } else {
                Err(QueueError::InvalidReceipt)
            });
        };
        if lease.receives == receives {
            self.drop_lease(topic, partition, offset, &lease)?;
        }
        Ok(Ok(()))
    }

    /// Hides the message `receipt` names until `now_ms + visibility_ms` (0:
    /// visible at once).
    pub fn queue_change_visibility(
        &mut self,
        topic: &str,
        receipt: &str,
        visibility_ms: u64,
        now_ms: u64,
    ) -> Result<Result<(), QueueError>, StorageError> {
        let (topic, partition, offset, receives) = match self.receipt_of(topic, receipt)? {
            Ok(found) => found,
            Err(err) => return Ok(Err(err)),
        };
        let Some(lease) = self.lease(topic, partition, offset)? else {
            return Ok(Err(if offset < self.queue_frontier(topic, partition)? {
                QueueError::NotInFlight
            } else {
                QueueError::InvalidReceipt
            }));
        };
        if lease.receives != receives {
            return Ok(Err(QueueError::InvalidReceipt));
        }
        if lease.visible_ms <= now_ms {
            return Ok(Err(QueueError::NotInFlight));
        }
        let visible_ms = now_ms.saturating_add(visibility_ms);
        if visible_ms > lease.first_ms.saturating_add(MAX_VISIBILITY_MS) {
            return Ok(Err(QueueError::TooLong));
        }
        self.drop_lease(topic, partition, offset, &lease)?;
        self.put_lease(
            topic,
            partition,
            offset,
            &Lease {
                visible_ms,
                ..lease
            },
        )?;
        Ok(Ok(()))
    }

    /// The topic id, partition, offset and receive count `receipt` names,
    /// when it is one of `topic`'s.
    fn receipt_of(
        &mut self,
        topic: &str,
        receipt: &str,
    ) -> Result<Result<Receipt, QueueError>, StorageError> {
        let Some(row) = self.topic_row(topic)? else {
            return Ok(Err(QueueError::NoSuchTopic));
        };
        Ok(match parse_receipt(receipt) {
            Some((id, partition, offset, receives))
                if id == row.id && partition < row.partitions && receives > 0 =>
            {
                Ok((id, partition, offset, receives))
            }
            _ => Err(QueueError::InvalidReceipt),
        })
    }

    fn queue_frontier(&mut self, topic: u32, partition: u32) -> Result<u64, StorageError> {
        Ok(self
            .trees
            .queue_frontiers()
            .get(self.ctx, &partition_key(topic, partition))?
            .map(|row| u64_at(&row, 0))
            .transpose()?
            .unwrap_or(0))
    }

    fn lease(
        &mut self,
        topic: u32,
        partition: u32,
        offset: u64,
    ) -> Result<Option<Lease>, StorageError> {
        self.trees
            .queue_leases()
            .get(self.ctx, &lease_key(topic, partition, offset))?
            .map(|row| decode_lease(&row))
            .transpose()
    }

    /// Records `lease`, and indexes it by when it is visible again.
    fn put_lease(
        &mut self,
        topic: u32,
        partition: u32,
        offset: u64,
        lease: &Lease,
    ) -> Result<(), StorageError> {
        let leases = self.trees.queue_leases();
        let key = lease_key(topic, partition, offset);
        let mode = &mut OpMode::Txn(self.txn);
        leases.delete(self.ctx, mode, &key)?;
        leases.insert_unique(self.ctx, mode, &key, &encode_lease(lease))?;
        self.trees.queue_times().insert_unique(
            self.ctx,
            mode,
            &time_key(topic, partition, lease.visible_ms, offset),
            &[],
        )?;
        Ok(())
    }

    /// Removes `lease` and its index entry.
    fn drop_lease(
        &mut self,
        topic: u32,
        partition: u32,
        offset: u64,
        lease: &Lease,
    ) -> Result<(), StorageError> {
        let mode = &mut OpMode::Txn(self.txn);
        self.trees
            .queue_leases()
            .delete(self.ctx, mode, &lease_key(topic, partition, offset))?;
        self.trees.queue_times().delete(
            self.ctx,
            mode,
            &time_key(topic, partition, lease.visible_ms, offset),
        )?;
        Ok(())
    }
}

fn be_u64(bytes: &[u8]) -> Result<u64, StorageError> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| corrupt("queue key"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leases_and_receipts_round_trip_and_corruption_is_refused() {
        let lease = Lease {
            visible_ms: 5,
            receives: 3,
            first_ms: 2,
        };
        let row = encode_lease(&lease);
        assert_eq!(decode_lease(&row).unwrap(), lease);
        assert!(decode_lease(&row[..19]).is_err());
        assert!(decode_lease(&[row.clone(), vec![0]].concat()).is_err());
        assert_eq!(parse_receipt(&receipt(7, 1, 42, 3)), Some((7, 1, 42, 3)));
        for bad in ["", "7-1-42", "7-1-42-3-9", "7-1-x-3", "-1-1-1"] {
            assert_eq!(parse_receipt(bad), None, "{bad}");
        }
    }
}
