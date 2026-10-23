//! Topics read as queues (the SQL queue's `sys.sp_s3_queue_*`), the way
//! Amazon SQS serves one: a receive hands out up to ten visible messages,
//! each hidden for its visibility timeout and named by a receipt handle new
//! with each receive; an acknowledged message is done; one not acknowledged
//! in time is visible again (delivery is at least once). A message's lease
//! records when it is visible again, how often and since when it has been
//! received, and whether it is done. A partition's head moves past the done
//! messages at its front, whose leases then go; records the topic's
//! retention removed are past it too.

use std::collections::BTreeMap;

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::StreamTxn;
use crate::store::{corrupt, partition_key, read_record, u64_at};

/// How long a received message stays hidden at most, from its first
/// receive (SQS's 12 hours).
pub const MAX_VISIBILITY_MS: u64 = 12 * 3_600_000;
/// Offsets one receive looks at, at most, past those in flight.
const SCAN_LIMIT: usize = 1000;

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
    done: bool,
}

/// `[visible ms u64][receives u32][first receive ms u64][done u8]`.
fn encode_lease(lease: &Lease) -> Vec<u8> {
    [
        &lease.visible_ms.to_le_bytes()[..],
        &lease.receives.to_le_bytes(),
        &lease.first_ms.to_le_bytes(),
        &[lease.done as u8],
    ]
    .concat()
}

fn decode_lease(row: &[u8]) -> Result<Lease, StorageError> {
    if row.len() != 21 || row[20] > 1 {
        return Err(corrupt("queue lease"));
    }
    Ok(Lease {
        visible_ms: u64_at(row, 0)?,
        receives: u32::from_le_bytes(row[8..12].try_into().unwrap()),
        first_ms: u64_at(row, 12)?,
        done: row[20] == 1,
    })
}

fn lease_key(topic: u32, partition: u32, offset: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&partition_key(topic, partition));
    key[8..].copy_from_slice(&offset.to_be_bytes());
    key
}

/// `topic-partition-offset-receives`.
fn receipt(topic: u32, partition: u32, offset: u64, receives: u32) -> String {
    format!("{topic}-{partition}-{offset}-{receives}")
}

fn parse_receipt(text: &str) -> Option<(u32, u32, u64, u32)> {
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
    /// Up to `max` visible messages of `topic`, partition by partition
    /// (each oldest first), each hidden until `now_ms + visibility_ms`.
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
        let mut out = Vec::new();
        let mut looked = 0;
        // Starting from a partition the time picks, so a busy one does not
        // keep the others waiting.
        let first = (now_ms % u64::from(topic.partitions)) as u32;
        for partition in (first..topic.partitions).chain(0..first) {
            let state = self.partition_row(topic.id, partition)?;
            let head = self.queue_head(topic.id, partition)?.max(state.start);
            let leases: BTreeMap<u64, Lease> = self
                .trees
                .queue_leases()
                .scan_range(
                    self.ctx,
                    Some(&lease_key(topic.id, partition, head)),
                    Some(&lease_key(topic.id, partition, u64::MAX)),
                )?
                .into_iter()
                .map(|(key, row)| Ok((u64_at_be(&key[8..])?, decode_lease(&row)?)))
                .collect::<Result<_, StorageError>>()?;
            let mut offset = head;
            while offset < state.next && out.len() < max && looked < SCAN_LIMIT {
                let lease = leases.get(&offset).copied();
                if lease.is_some_and(|lease| lease.done || lease.visible_ms > now_ms) {
                    offset += 1;
                    continue;
                }
                looked += 1;
                let record =
                    read_record(self.ctx, &self.trees.records(), topic.id, partition, offset)?
                        .ok_or_else(|| corrupt("partition missing a record"))?;
                let lease = Lease {
                    visible_ms: now_ms.saturating_add(visibility_ms),
                    receives: lease.map_or(0, |lease| lease.receives) + 1,
                    first_ms: lease.map_or(now_ms, |lease| lease.first_ms),
                    done: false,
                };
                self.put_lease(topic.id, partition, offset, &lease)?;
                out.push(QueueMessage {
                    partition,
                    offset,
                    receipt: receipt(topic.id, partition, offset, lease.receives),
                    value: record.value,
                    timestamp_ms: record.timestamp_ms,
                    receives: lease.receives,
                    first_receive_ms: lease.first_ms,
                });
                offset += 1;
            }
        }
        Ok(Ok(out))
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
        let key = lease_key(topic, partition, offset);
        let Some(row) = self.trees.queue_leases().get(self.ctx, &key)? else {
            // Done (its lease gone past the head), or never received.
            let head = self.queue_head(topic, partition)?;
            let start = self.partition_row(topic, partition)?.start;
            return Ok(if offset < head.max(start) {
                Ok(())
            } else {
                Err(QueueError::InvalidReceipt)
            });
        };
        let mut lease = decode_lease(&row)?;
        if lease.receives != receives || lease.done {
            return Ok(Ok(()));
        }
        lease.done = true;
        self.put_lease(topic, partition, offset, &lease)?;
        self.advance_head(topic, partition)?;
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
        let Some(row) = self
            .trees
            .queue_leases()
            .get(self.ctx, &lease_key(topic, partition, offset))?
        else {
            return Ok(Err(QueueError::InvalidReceipt));
        };
        let mut lease = decode_lease(&row)?;
        if lease.receives != receives {
            return Ok(Err(QueueError::InvalidReceipt));
        }
        if lease.done || lease.visible_ms <= now_ms {
            return Ok(Err(QueueError::NotInFlight));
        }
        let visible_ms = now_ms.saturating_add(visibility_ms);
        if visible_ms > lease.first_ms.saturating_add(MAX_VISIBILITY_MS) {
            return Ok(Err(QueueError::TooLong));
        }
        lease.visible_ms = visible_ms;
        self.put_lease(topic, partition, offset, &lease)?;
        Ok(Ok(()))
    }

    /// The topic id, partition, offset and receive count `receipt` names,
    /// when it is one of `topic`'s.
    fn receipt_of(
        &mut self,
        topic: &str,
        receipt: &str,
    ) -> Result<Result<(u32, u32, u64, u32), QueueError>, StorageError> {
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

    fn queue_head(&mut self, topic: u32, partition: u32) -> Result<u64, StorageError> {
        Ok(self
            .trees
            .queue_heads()
            .get(self.ctx, &partition_key(topic, partition))?
            .map(|row| u64_at(&row, 0))
            .transpose()?
            .unwrap_or(0))
    }

    fn put_lease(
        &mut self,
        topic: u32,
        partition: u32,
        offset: u64,
        lease: &Lease,
    ) -> Result<(), StorageError> {
        let tree = self.trees.queue_leases();
        let key = lease_key(topic, partition, offset);
        let mode = &mut OpMode::Txn(self.txn);
        tree.delete(self.ctx, mode, &key)?;
        tree.insert_unique(self.ctx, mode, &key, &encode_lease(lease))?;
        Ok(())
    }

    /// Moves the partition's head past the done messages at its front (and
    /// those retention removed), removing their leases.
    fn advance_head(&mut self, topic: u32, partition: u32) -> Result<(), StorageError> {
        let start = self.partition_row(topic, partition)?.start;
        let mut head = self.queue_head(topic, partition)?.max(start);
        let leases = self.trees.queue_leases();
        let mode = &mut OpMode::Txn(self.txn);
        // Leases of messages retention removed.
        for (key, _) in leases.scan_range(
            self.ctx,
            Some(&lease_key(topic, partition, 0)),
            Some(&lease_key(topic, partition, start.saturating_sub(1))),
        )? {
            if u64_at_be(&key[8..])? < start {
                leases.delete(self.ctx, mode, &key)?;
            }
        }
        loop {
            let key = lease_key(topic, partition, head);
            match leases.get(self.ctx, &key)? {
                Some(row) if decode_lease(&row)?.done => {
                    leases.delete(self.ctx, mode, &key)?;
                    head += 1;
                }
                _ => break,
            }
        }
        let heads = self.trees.queue_heads();
        let key = partition_key(topic, partition);
        heads.delete(self.ctx, mode, &key)?;
        heads.insert_unique(self.ctx, mode, &key, &head.to_le_bytes())?;
        Ok(())
    }
}

fn u64_at_be(bytes: &[u8]) -> Result<u64, StorageError> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| corrupt("queue lease key"))
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
            done: true,
        };
        let row = encode_lease(&lease);
        assert_eq!(decode_lease(&row).unwrap(), lease);
        assert!(decode_lease(&row[..20]).is_err());
        let mut bad = row.clone();
        bad[20] = 2;
        assert!(decode_lease(&bad).is_err());
        assert_eq!(parse_receipt(&receipt(7, 1, 42, 3)), Some((7, 1, 42, 3)));
        for bad in ["", "7-1-42", "7-1-42-3-9", "7-1-x-3", "-1-1-1"] {
            assert_eq!(parse_receipt(bad), None, "{bad}");
        }
    }
}
