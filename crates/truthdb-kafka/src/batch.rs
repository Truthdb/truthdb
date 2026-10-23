//! Records as a Kafka record batch (format v2, "magic" 2): one batch per
//! partition a fetch returns, uncompressed, with no producer id, records'
//! timestamps their append times (as `CreateTime`, attributes 0).

use truthdb_streams::Record;

use crate::wire::Writer;

static CRC32C: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

/// The bytes before the CRC's range: base offset, batch length, partition
/// leader epoch, magic and the CRC itself.
const CRC_START: usize = 8 + 4 + 4 + 1 + 4;

/// `records` (consecutive offsets, at least one) as one record batch.
pub fn encode(records: &[Record]) -> Vec<u8> {
    let first = &records[0];
    let base_ms = first.timestamp_ms as i64;
    let max_ms = records
        .iter()
        .map(|record| record.timestamp_ms)
        .max()
        .unwrap_or_default() as i64;
    let mut batch = Writer::default();
    batch
        .i64(first.offset as i64)
        .i32(0) // batch length, set below
        .i32(-1) // partition leader epoch: none (no leader changes)
        .i8(2) // magic
        .i32(0) // CRC, set below
        .i16(0) // attributes
        .i32((records[records.len() - 1].offset - first.offset) as i32)
        .i64(base_ms)
        .i64(max_ms)
        .i64(-1) // producer id
        .i16(-1) // producer epoch
        .i32(-1) // base sequence
        .i32(records.len() as i32);
    for record in records {
        let mut body = Writer::default();
        body.i8(0) // attributes
            .varint(record.timestamp_ms as i64 - base_ms)
            .varint((record.offset - first.offset) as i64);
        match &record.key {
            Some(key) => {
                body.varint(key.len() as i64);
                body.bytes.extend_from_slice(key);
            }
            None => {
                body.varint(-1);
            }
        }
        body.varint(record.value.len() as i64);
        body.bytes.extend_from_slice(&record.value);
        body.varint(0); // headers
        batch.varint(body.bytes.len() as i64);
        batch.bytes.extend_from_slice(&body.bytes);
    }
    let mut bytes = batch.bytes;
    let length = (bytes.len() - 12) as i32;
    bytes[8..12].copy_from_slice(&length.to_be_bytes());
    let crc = CRC32C.checksum(&bytes[CRC_START..]);
    bytes[CRC_START - 4..CRC_START].copy_from_slice(&crc.to_be_bytes());
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(offset: u64, timestamp_ms: u64, key: Option<&[u8]>, value: &[u8]) -> Record {
        Record {
            offset,
            timestamp_ms,
            key: key.map(<[u8]>::to_vec),
            value: value.to_vec(),
        }
    }

    #[test]
    fn encodes_a_batch_as_kafka_does() {
        // The same batch built by kafka-python 3.0.11's
        // DefaultRecordBatchBuilder(magic=2, compression_type=0,
        // is_transactional=False, producer_id=-1, producer_epoch=-1,
        // base_sequence=-1, batch_size=1 << 20) with append(0, 1000, b"k",
        // b"v1", []) and append(1, 1005, None, b"value-2", []), base offset
        // 7 and partition leader epoch -1 (bytes the CRC does not cover).
        let bytes = encode(&[
            record(7, 1000, Some(b"k"), b"v1"),
            record(8, 1005, None, b"value-2"),
        ]);
        let expected = "000000000000000700000049ffffffff02af6ab8c100000000000100000000000003e8\
                        00000000000003edffffffffffffffffffffffffffff0000000212000000026b04763100\
                        1a000a02010e76616c75652d3200";
        assert_eq!(hex(&bytes), expected);
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
