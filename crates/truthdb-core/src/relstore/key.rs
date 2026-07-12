//! Order-preserving key encoding: encoded keys compare with `memcmp` in the
//! same order as their typed values.
//!
//! Per key column: a NULL sorts first as a single `0x00` byte; non-NULL
//! values are `0x01` followed by a type-specific payload:
//! - integers/decimals: big-endian with the sign bit flipped
//! - floats: IEEE-754 total-order transform, big-endian
//! - date/time/datetime2: their tick counts, big-endian (date-major)
//! - uniqueidentifier: bytes permuted into SQL Server comparison order
//! - strings/binary: raw bytes with `0x00 -> 0x00 0xFF` escaping and a
//!   `0x00 0x00` terminator so composite keys stay order-preserving
//!   (NVARCHAR uses UTF-16BE code units — binary collation until Stage 5;
//!   collation sort keys replace these payloads then).
//!
//! Keys are compared, never decoded: B+ tree leaves carry the full row, so
//! values are always recoverable without reversing this encoding.

use crate::relstore::row::Schema;
use crate::relstore::types::{Datum, TypeError};

const NULL_MARKER: u8 = 0x00;
const VALUE_MARKER: u8 = 0x01;

/// SQL Server's uniqueidentifier comparison evaluates bytes in this
/// significance order (most significant first).
const GUID_COMPARE_ORDER: [usize; 16] = [10, 11, 12, 13, 14, 15, 8, 9, 7, 6, 5, 4, 3, 2, 1, 0];

/// Encodes the key columns (by schema index) of a row.
pub fn encode_key(
    schema: &Schema,
    key_columns: &[usize],
    values: &[Datum],
) -> Result<Vec<u8>, TypeError> {
    let mut out = Vec::new();
    for &index in key_columns {
        let column = schema
            .columns
            .get(index)
            .ok_or_else(|| TypeError(format!("key column index {index} out of range")))?;
        let value = values
            .get(index)
            .ok_or_else(|| TypeError(format!("row has no value for key column {index}")))?;
        let _ = column;
        encode_datum(value, &mut out)?;
    }
    Ok(out)
}

pub fn encode_datum(value: &Datum, out: &mut Vec<u8>) -> Result<(), TypeError> {
    if value.is_null() {
        out.push(NULL_MARKER);
        return Ok(());
    }
    out.push(VALUE_MARKER);
    match value {
        Datum::TinyInt(v) => out.push(*v),
        Datum::SmallInt(v) => out.extend_from_slice(&((*v as u16) ^ 0x8000).to_be_bytes()),
        Datum::Int(v) => out.extend_from_slice(&((*v as u32) ^ 0x8000_0000).to_be_bytes()),
        Datum::BigInt(v) => {
            out.extend_from_slice(&((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes())
        }
        Datum::Bit(v) => out.push(*v as u8),
        Datum::Real(v) => out.extend_from_slice(&total_order_f32(*v).to_be_bytes()),
        Datum::Float(v) => out.extend_from_slice(&total_order_f64(*v).to_be_bytes()),
        Datum::Decimal(v) => out.extend_from_slice(&((*v as u128) ^ (1u128 << 127)).to_be_bytes()),
        Datum::Date(days) => out.extend_from_slice(&days.to_be_bytes()),
        Datum::Time(ticks) => out.extend_from_slice(&ticks.to_be_bytes()),
        Datum::DateTime2(days, ticks) => {
            out.extend_from_slice(&days.to_be_bytes());
            out.extend_from_slice(&ticks.to_be_bytes());
        }
        Datum::UniqueIdentifier(bytes) => {
            for &position in &GUID_COMPARE_ORDER {
                out.push(bytes[position]);
            }
        }
        Datum::VarChar(s) => encode_escaped(s.as_bytes(), out),
        Datum::NVarChar(s) => {
            let units: Vec<u8> = s.encode_utf16().flat_map(|u| u.to_be_bytes()).collect();
            encode_escaped(&units, out);
        }
        Datum::VarBinary(b) => encode_escaped(b, out),
        Datum::Null => unreachable!(),
    }
    Ok(())
}

/// Escapes 0x00 bytes so a terminator can mark the end of the value without
/// breaking ordering: `0x00` becomes `0x00 0xFF`, and the value ends with
/// `0x00 0x00`. A shorter prefix therefore always sorts before any
/// continuation.
fn encode_escaped(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0x00);
    out.push(0x00);
}

/// IEEE-754 total-order transform: the resulting unsigned integers compare
/// like the floats (with -0.0 < 0.0 and NaN ordered after +inf).
fn total_order_f64(v: f64) -> u64 {
    let bits = v.to_bits();
    if bits & 0x8000_0000_0000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000_0000_0000
    }
}

fn total_order_f32(v: f32) -> u32 {
    let bits = v.to_bits();
    if bits & 0x8000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    fn encoded(value: &Datum) -> Vec<u8> {
        let mut out = Vec::new();
        encode_datum(value, &mut out).expect("encode");
        out
    }

    fn assert_order(a: &Datum, b: &Datum, expected: Ordering) {
        assert_eq!(
            encoded(a).cmp(&encoded(b)),
            expected,
            "key order mismatch for {a:?} vs {b:?}"
        );
    }

    /// Deterministic xorshift so the property tests need no new deps.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
    }

    #[test]
    fn null_sorts_first() {
        for value in [
            Datum::Int(i32::MIN),
            Datum::BigInt(i64::MIN),
            Datum::Float(f64::NEG_INFINITY),
            Datum::VarChar(String::new()),
        ] {
            assert_order(&Datum::Null, &value, Ordering::Less);
        }
    }

    #[test]
    fn integer_order_is_preserved() {
        let mut rng = Rng(0xDEADBEEF12345678);
        for _ in 0..2000 {
            let a = rng.next() as i64;
            let b = rng.next() as i64;
            assert_order(&Datum::BigInt(a), &Datum::BigInt(b), a.cmp(&b));
            let (a, b) = (a as i32, b as i32);
            assert_order(&Datum::Int(a), &Datum::Int(b), a.cmp(&b));
            let (a, b) = (a as i16, b as i16);
            assert_order(&Datum::SmallInt(a), &Datum::SmallInt(b), a.cmp(&b));
        }
    }

    #[test]
    fn float_order_is_preserved() {
        let interesting = [
            f64::NEG_INFINITY,
            -1e300,
            -1.5,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            1.5,
            1e300,
            f64::INFINITY,
        ];
        for (i, a) in interesting.iter().enumerate() {
            for (j, b) in interesting.iter().enumerate() {
                let expected = if i == j {
                    // -0.0 and 0.0 encode distinctly (total order) but any
                    // consistent order is fine; only test distinct indices.
                    continue;
                } else {
                    i.cmp(&j)
                };
                assert_order(&Datum::Float(*a), &Datum::Float(*b), expected);
            }
        }
    }

    #[test]
    fn decimal_order_is_preserved() {
        let mut rng = Rng(0xC0FFEE);
        for _ in 0..2000 {
            let a = (rng.next() as i128) * (rng.next() as i64 as i128);
            let b = (rng.next() as i128) * (rng.next() as i64 as i128);
            assert_order(&Datum::Decimal(a), &Datum::Decimal(b), a.cmp(&b));
        }
    }

    #[test]
    fn string_order_and_embedded_zero_safety() {
        assert_order(
            &Datum::VarChar("a".to_string()),
            &Datum::VarChar("b".to_string()),
            Ordering::Less,
        );
        assert_order(
            &Datum::VarChar("a".to_string()),
            &Datum::VarChar("ab".to_string()),
            Ordering::Less,
        );
        // Embedded NUL must not terminate the key early.
        assert_order(
            &Datum::VarBinary(vec![0x61]),
            &Datum::VarBinary(vec![0x61, 0x00]),
            Ordering::Less,
        );
        assert_order(
            &Datum::VarBinary(vec![0x61, 0x00]),
            &Datum::VarBinary(vec![0x61, 0x00, 0x00]),
            Ordering::Less,
        );
        assert_order(
            &Datum::VarBinary(vec![0x61, 0x00, 0xFF]),
            &Datum::VarBinary(vec![0x61, 0x01]),
            Ordering::Less,
        );
    }

    #[test]
    fn composite_keys_compare_column_major() {
        use crate::relstore::row::Column;
        use crate::relstore::types::ColumnType;
        let schema = Schema {
            columns: vec![
                Column {
                    name: "a".to_string(),
                    column_type: ColumnType::VarChar { max_len: 10 },
                    nullable: true,
                    collation: None,
                },
                Column {
                    name: "b".to_string(),
                    column_type: ColumnType::Int,
                    nullable: true,
                    collation: None,
                },
            ],
        };
        let key = |s: &str, n: i32| {
            encode_key(
                &schema,
                &[0, 1],
                &[Datum::VarChar(s.to_string()), Datum::Int(n)],
            )
            .expect("key")
        };
        // First column dominates; the terminator keeps "ab" < "b" even
        // though 'b' bytes follow in the second key.
        assert!(key("ab", 999) < key("b", 0));
        assert!(key("a", 2) < key("a", 10));
        assert!(key("a", 10) < key("ab", 0));
    }

    #[test]
    fn temporal_order_is_chronological() {
        assert_order(&Datum::Date(100), &Datum::Date(101), Ordering::Less);
        assert_order(&Datum::Time(5), &Datum::Time(6), Ordering::Less);
        assert_order(
            &Datum::DateTime2(100, u64::MAX >> 24),
            &Datum::DateTime2(101, 0),
            Ordering::Less,
        );
    }

    #[test]
    fn guid_order_matches_sql_server_byte_groups() {
        // Differ only in byte 15 (most significant group) vs byte 0 (least).
        let mut low = [0u8; 16];
        let mut high = [0u8; 16];
        low[0] = 0xFF; // least significant position
        high[15] = 0x01; // part of the most significant group
        assert_order(
            &Datum::UniqueIdentifier(low),
            &Datum::UniqueIdentifier(high),
            Ordering::Less,
        );
    }
}
