//! TDS TYPE_INFO and value codecs for the Stage 4 type set: INTN, BITN,
//! FLTN, NVARCHAR, and BIGVARCHR (MS-TDS 2.2.5.4 / 2.2.5.5).
//!
//! Every Stage 3 result column maps to one of these nullable ("N") variants
//! so a NULL is always representable.

use truthdb_core::relstore::types::{ColumnType, Datum};

// Type tokens.
const INTN: u8 = 0x26;
const BITN: u8 = 0x68;
const FLTN: u8 = 0x6d;
const NVARCHAR: u8 = 0xe7;
const BIGVARCHR: u8 = 0xa7;

/// A 5-byte collation for a Latin1-general, case-insensitive, accent-
/// sensitive locale (LCID 0x0409, SortId 0x34 = code page 1252). Character
/// columns carry it; clients use it to pick a decoder for BIGVARCHR bytes.
const COLLATION: [u8; 5] = [0x09, 0x04, 0xd0, 0x00, 0x34];

/// Max UCS-2 code units in a non-MAX NVARCHAR row value: 32767 units = 65534
/// bytes, the largest even byte count a u16 value-length prefix can hold.
const MAX_USHORT_UCS2_UNITS: usize = 32767;

/// The TDS type a result column is sent as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TdsType {
    Int(u8),   // INTN with max byte length 1/2/4/8
    Bit,       // BITN
    Float(u8), // FLTN with max byte length 4/8
    NVarChar(u16),
    VarChar(u16),
}

fn tds_type(column_type: &ColumnType) -> TdsType {
    match column_type {
        ColumnType::TinyInt => TdsType::Int(1),
        ColumnType::SmallInt => TdsType::Int(2),
        ColumnType::Int => TdsType::Int(4),
        ColumnType::BigInt => TdsType::Int(8),
        ColumnType::Bit => TdsType::Bit,
        ColumnType::Real => TdsType::Float(4),
        ColumnType::Float => TdsType::Float(8),
        ColumnType::NVarChar { max_len } => TdsType::NVarChar((*max_len).max(1)),
        ColumnType::VarChar { max_len } => TdsType::VarChar((*max_len).max(1)),
        // Types not producible by Stage 3/4 SQL fall back to NVARCHAR of the
        // rendered text.
        _ => TdsType::NVarChar(4000),
    }
}

/// Encodes the TYPE_INFO bytes for a result column.
pub fn encode_type_info(column_type: &ColumnType) -> Vec<u8> {
    let mut out = Vec::new();
    match tds_type(column_type) {
        TdsType::Int(len) => {
            out.push(INTN);
            out.push(len);
        }
        TdsType::Bit => {
            out.push(BITN);
            out.push(1);
        }
        TdsType::Float(len) => {
            out.push(FLTN);
            out.push(len);
        }
        TdsType::NVarChar(max_len) => {
            out.push(NVARCHAR);
            // NVARCHAR length is in bytes; UCS-2 doubles the char count.
            out.extend_from_slice(&max_byte_len(max_len).to_le_bytes());
            out.extend_from_slice(&COLLATION);
        }
        TdsType::VarChar(max_len) => {
            out.push(BIGVARCHR);
            out.extend_from_slice(&max_len.to_le_bytes());
            out.extend_from_slice(&COLLATION);
        }
    }
    out
}

/// NVARCHAR max byte length, clamped to the non-MAX limit (8000 bytes).
fn max_byte_len(max_chars: u16) -> u16 {
    (max_chars as u32 * 2).min(8000) as u16
}

/// Encodes one row value for a column.
pub fn encode_value(datum: &Datum, column_type: &ColumnType) -> Vec<u8> {
    let mut out = Vec::new();
    match tds_type(column_type) {
        TdsType::Int(len) => match int_value(datum) {
            Some(v) => {
                out.push(len);
                out.extend_from_slice(&v.to_le_bytes()[..len as usize]);
            }
            None => out.push(0), // NULL: zero-length
        },
        TdsType::Bit => match datum {
            Datum::Bit(b) => {
                out.push(1);
                out.push(*b as u8);
            }
            Datum::Null => out.push(0),
            _ => out.push(0),
        },
        TdsType::Float(len) => match float_value(datum) {
            Some(v) => {
                out.push(len);
                if len == 4 {
                    out.extend_from_slice(&(v as f32).to_le_bytes());
                } else {
                    out.extend_from_slice(&v.to_le_bytes());
                }
            }
            None => out.push(0),
        },
        TdsType::NVarChar(_) => match string_value(datum) {
            Some(s) => {
                // Cap at 32767 UCS-2 units so the u16 byte-length prefix
                // (<= 65534) never wraps; a longer value would need
                // NVARCHAR(MAX)/PLP, which arrives in a later stage.
                let bytes: Vec<u8> = s
                    .encode_utf16()
                    .take(MAX_USHORT_UCS2_UNITS)
                    .flat_map(|u| u.to_le_bytes())
                    .collect();
                out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                out.extend_from_slice(&bytes);
            }
            // NULL charbin uses the 0xFFFF sentinel length.
            None => out.extend_from_slice(&0xffffu16.to_le_bytes()),
        },
        TdsType::VarChar(_) => match string_value(datum) {
            Some(s) => {
                // BIGVARCHR bytes are decoded by the client in the advertised
                // CP1252 collation, so transcode from UTF-8 (unmappable ->
                // '?', matching SQL Server) instead of shipping raw UTF-8 that
                // a CP1252 client would mojibake. Cap at 65535 bytes for u16.
                let mut bytes = encode_cp1252(&s);
                bytes.truncate(u16::MAX as usize);
                out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                out.extend_from_slice(&bytes);
            }
            None => out.extend_from_slice(&0xffffu16.to_le_bytes()),
        },
    }
    out
}

fn int_value(datum: &Datum) -> Option<i64> {
    match datum {
        Datum::TinyInt(v) => Some(*v as i64),
        Datum::SmallInt(v) => Some(*v as i64),
        Datum::Int(v) => Some(*v as i64),
        Datum::BigInt(v) => Some(*v),
        _ => None,
    }
}

fn float_value(datum: &Datum) -> Option<f64> {
    match datum {
        Datum::Real(v) => Some(*v as f64),
        Datum::Float(v) => Some(*v),
        _ => None,
    }
}

fn string_value(datum: &Datum) -> Option<String> {
    match datum {
        Datum::VarChar(s) | Datum::NVarChar(s) => Some(s.clone()),
        Datum::Null => None,
        // Any other datum reaching a string column is rendered as text.
        other => truthdb_core::rel::render_cell(other, &ColumnType::NVarChar { max_len: 4000 }),
    }
}

/// Encodes a string as Windows-1252 (the advertised BIGVARCHR collation),
/// substituting '?' for characters with no CP1252 representation — the same
/// lossy behavior SQL Server applies when storing Unicode into a Latin1
/// VARCHAR. CP1252 equals ISO-8859-1 except the 0x80..=0x9F block, which maps
/// a handful of punctuation/symbol code points.
fn encode_cp1252(s: &str) -> Vec<u8> {
    s.chars()
        .map(|c| char_to_cp1252(c).unwrap_or(b'?'))
        .collect()
}

fn char_to_cp1252(c: char) -> Option<u8> {
    let cp = c as u32;
    let byte = match cp {
        0x00..=0x7F | 0xA0..=0xFF => cp as u8,
        0x20AC => 0x80,
        0x201A => 0x82,
        0x0192 => 0x83,
        0x201E => 0x84,
        0x2026 => 0x85,
        0x2020 => 0x86,
        0x2021 => 0x87,
        0x02C6 => 0x88,
        0x2030 => 0x89,
        0x0160 => 0x8A,
        0x2039 => 0x8B,
        0x0152 => 0x8C,
        0x017D => 0x8E,
        0x2018 => 0x91,
        0x2019 => 0x92,
        0x201C => 0x93,
        0x201D => 0x94,
        0x2022 => 0x95,
        0x2013 => 0x96,
        0x2014 => 0x97,
        0x02DC => 0x98,
        0x2122 => 0x99,
        0x0161 => 0x9A,
        0x203A => 0x9B,
        0x0153 => 0x9C,
        0x017E => 0x9E,
        0x0178 => 0x9F,
        _ => return None,
    };
    Some(byte)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_type_info_and_values() {
        assert_eq!(encode_type_info(&ColumnType::Int), vec![INTN, 4]);
        assert_eq!(
            encode_value(&Datum::Int(258), &ColumnType::Int),
            vec![4, 0x02, 0x01, 0x00, 0x00]
        );
        assert_eq!(encode_value(&Datum::Null, &ColumnType::Int), vec![0]);
        assert_eq!(
            encode_value(&Datum::BigInt(-1), &ColumnType::BigInt),
            vec![8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
        );
    }

    #[test]
    fn bit_and_float_values() {
        assert_eq!(encode_type_info(&ColumnType::Bit), vec![BITN, 1]);
        assert_eq!(
            encode_value(&Datum::Bit(true), &ColumnType::Bit),
            vec![1, 1]
        );
        assert_eq!(encode_value(&Datum::Null, &ColumnType::Bit), vec![0]);
        assert_eq!(encode_type_info(&ColumnType::Float), vec![FLTN, 8]);
        assert_eq!(encode_value(&Datum::Float(1.5), &ColumnType::Float), {
            let mut v = vec![8u8];
            v.extend_from_slice(&1.5f64.to_le_bytes());
            v
        });
    }

    #[test]
    fn nvarchar_type_info_and_values() {
        let ct = ColumnType::NVarChar { max_len: 50 };
        let ti = encode_type_info(&ct);
        assert_eq!(ti[0], NVARCHAR);
        assert_eq!(u16::from_le_bytes([ti[1], ti[2]]), 100); // 50 chars * 2
        assert_eq!(&ti[3..8], &COLLATION);

        let value = encode_value(&Datum::NVarChar("hi".to_string()), &ct);
        assert_eq!(u16::from_le_bytes([value[0], value[1]]), 4); // 2 chars * 2
        assert_eq!(&value[2..], &[b'h', 0, b'i', 0]);

        // NULL uses 0xFFFF.
        assert_eq!(encode_value(&Datum::Null, &ct), vec![0xff, 0xff]);
    }

    #[test]
    fn varchar_bytes_are_single_byte() {
        let ct = ColumnType::VarChar { max_len: 20 };
        let ti = encode_type_info(&ct);
        assert_eq!(ti[0], BIGVARCHR);
        let value = encode_value(&Datum::VarChar("abc".to_string()), &ct);
        assert_eq!(u16::from_le_bytes([value[0], value[1]]), 3);
        assert_eq!(&value[2..], b"abc");
    }

    #[test]
    fn varchar_transcodes_to_cp1252_not_utf8() {
        let ct = ColumnType::VarChar { max_len: 20 };
        // 'café': é is one CP1252 byte 0xE9 (two UTF-8 bytes); '€' is 0x80;
        // an unmappable char ('中') becomes '?'.
        let value = encode_value(&Datum::VarChar("café€中".to_string()), &ct);
        assert_eq!(u16::from_le_bytes([value[0], value[1]]), 6);
        assert_eq!(&value[2..], &[b'c', b'a', b'f', 0xE9, 0x80, b'?']);
    }

    #[test]
    fn nvarchar_value_length_prefix_never_wraps() {
        // A 40000-char value is 80000 UTF-16 bytes; a naive `as u16` would wrap
        // to 14464 and desync the ROW stream. The value is capped so the prefix
        // matches the emitted bytes exactly.
        let ct = ColumnType::NVarChar { max_len: 4000 };
        let value = encode_value(&Datum::NVarChar("a".repeat(40000)), &ct);
        let declared = u16::from_le_bytes([value[0], value[1]]) as usize;
        assert_eq!(declared, MAX_USHORT_UCS2_UNITS * 2); // 65534
        assert_eq!(value.len(), 2 + declared);
    }
}
