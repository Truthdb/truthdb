//! TDS TYPE_INFO and value codecs (MS-TDS 2.2.5.4 / 2.2.5.5). Covers INTN,
//! BITN, FLTN, DECIMALN, DATE, DATETIME2, UNIQUEIDENTIFIER, NVARCHAR,
//! BIGVARCHR, and BIGVARBINARY — one per storage column type. Nullable ("N")
//! variants make a NULL always representable via a zero (or 0xFFFF) length.

use truthdb_core::relstore::types::{ColumnType, Datum};

// Type tokens.
const INTN: u8 = 0x26;
const BITN: u8 = 0x68;
const FLTN: u8 = 0x6d;
const DECIMALN: u8 = 0x6a;
const DATEN: u8 = 0x28;
const DATETIME2N: u8 = 0x2a;
const GUID: u8 = 0x24;
const NVARCHAR: u8 = 0xe7;
const BIGVARCHR: u8 = 0xa7;
const BIGVARBINARY: u8 = 0xa5;

/// A 5-byte collation for a Latin1-general, case-insensitive, accent-
/// sensitive locale (LCID 0x0409, SortId 0x34 = code page 1252). Character
/// columns carry it; clients use it to pick a decoder for BIGVARCHR bytes.
pub const COLLATION: [u8; 5] = [0x09, 0x04, 0xd0, 0x00, 0x34];

/// Max UCS-2 code units in a non-MAX NVARCHAR row value: 32767 units = 65534
/// bytes, the largest even byte count a u16 value-length prefix can hold.
const MAX_USHORT_UCS2_UNITS: usize = 32767;

/// The TDS type a result column is sent as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TdsType {
    Int(u8),         // INTN with max byte length 1/2/4/8
    Bit,             // BITN
    Float(u8),       // FLTN with max byte length 4/8
    Decimal(u8, u8), // DECIMALN (precision, scale)
    Date,            // DATEN
    DateTime2,       // DATETIME2N (scale 7)
    Guid,            // UNIQUEIDENTIFIER
    NVarChar(u16),
    VarChar(u16),
    VarBinary(u16),
    /// (MAX) types: 0xFFFF max length in COLMETADATA, PLP-encoded values.
    NVarCharMax,
    VarCharMax,
    VarBinaryMax,
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
        ColumnType::Decimal { precision, scale } => TdsType::Decimal(*precision, *scale),
        ColumnType::Date => TdsType::Date,
        ColumnType::DateTime2 => TdsType::DateTime2,
        ColumnType::UniqueIdentifier => TdsType::Guid,
        ColumnType::NVarChar { max_len } => TdsType::NVarChar((*max_len).max(1)),
        ColumnType::VarChar { max_len } => TdsType::VarChar((*max_len).max(1)),
        ColumnType::VarBinary { max_len } => TdsType::VarBinary((*max_len).max(1)),
        ColumnType::NVarCharMax => TdsType::NVarCharMax,
        ColumnType::VarCharMax => TdsType::VarCharMax,
        ColumnType::VarBinaryMax => TdsType::VarBinaryMax,
        // TIME (rare) falls back to NVARCHAR of its rendered text.
        _ => TdsType::NVarChar(4000),
    }
}

/// Bytes a DECIMAL of `precision` occupies on the wire: 1 sign byte + magnitude
/// (4/8/12/16 bytes by precision bucket).
fn decimal_len(precision: u8) -> u8 {
    match precision {
        0..=9 => 5,
        10..=19 => 9,
        20..=28 => 13,
        _ => 17,
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
        TdsType::Decimal(precision, scale) => {
            out.push(DECIMALN);
            out.push(decimal_len(precision));
            out.push(precision.max(1));
            out.push(scale);
        }
        TdsType::Date => out.push(DATEN),
        TdsType::DateTime2 => {
            out.push(DATETIME2N);
            out.push(7); // 100ns scale
        }
        TdsType::Guid => {
            out.push(GUID);
            out.push(16);
        }
        TdsType::VarBinary(max_len) => {
            out.push(BIGVARBINARY);
            // Clamp below 0xFFFF, which drivers read as VARBINARY(MAX)/PLP.
            out.extend_from_slice(&max_len.min(8000).to_le_bytes());
        }
        TdsType::NVarChar(max_len) => {
            out.push(NVARCHAR);
            // NVARCHAR length is in bytes; UCS-2 doubles the char count.
            out.extend_from_slice(&max_byte_len(max_len).to_le_bytes());
            out.extend_from_slice(&COLLATION);
        }
        TdsType::VarChar(max_len) => {
            out.push(BIGVARCHR);
            // Clamp below 0xFFFF, which drivers read as VARCHAR(MAX)/PLP.
            out.extend_from_slice(&max_len.min(8000).to_le_bytes());
            out.extend_from_slice(&COLLATION);
        }
        // (MAX): the 0xFFFF length IS the PLP marker drivers key on.
        TdsType::NVarCharMax => {
            out.push(NVARCHAR);
            out.extend_from_slice(&0xffffu16.to_le_bytes());
            out.extend_from_slice(&COLLATION);
        }
        TdsType::VarCharMax => {
            out.push(BIGVARCHR);
            out.extend_from_slice(&0xffffu16.to_le_bytes());
            out.extend_from_slice(&COLLATION);
        }
        TdsType::VarBinaryMax => {
            out.push(BIGVARBINARY);
            out.extend_from_slice(&0xffffu16.to_le_bytes());
        }
    }
    out
}

/// PLP (partially length-prefixed) value: total length u64, one data chunk,
/// zero terminator. NULL is the PLP null sentinel.
fn encode_plp(out: &mut Vec<u8>, bytes: Option<Vec<u8>>) {
    match bytes {
        None => out.extend_from_slice(&u64::MAX.to_le_bytes()),
        Some(bytes) => {
            out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            if !bytes.is_empty() {
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(&bytes);
            }
            out.extend_from_slice(&0u32.to_le_bytes());
        }
    }
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
        TdsType::Decimal(precision, _) => match datum {
            Datum::Decimal(unscaled) => {
                let len = decimal_len(precision);
                out.push(len);
                out.push(if *unscaled >= 0 { 1 } else { 0 }); // sign
                let mag = unscaled.unsigned_abs();
                out.extend_from_slice(&mag.to_le_bytes()[..(len - 1) as usize]);
            }
            _ => out.push(0), // NULL
        },
        TdsType::Date => match datum {
            Datum::Date(days) => {
                out.push(3);
                out.extend_from_slice(&days.to_le_bytes()[..3]);
            }
            _ => out.push(0),
        },
        TdsType::DateTime2 => match datum {
            Datum::DateTime2(days, ticks) => {
                out.push(8); // 5 time bytes (scale 7) + 3 date bytes
                out.extend_from_slice(&ticks.to_le_bytes()[..5]);
                out.extend_from_slice(&days.to_le_bytes()[..3]);
            }
            _ => out.push(0),
        },
        TdsType::Guid => match datum {
            Datum::UniqueIdentifier(bytes) => {
                out.push(16);
                out.extend_from_slice(bytes);
            }
            _ => out.push(0),
        },
        TdsType::VarBinary(_) => match datum {
            Datum::VarBinary(bytes) => {
                let capped = &bytes[..bytes.len().min(u16::MAX as usize)];
                out.extend_from_slice(&(capped.len() as u16).to_le_bytes());
                out.extend_from_slice(capped);
            }
            _ => out.extend_from_slice(&0xffffu16.to_le_bytes()),
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
        TdsType::NVarCharMax => encode_plp(
            &mut out,
            string_value(datum).map(|s| s.encode_utf16().flat_map(|u| u.to_le_bytes()).collect()),
        ),
        TdsType::VarCharMax => encode_plp(&mut out, string_value(datum).map(|s| encode_cp1252(&s))),
        TdsType::VarBinaryMax => encode_plp(
            &mut out,
            match datum {
                Datum::VarBinary(bytes) => Some(bytes.clone()),
                _ => None,
            },
        ),
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

    /// (MAX) columns advertise the 0xFFFF PLP marker and their values ride
    /// PLP framing: total u64, one chunk, zero terminator; NULL is the PLP
    /// null sentinel. Byte-pinned — drivers key on exactly these shapes.
    #[test]
    fn max_types_are_plp_on_the_wire() {
        let info = encode_type_info(&ColumnType::NVarCharMax);
        assert_eq!(info[0], NVARCHAR);
        assert_eq!(&info[1..3], &0xffffu16.to_le_bytes());
        assert_eq!(&info[3..8], &COLLATION);

        let value = encode_value(&Datum::NVarChar("hi".into()), &ColumnType::NVarCharMax);
        let mut expected = Vec::new();
        expected.extend_from_slice(&4u64.to_le_bytes()); // total bytes (UCS-2)
        expected.extend_from_slice(&4u32.to_le_bytes()); // one chunk
        expected.extend_from_slice(&[b'h', 0, b'i', 0]);
        expected.extend_from_slice(&0u32.to_le_bytes()); // terminator
        assert_eq!(value, expected);

        assert_eq!(
            encode_value(&Datum::Null, &ColumnType::VarBinaryMax),
            u64::MAX.to_le_bytes().to_vec(),
            "PLP NULL sentinel"
        );

        // An empty value is a zero total with just the terminator.
        assert_eq!(
            encode_value(&Datum::VarChar(String::new()), &ColumnType::VarCharMax),
            [0u64.to_le_bytes().as_slice(), 0u32.to_le_bytes().as_slice()].concat(),
        );
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

    #[test]
    fn decimal_type_info_and_value() {
        let ct = ColumnType::Decimal {
            precision: 10,
            scale: 2,
        };
        // DECIMALN token, max_len 9 (precision bucket 10-19), precision, scale.
        assert_eq!(encode_type_info(&ct), vec![DECIMALN, 9, 10, 2]);
        // 123.45 -> unscaled 12345, positive sign, LE magnitude in 8 bytes.
        let value = encode_value(&Datum::Decimal(12345), &ct);
        assert_eq!(value[0], 9); // length
        assert_eq!(value[1], 1); // sign = positive
        assert_eq!(&value[2..], &12345u64.to_le_bytes());
        // Negative sign byte is 0.
        let neg = encode_value(&Datum::Decimal(-12345), &ct);
        assert_eq!(neg[1], 0);
        assert_eq!(&neg[2..], &12345u64.to_le_bytes());
        // NULL is a zero length.
        assert_eq!(encode_value(&Datum::Null, &ct), vec![0]);
    }

    #[test]
    fn date_and_datetime2_codecs() {
        // DATE: bare token, value = len 3 + 3 LE date bytes.
        assert_eq!(encode_type_info(&ColumnType::Date), vec![DATEN]);
        let d = encode_value(&Datum::Date(738000), &ColumnType::Date);
        assert_eq!(d[0], 3);
        assert_eq!(&d[1..], &738000u32.to_le_bytes()[..3]);

        // DATETIME2: token + scale 7; value = len 8 + 5 time + 3 date.
        assert_eq!(
            encode_type_info(&ColumnType::DateTime2),
            vec![DATETIME2N, 7]
        );
        let dt = encode_value(&Datum::DateTime2(738000, 12345678), &ColumnType::DateTime2);
        assert_eq!(dt[0], 8);
        assert_eq!(&dt[1..6], &12345678u64.to_le_bytes()[..5]);
        assert_eq!(&dt[6..9], &738000u32.to_le_bytes()[..3]);
    }

    #[test]
    fn guid_codec() {
        let bytes = [
            0xff, 0x19, 0x96, 0x6f, 0x86, 0x8b, 0x11, 0xd0, 0xb4, 0x2d, 0x00, 0xc0, 0x4f, 0xc9,
            0x64, 0xff,
        ];
        assert_eq!(
            encode_type_info(&ColumnType::UniqueIdentifier),
            vec![GUID, 16]
        );
        let value = encode_value(
            &Datum::UniqueIdentifier(bytes),
            &ColumnType::UniqueIdentifier,
        );
        assert_eq!(value[0], 16);
        assert_eq!(&value[1..], &bytes);
        assert_eq!(
            encode_value(&Datum::Null, &ColumnType::UniqueIdentifier),
            vec![0]
        );
    }
}
