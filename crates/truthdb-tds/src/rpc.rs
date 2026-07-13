//! TDS RPC request decoding (MS-TDS 2.2.6.6).
//!
//! Covers `sp_executesql` — the RPC every mainstream driver (go-mssqldb, pytds,
//! JDBC, ODBC) uses to run a parameterized query. The request carries the
//! statement text, a parameter-declaration string, and the typed parameter
//! values; this module decodes the values into [`RpcParam`]s the engine seeds
//! as batch variables. Parameters are decoded as typed values, never spliced
//! back into SQL text, so a parameter can never change the statement's shape.
//!
//! `sp_prepare`/`sp_execute`/`sp_prepexec`/`sp_unprepare` (handle-based prepared
//! statements) are a later increment; they surface here as [`RpcProc::Other`].

use std::io;

use truthdb_core::rel::RpcParam;
use truthdb_core::relstore::types::{ColumnType, Datum};

// Data type tokens (MS-TDS 2.2.5.4) — the variable/nullable forms drivers send
// for parameters, plus DATE/DATETIME2/GUID/DECIMAL.
const INTN: u8 = 0x26;
const BITN: u8 = 0x68;
const FLTN: u8 = 0x6d;
const DECIMALN: u8 = 0x6a;
const NUMERICN: u8 = 0x6c;
const DATEN: u8 = 0x28;
const DATETIME2N: u8 = 0x2a;
const GUID: u8 = 0x24;
const NVARCHAR: u8 = 0xe7;
const NCHAR: u8 = 0xef;
const BIGVARCHR: u8 = 0xa7;
const BIGCHAR: u8 = 0xaf;
const BIGVARBINARY: u8 = 0xa5;

/// A `USHORT` charbin length that means NVARCHAR(MAX)/VARCHAR(MAX)/VARBINARY(MAX)
/// — the value arrives PLP-chunked rather than length-prefixed.
const PLP_MARKER: u16 = 0xffff;
/// A `USHORT` value length meaning the value is NULL (non-PLP charbin).
const CHARBIN_NULL: u16 = 0xffff;
/// A PLP total length meaning the whole value is NULL.
const PLP_NULL: u64 = 0xffff_ffff_ffff_ffff;

/// The well-known `ProcID` for `sp_executesql` (MS-TDS 2.2.6.6).
const SP_EXECUTESQL_PROCID: u16 = 10;
/// Sentinel `NameLenProcID` length selecting a well-known `ProcID` instead of a
/// procedure name.
const PROCID_SENTINEL: u16 = 0xffff;

/// Which procedure an RPC targets.
pub enum RpcProc {
    /// `sp_executesql` (by ProcID 10 or by name).
    SpExecuteSql,
    /// Any other procedure — reported back to the client as unsupported.
    Other(String),
}

/// A decoded RPC request: the target procedure and its ordered parameters.
pub struct RpcRequest {
    pub proc: RpcProc,
    pub params: Vec<RpcParam>,
}

/// Decodes an RPC request body (the bytes *after* the ALL_HEADERS block).
pub fn parse_rpc_request(body: &[u8]) -> io::Result<RpcRequest> {
    let mut c = Cursor::new(body);
    // NameLenProcID: a US_VARCHAR proc name, or 0xFFFF + a well-known ProcID.
    let name_len = c.u16()?;
    let proc = if name_len == PROCID_SENTINEL {
        let procid = c.u16()?;
        if procid == SP_EXECUTESQL_PROCID {
            RpcProc::SpExecuteSql
        } else {
            RpcProc::Other(format!("ProcID #{procid}"))
        }
    } else {
        let name = c.utf16(name_len as usize * 2)?;
        if name.eq_ignore_ascii_case("sp_executesql") {
            RpcProc::SpExecuteSql
        } else {
            RpcProc::Other(name)
        }
    };
    let _option_flags = c.u16()?;

    let mut params = Vec::new();
    while c.remaining() > 0 {
        // A batch flag (0x80 = fWithRecompile / 0xFF = separator) begins the
        // next RPC in a multi-RPC request; we run the first and stop there.
        let lead = c.peek_u8();
        if lead == 0xff || lead == 0x80 {
            break;
        }
        let name_len = c.u8()? as usize;
        let name = c.utf16(name_len * 2)?;
        let _status = c.u8()?; // StatusFlags: fByRefValue / fDefaultValue.
        let (column_type, value) = decode_param(&mut c)?;
        params.push(RpcParam {
            name,
            column_type,
            value,
        });
    }
    Ok(RpcRequest { proc, params })
}

/// Splits an `sp_executesql` parameter list into (statement text, value
/// parameters). Layout: `@stmt`, optional `@params` declaration, then the
/// value parameters — which are the only ones seeded as batch variables.
pub fn split_sp_executesql(mut params: Vec<RpcParam>) -> io::Result<(String, Vec<RpcParam>)> {
    if params.is_empty() {
        return Err(protocol_err("sp_executesql: missing statement parameter"));
    }
    let values = params.split_off(params.len().min(2));
    let stmt = match &params[0].value {
        Datum::NVarChar(s) | Datum::VarChar(s) => s.clone(),
        Datum::Null => return Err(protocol_err("sp_executesql: NULL statement")),
        _ => return Err(protocol_err("sp_executesql: statement is not a string")),
    };
    Ok((stmt, values))
}

/// Decodes one parameter's TYPE_INFO and value into a column type + datum.
fn decode_param(c: &mut Cursor) -> io::Result<(ColumnType, Datum)> {
    let token = c.u8()?;
    match token {
        INTN => {
            let max_len = c.u8()?;
            let column_type = int_type(max_len)?;
            let value = match c.u8()? as usize {
                0 => Datum::Null,
                1 => Datum::TinyInt(c.u8()?),
                2 => Datum::SmallInt(i16::from_le_bytes(c.array::<2>()?)),
                4 => Datum::Int(i32::from_le_bytes(c.array::<4>()?)),
                8 => Datum::BigInt(i64::from_le_bytes(c.array::<8>()?)),
                n => return Err(protocol_err(&format!("INTN bad value length {n}"))),
            };
            Ok((column_type, value))
        }
        BITN => {
            let _max_len = c.u8()?;
            let value = match c.u8()? {
                0 => Datum::Null,
                _ => Datum::Bit(c.u8()? != 0),
            };
            Ok((ColumnType::Bit, value))
        }
        FLTN => {
            let max_len = c.u8()?;
            let column_type = match max_len {
                4 => ColumnType::Real,
                8 => ColumnType::Float,
                n => return Err(protocol_err(&format!("FLTN bad max length {n}"))),
            };
            let value = match c.u8()? as usize {
                0 => Datum::Null,
                4 => Datum::Real(f32::from_le_bytes(c.array::<4>()?)),
                8 => Datum::Float(f64::from_le_bytes(c.array::<8>()?)),
                n => return Err(protocol_err(&format!("FLTN bad value length {n}"))),
            };
            Ok((column_type, value))
        }
        DECIMALN | NUMERICN => {
            let _max_len = c.u8()?;
            let precision = c.u8()?;
            let scale = c.u8()?;
            let value = match c.u8()? as usize {
                0 => Datum::Null,
                // 1 sign byte + at most a 16-byte magnitude (an i128); a longer
                // length is malformed and would overflow the shift below.
                len if len <= 17 => {
                    let sign = c.u8()?;
                    let mag = c.bytes(len - 1)?;
                    let mut acc: i128 = 0;
                    for (i, byte) in mag.iter().enumerate() {
                        acc |= (*byte as i128) << (8 * i);
                    }
                    Datum::Decimal(if sign == 0 { -acc } else { acc })
                }
                len => return Err(protocol_err(&format!("DECIMALN bad value length {len}"))),
            };
            Ok((ColumnType::Decimal { precision, scale }, value))
        }
        DATEN => {
            let value = match c.u8()? {
                0 => Datum::Null,
                _ => Datum::Date(u32::from_le_bytes(c.array_padded::<3, 4>()?)),
            };
            Ok((ColumnType::Date, value))
        }
        DATETIME2N => {
            let scale = c.u8()?;
            let value = match c.u8()? as usize {
                0 => Datum::Null,
                // A DATETIME2 body is a 3/4/5-byte time field (by scale) plus a
                // 3-byte date = total 6..=8. Anything else is malformed; without
                // this bound `total - 3` and the tick scaling could overflow.
                total if (6..=8).contains(&total) => {
                    let raw = c.uint_le(total - 3)?;
                    // The wire value is in 10^-scale-second units; the datum
                    // stores 100ns (10^-7-second) ticks. With a valid time field
                    // (<= 5 bytes) this product cannot overflow, but saturate
                    // defensively rather than wrap.
                    let ticks = raw.saturating_mul(10u64.pow(7u32.saturating_sub(scale as u32)));
                    let days = u32::from_le_bytes(c.array_padded::<3, 4>()?);
                    Datum::DateTime2(days, ticks)
                }
                total => {
                    return Err(protocol_err(&format!(
                        "DATETIME2N bad value length {total}"
                    )));
                }
            };
            Ok((ColumnType::DateTime2, value))
        }
        GUID => {
            let _max_len = c.u8()?;
            let value = match c.u8()? {
                0 => Datum::Null,
                _ => Datum::UniqueIdentifier(c.array::<16>()?),
            };
            Ok((ColumnType::UniqueIdentifier, value))
        }
        NVARCHAR | NCHAR => {
            let max_len = c.u16()?;
            c.skip(5)?; // collation
            let column_type = ColumnType::NVarChar {
                max_len: (max_len / 2).max(1),
            };
            let value = match charbin_bytes(c, max_len)? {
                Some(bytes) => Datum::NVarChar(utf16le_to_string(&bytes)),
                None => Datum::Null,
            };
            Ok((column_type, value))
        }
        BIGVARCHR | BIGCHAR => {
            let max_len = c.u16()?;
            c.skip(5)?; // collation
            let column_type = ColumnType::VarChar {
                max_len: max_len.max(1),
            };
            let value = match charbin_bytes(c, max_len)? {
                Some(bytes) => Datum::VarChar(cp1252_to_string(&bytes)),
                None => Datum::Null,
            };
            Ok((column_type, value))
        }
        BIGVARBINARY => {
            let max_len = c.u16()?;
            let column_type = ColumnType::VarBinary {
                max_len: max_len.max(1),
            };
            let value = match charbin_bytes(c, max_len)? {
                Some(bytes) => Datum::VarBinary(bytes),
                None => Datum::Null,
            };
            Ok((column_type, value))
        }
        other => Err(protocol_err(&format!(
            "unsupported RPC parameter type token 0x{other:02x}"
        ))),
    }
}

/// The ColumnType for an INTN of the given declared byte width.
fn int_type(max_len: u8) -> io::Result<ColumnType> {
    match max_len {
        1 => Ok(ColumnType::TinyInt),
        2 => Ok(ColumnType::SmallInt),
        4 => Ok(ColumnType::Int),
        8 => Ok(ColumnType::BigInt),
        n => Err(protocol_err(&format!("INTN bad max length {n}"))),
    }
}

/// Reads a charbin (NVARCHAR/VARCHAR/VARBINARY) value body: either a PLP stream
/// (declared max length 0xFFFF) or a `USHORT`-length-prefixed run. Returns the
/// raw bytes, or `None` for NULL.
fn charbin_bytes(c: &mut Cursor, max_len: u16) -> io::Result<Option<Vec<u8>>> {
    if max_len == PLP_MARKER {
        return plp_bytes(c);
    }
    let len = c.u16()?;
    if len == CHARBIN_NULL {
        return Ok(None);
    }
    Ok(Some(c.bytes(len as usize)?.to_vec()))
}

/// Reads a PLP-encoded value (MS-TDS 2.2.5.2.3): an 8-byte total length, then
/// length-prefixed chunks terminated by a zero-length chunk.
fn plp_bytes(c: &mut Cursor) -> io::Result<Option<Vec<u8>>> {
    let total = c.u64()?;
    if total == PLP_NULL {
        return Ok(None);
    }
    let mut out = Vec::new();
    loop {
        let chunk = c.u32()? as usize;
        if chunk == 0 {
            break;
        }
        out.extend_from_slice(c.bytes(chunk)?);
    }
    Ok(Some(out))
}

fn utf16le_to_string(bytes: &[u8]) -> String {
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    String::from_utf16_lossy(&units)
}

/// Decodes Windows-1252 bytes to a string — the inverse of the encoder in
/// `typeinfo`. 0x00–0x7F and 0xA0–0xFF are Latin-1 (== Unicode); 0x80–0x9F map
/// to specific punctuation/symbols, with the five unassigned slots replaced.
fn cp1252_to_string(bytes: &[u8]) -> String {
    bytes.iter().map(|b| cp1252_char(*b)).collect()
}

fn cp1252_char(b: u8) -> char {
    match b {
        0x80 => '\u{20AC}',
        0x82 => '\u{201A}',
        0x83 => '\u{0192}',
        0x84 => '\u{201E}',
        0x85 => '\u{2026}',
        0x86 => '\u{2020}',
        0x87 => '\u{2021}',
        0x88 => '\u{02C6}',
        0x89 => '\u{2030}',
        0x8A => '\u{0160}',
        0x8B => '\u{2039}',
        0x8C => '\u{0152}',
        0x8E => '\u{017D}',
        0x91 => '\u{2018}',
        0x92 => '\u{2019}',
        0x93 => '\u{201C}',
        0x94 => '\u{201D}',
        0x95 => '\u{2022}',
        0x96 => '\u{2013}',
        0x97 => '\u{2014}',
        0x98 => '\u{02DC}',
        0x99 => '\u{2122}',
        0x9A => '\u{0161}',
        0x9B => '\u{203A}',
        0x9C => '\u{0153}',
        0x9E => '\u{017E}',
        0x9F => '\u{0178}',
        0x81 | 0x8D | 0x8F | 0x90 | 0x9D => '\u{FFFD}',
        other => other as char,
    }
}

fn protocol_err(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

/// A little-endian byte cursor over an RPC body. Every read is bounds-checked
/// and returns an `InvalidData` error on underflow, so a truncated or malformed
/// request fails cleanly instead of panicking.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn peek_u8(&self) -> u8 {
        self.buf.get(self.pos).copied().unwrap_or(0)
    }

    fn bytes(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|end| *end <= self.buf.len())
            .ok_or_else(|| protocol_err("RPC request truncated"))?;
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn skip(&mut self, n: usize) -> io::Result<()> {
        self.bytes(n).map(|_| ())
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.array::<2>()?))
    }

    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.array::<4>()?))
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.array::<8>()?))
    }

    /// Reads `n` (<= 8) little-endian bytes into a u64. A wider request is
    /// rejected rather than shifting past the width of a u64.
    fn uint_le(&mut self, n: usize) -> io::Result<u64> {
        if n > 8 {
            return Err(protocol_err("integer field wider than 8 bytes"));
        }
        let mut acc = 0u64;
        for (i, byte) in self.bytes(n)?.iter().enumerate() {
            acc |= (*byte as u64) << (8 * i);
        }
        Ok(acc)
    }

    fn array<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.bytes(N)?);
        Ok(out)
    }

    /// Reads `N` bytes into the low bytes of an `M`-byte array (M > N), leaving
    /// the high bytes zero — for 3-byte DATE/DATETIME2 fields read as a u32.
    fn array_padded<const N: usize, const M: usize>(&mut self) -> io::Result<[u8; M]> {
        let mut out = [0u8; M];
        out[..N].copy_from_slice(self.bytes(N)?);
        Ok(out)
    }

    /// Reads `byte_count` bytes as a UTF-16LE string.
    fn utf16(&mut self, byte_count: usize) -> io::Result<String> {
        Ok(utf16le_to_string(self.bytes(byte_count)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a minimal `sp_executesql` RPC body (ProcID form) with a single
    /// `@p1 INT` value parameter, and an empty `@params` declaration.
    fn sample_body() -> Vec<u8> {
        let mut b = Vec::new();
        // NameLenProcID: 0xFFFF + ProcID 10.
        b.extend_from_slice(&PROCID_SENTINEL.to_le_bytes());
        b.extend_from_slice(&SP_EXECUTESQL_PROCID.to_le_bytes());
        b.extend_from_slice(&0u16.to_le_bytes()); // OptionFlags

        // Param 0: @stmt = N'SELECT @p1' (NVARCHAR).
        push_nvarchar_param(&mut b, "", "SELECT @p1");
        // Param 1: @params = N'@p1 int' (NVARCHAR).
        push_nvarchar_param(&mut b, "@params", "@p1 int");
        // Param 2: @p1 = 258 (INTN, 4 bytes).
        push_name(&mut b, "@p1");
        b.push(0x00); // status
        b.push(INTN);
        b.push(4); // max len
        b.push(4); // value len
        b.extend_from_slice(&258i32.to_le_bytes());
        b
    }

    fn push_name(b: &mut Vec<u8>, name: &str) {
        let units: Vec<u16> = name.encode_utf16().collect();
        b.push(units.len() as u8);
        for u in units {
            b.extend_from_slice(&u.to_le_bytes());
        }
    }

    fn push_nvarchar_param(b: &mut Vec<u8>, name: &str, value: &str) {
        push_name(b, name);
        b.push(0x00); // status
        b.push(NVARCHAR);
        b.extend_from_slice(&8000u16.to_le_bytes()); // max len
        b.extend_from_slice(&[0x09, 0x04, 0xd0, 0x00, 0x34]); // collation
        let bytes: Vec<u8> = value.encode_utf16().flat_map(|u| u.to_le_bytes()).collect();
        b.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
        b.extend_from_slice(&bytes);
    }

    #[test]
    fn decodes_sp_executesql_with_int_param() {
        let req = parse_rpc_request(&sample_body()).expect("parse");
        assert!(matches!(req.proc, RpcProc::SpExecuteSql));
        assert_eq!(req.params.len(), 3);

        let (sql, values) = split_sp_executesql(req.params).expect("split");
        assert_eq!(sql, "SELECT @p1");
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].name, "@p1");
        assert!(matches!(values[0].column_type, ColumnType::Int));
        assert!(matches!(values[0].value, Datum::Int(258)));
    }

    #[test]
    fn decodes_null_and_string_and_bigint_params() {
        let mut b = Vec::new();
        b.extend_from_slice(&PROCID_SENTINEL.to_le_bytes());
        b.extend_from_slice(&SP_EXECUTESQL_PROCID.to_le_bytes());
        b.extend_from_slice(&0u16.to_le_bytes());
        push_nvarchar_param(&mut b, "", "stmt");
        push_nvarchar_param(&mut b, "@params", "decl");
        // @s = N'café' (NVARCHAR).
        push_nvarchar_param(&mut b, "@s", "café");
        // @big = 5_000_000_000 (INTN, 8 bytes).
        push_name(&mut b, "@big");
        b.push(0x00);
        b.push(INTN);
        b.push(8);
        b.push(8);
        b.extend_from_slice(&5_000_000_000i64.to_le_bytes());
        // @n = NULL INT.
        push_name(&mut b, "@n");
        b.push(0x00);
        b.push(INTN);
        b.push(4);
        b.push(0); // NULL

        let req = parse_rpc_request(&b).expect("parse");
        let (_sql, values) = split_sp_executesql(req.params).expect("split");
        assert_eq!(values.len(), 3);
        assert!(matches!(&values[0].value, Datum::NVarChar(s) if s == "café"));
        assert!(matches!(values[1].value, Datum::BigInt(5_000_000_000)));
        assert!(matches!(values[2].value, Datum::Null));
    }

    #[test]
    fn unknown_proc_reports_other() {
        let mut b = Vec::new();
        b.extend_from_slice(&PROCID_SENTINEL.to_le_bytes());
        b.extend_from_slice(&11u16.to_le_bytes()); // sp_prepare
        b.extend_from_slice(&0u16.to_le_bytes());
        let req = parse_rpc_request(&b).expect("parse");
        assert!(matches!(req.proc, RpcProc::Other(_)));
    }

    #[test]
    fn truncated_body_errors_not_panics() {
        // ProcID form then a param cut off mid-value.
        let mut b = Vec::new();
        b.extend_from_slice(&PROCID_SENTINEL.to_le_bytes());
        b.extend_from_slice(&SP_EXECUTESQL_PROCID.to_le_bytes());
        b.extend_from_slice(&0u16.to_le_bytes());
        push_name(&mut b, "@p1");
        b.push(0x00);
        b.push(INTN);
        b.push(4);
        b.push(4); // claims 4 value bytes...
        b.extend_from_slice(&[0x01, 0x02]); // ...only 2 present
        assert!(parse_rpc_request(&b).is_err());
    }

    /// A malformed value length must yield an error, never an arithmetic-overflow
    /// panic in the length-driven decoders. These bodies would panic (debug) or
    /// silently mis-decode (release) without the range checks.
    #[test]
    fn malformed_lengths_error_not_panic() {
        let header = |token: &[u8]| {
            let mut b = Vec::new();
            b.extend_from_slice(&PROCID_SENTINEL.to_le_bytes());
            b.extend_from_slice(&SP_EXECUTESQL_PROCID.to_le_bytes());
            b.extend_from_slice(&0u16.to_le_bytes());
            push_name(&mut b, "@p");
            b.push(0x00); // status
            b.extend_from_slice(token);
            b
        };

        // DATETIME2N with value length 1 (< the minimum 6) — `total - 3` would
        // underflow.
        let mut short_dt2 = header(&[DATETIME2N, 0x00]); // scale 0
        short_dt2.push(1); // value length 1
        short_dt2.push(0xAA);
        assert!(parse_rpc_request(&short_dt2).is_err());

        // DATETIME2N with value length 12 (time field 9 bytes) — `uint_le(9)`
        // would shift past a u64.
        let mut long_dt2 = header(&[DATETIME2N, 0x00]);
        long_dt2.push(12);
        long_dt2.extend_from_slice(&[0xFF; 12]);
        assert!(parse_rpc_request(&long_dt2).is_err());

        // DECIMALN with value length 18 (17-byte magnitude) — the i128 shift
        // would reach `<< 128`.
        let mut big_dec = header(&[DECIMALN, 0x00, 0x00, 0x00]); // max_len, prec, scale
        big_dec.push(18); // value length
        big_dec.push(0x01); // sign
        big_dec.extend_from_slice(&[0x00; 17]);
        assert!(parse_rpc_request(&big_dec).is_err());
    }
}
