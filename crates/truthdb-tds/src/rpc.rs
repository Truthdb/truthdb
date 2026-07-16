//! TDS RPC request decoding (MS-TDS 2.2.6.6).
//!
//! Covers `sp_executesql` — the RPC every mainstream driver (go-mssqldb, pytds,
//! JDBC, ODBC) uses to run a parameterized query. The request carries the
//! statement text, a parameter-declaration string, and the typed parameter
//! values; this module decodes the values into [`RpcParam`]s the engine seeds
//! as batch variables. Parameters are decoded as typed values, never spliced
//! back into SQL text, so a parameter can never change the statement's shape.
//!
//! `sp_prepare`/`sp_execute`/`sp_prepexec`/`sp_unprepare` (handle-based
//! prepared statements) dispatch by well-known ProcID or by name; `sp_cursor*`
//! is recognized so the server can reject server-side cursors distinctly.

use std::io;

use truthdb_core::rel::RpcParam;
use truthdb_core::relstore::types::{ColumnType, Datum};

// Data type tokens (MS-TDS 2.2.5.4) — the variable/nullable forms drivers send
// for parameters, plus DATE/DATETIME2/GUID/DECIMAL.
/// A typeless NULL: no metadata, no value body (go-mssqldb sends a bare `nil`
/// parameter this way).
const NULLTYPE: u8 = 0x1f;
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
/// Well-known `ProcID`s for the prepared-statement handle family.
const SP_PREPARE_PROCID: u16 = 11;
const SP_EXECUTE_PROCID: u16 = 12;
const SP_PREPEXEC_PROCID: u16 = 13;
const SP_UNPREPARE_PROCID: u16 = 15;
/// The `sp_cursor*` family occupies ProcIDs 1–9 (sp_cursor through
/// sp_cursorunprepare) — recognized so they can be rejected politely.
const SP_CURSOR_FIRST_PROCID: u16 = 1;
const SP_CURSOR_LAST_PROCID: u16 = 9;
/// Sentinel `NameLenProcID` length selecting a well-known `ProcID` instead of a
/// procedure name.
const PROCID_SENTINEL: u16 = 0xffff;

/// Which procedure an RPC targets.
pub enum RpcProc {
    /// `sp_executesql` (by ProcID 10 or by name).
    SpExecuteSql,
    /// `sp_prepare` (ProcID 11): store a statement, return a handle.
    SpPrepare,
    /// `sp_execute` (ProcID 12): run a prepared handle with values.
    SpExecute,
    /// `sp_prepexec` (ProcID 13): prepare and run in one round trip.
    SpPrepExec,
    /// `sp_unprepare` (ProcID 15): drop a prepared handle.
    SpUnprepare,
    /// `sp_describe_first_result_set` (by name): metadata discovery.
    SpDescribeFirstResultSet,
    /// An `sp_cursor*` procedure — server-side cursors are not supported, and
    /// say so distinctly rather than "not found".
    SpCursor(String),
    /// Any other procedure — reported back to the client as unsupported.
    Other(String),
}

/// A decoded RPC request: the target procedure and its ordered parameters.
pub struct RpcRequest {
    pub proc: RpcProc,
    pub params: Vec<RpcParam>,
}

/// Decodes an RPC request body (the bytes *after* the ALL_HEADERS block).
/// A request may carry several RPCs separated by batch flags (0xFF, or 0x80
/// from older TDS versions) — mssql-jdbc batches sp_unprepare calls with the
/// next execution's sp_prepexec this way. Each runs in order and answers its
/// own DONEPROC-framed reply within the one response.
pub fn parse_rpc_requests(body: &[u8]) -> io::Result<Vec<RpcRequest>> {
    let mut c = Cursor::new(body);
    let mut requests = Vec::new();
    loop {
        requests.push(parse_one_rpc(&mut c)?);
        if c.remaining() == 0 {
            return Ok(requests);
        }
        // The separator byte; the next RPC's NameLenProcID follows.
        let _flag = c.u8()?;
        if c.remaining() == 0 {
            // A trailing separator with nothing after it.
            return Ok(requests);
        }
    }
}

/// Decodes one RPC from the cursor, stopping at a batch separator.
fn parse_one_rpc(c: &mut Cursor) -> io::Result<RpcRequest> {
    // NameLenProcID: a US_VARCHAR proc name, or 0xFFFF + a well-known ProcID.
    let name_len = c.u16()?;
    let proc = if name_len == PROCID_SENTINEL {
        let procid = c.u16()?;
        match procid {
            SP_EXECUTESQL_PROCID => RpcProc::SpExecuteSql,
            SP_PREPARE_PROCID => RpcProc::SpPrepare,
            SP_EXECUTE_PROCID => RpcProc::SpExecute,
            SP_PREPEXEC_PROCID => RpcProc::SpPrepExec,
            SP_UNPREPARE_PROCID => RpcProc::SpUnprepare,
            SP_CURSOR_FIRST_PROCID..=SP_CURSOR_LAST_PROCID => {
                RpcProc::SpCursor(format!("ProcID #{procid}"))
            }
            _ => RpcProc::Other(format!("ProcID #{procid}")),
        }
    } else {
        let name = c.utf16(name_len as usize * 2)?;
        if name.eq_ignore_ascii_case("sp_executesql") {
            RpcProc::SpExecuteSql
        } else if name.eq_ignore_ascii_case("sp_prepare") {
            RpcProc::SpPrepare
        } else if name.eq_ignore_ascii_case("sp_execute") {
            RpcProc::SpExecute
        } else if name.eq_ignore_ascii_case("sp_prepexec") {
            RpcProc::SpPrepExec
        } else if name.eq_ignore_ascii_case("sp_unprepare") {
            RpcProc::SpUnprepare
        } else if name.eq_ignore_ascii_case("sp_describe_first_result_set") {
            RpcProc::SpDescribeFirstResultSet
        } else if name.to_ascii_lowercase().starts_with("sp_cursor") {
            RpcProc::SpCursor(name)
        } else {
            RpcProc::Other(name)
        }
    };
    let _option_flags = c.u16()?;

    let mut params = Vec::new();
    while c.remaining() > 0 {
        // A batch flag (0x80 = old-style / 0xFF = separator) begins the next
        // RPC in a multi-RPC request; the caller consumes it and loops.
        let lead = c.peek_u8();
        if lead == 0xff || lead == 0x80 {
            break;
        }
        let name_len = c.u8()? as usize;
        let name = c.utf16(name_len * 2)?;
        let _status = c.u8()?; // StatusFlags: fByRefValue / fDefaultValue.
        let (column_type, value) = decode_param(c)?;
        params.push(RpcParam {
            name,
            column_type,
            value,
        });
    }
    Ok(RpcRequest { proc, params })
}

/// Decodes a body expected to hold exactly one RPC (tests' shorthand).
#[cfg(test)]
pub fn parse_rpc_request(body: &[u8]) -> io::Result<RpcRequest> {
    let mut requests = parse_rpc_requests(body)?;
    Ok(requests.remove(0))
}

/// Splits an `sp_executesql` parameter list into (statement text, parameter
/// declarations, value parameters). Layout: `@stmt`, optional `@params`
/// declaration, then the values — which seed batch variables, named from the
/// declaration list when the driver sends them unnamed (mssql-jdbc does;
/// pytds and go-mssqldb name theirs).
pub fn split_sp_executesql(
    mut params: Vec<RpcParam>,
) -> io::Result<(String, String, Vec<RpcParam>)> {
    if params.is_empty() {
        return Err(protocol_err("sp_executesql: missing statement parameter"));
    }
    let values = params.split_off(params.len().min(2));
    let stmt = match &params[0].value {
        Datum::NVarChar(s) | Datum::VarChar(s) => s.clone(),
        Datum::Null => return Err(protocol_err("sp_executesql: NULL statement")),
        _ => return Err(protocol_err("sp_executesql: statement is not a string")),
    };
    let decls = match params.get(1) {
        Some(p) => opt_string(p, "sp_executesql: @params")?,
        None => String::new(),
    };
    Ok((stmt, decls, values))
}

/// Reads a string parameter that may be NULL (an empty declaration list).
fn opt_string(param: &RpcParam, what: &str) -> io::Result<String> {
    match &param.value {
        Datum::NVarChar(s) | Datum::VarChar(s) => Ok(s.clone()),
        Datum::Null => Ok(String::new()),
        _ => Err(protocol_err(&format!("{what} is not a string"))),
    }
}

/// Reads a required integer parameter (a prepared-statement handle).
fn int_param(param: &RpcParam, what: &str) -> io::Result<i32> {
    match param.value {
        Datum::Int(v) => Ok(v),
        Datum::SmallInt(v) => Ok(v as i32),
        Datum::TinyInt(v) => Ok(v as i32),
        Datum::BigInt(v) => {
            i32::try_from(v).map_err(|_| protocol_err(&format!("{what} out of range: {v}")))
        }
        _ => Err(protocol_err(&format!("{what} is not an integer"))),
    }
}

/// Splits an `sp_prepare` parameter list — `@handle` OUTPUT (ignored on input),
/// `@params` declarations, `@stmt`, optional `@options` — into (declarations,
/// statement text).
pub fn split_sp_prepare(params: Vec<RpcParam>) -> io::Result<(String, String)> {
    if params.len() < 3 {
        return Err(protocol_err("sp_prepare: expected @handle, @params, @stmt"));
    }
    let decls = opt_string(&params[1], "sp_prepare: @params")?;
    let stmt = match opt_string(&params[2], "sp_prepare: @stmt")? {
        s if s.is_empty() => return Err(protocol_err("sp_prepare: NULL statement")),
        s => s,
    };
    Ok((decls, stmt))
}

/// Splits an `sp_execute` parameter list — `@handle`, then the value
/// parameters — into (handle, values).
pub fn split_sp_execute(mut params: Vec<RpcParam>) -> io::Result<(i32, Vec<RpcParam>)> {
    if params.is_empty() {
        return Err(protocol_err("sp_execute: missing @handle"));
    }
    let values = params.split_off(1);
    let handle = int_param(&params[0], "sp_execute: @handle")?;
    Ok((handle, values))
}

/// Splits an `sp_prepexec` parameter list — `@handle` OUTPUT (ignored on
/// input), `@params` declarations, `@stmt`, then the value parameters — into
/// (declarations, statement text, values).
pub fn split_sp_prepexec(mut params: Vec<RpcParam>) -> io::Result<(String, String, Vec<RpcParam>)> {
    if params.len() < 3 {
        return Err(protocol_err(
            "sp_prepexec: expected @handle, @params, @stmt",
        ));
    }
    let values = params.split_off(3);
    let decls = opt_string(&params[1], "sp_prepexec: @params")?;
    let stmt = match opt_string(&params[2], "sp_prepexec: @stmt")? {
        s if s.is_empty() => return Err(protocol_err("sp_prepexec: NULL statement")),
        s => s,
    };
    Ok((decls, stmt, values))
}

/// Splits an `sp_describe_first_result_set` parameter list — `@tsql`, then
/// optional `@params`/`@browse_information_mode` (both ignored: declared
/// parameters need no binding to derive columns) — into the statement text.
/// An empty `@tsql` is a valid empty batch (describes as zero rows); only a
/// NULL one is rejected.
pub fn split_sp_describe(params: Vec<RpcParam>) -> io::Result<String> {
    if params.is_empty() {
        return Err(protocol_err("sp_describe_first_result_set: missing @tsql"));
    }
    match &params[0].value {
        Datum::NVarChar(s) | Datum::VarChar(s) => Ok(s.clone()),
        Datum::Null => Err(protocol_err("sp_describe_first_result_set: NULL @tsql")),
        _ => Err(protocol_err(
            "sp_describe_first_result_set: @tsql is not a string",
        )),
    }
}

/// Splits an `sp_unprepare` parameter list into its handle.
pub fn split_sp_unprepare(params: Vec<RpcParam>) -> io::Result<i32> {
    if params.is_empty() {
        return Err(protocol_err("sp_unprepare: missing @handle"));
    }
    int_param(&params[0], "sp_unprepare: @handle")
}

/// Decodes one parameter's TYPE_INFO and value into a column type + datum.
fn decode_param(c: &mut Cursor) -> io::Result<(ColumnType, Datum)> {
    let token = c.u8()?;
    match token {
        // A typeless NULL carries no metadata and no value. The stored column
        // type is unused (the value coerces to NULL wherever it is read).
        NULLTYPE => Ok((ColumnType::Int, Datum::Null)),
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
    fn a_multi_rpc_body_parses_each_rpc() {
        let mut b = Vec::new();
        b.extend_from_slice(&PROCID_SENTINEL.to_le_bytes());
        b.extend_from_slice(&SP_EXECUTESQL_PROCID.to_le_bytes());
        b.extend_from_slice(&0u16.to_le_bytes());
        push_nvarchar_param(&mut b, "", "SELECT 1 AS a");
        b.push(0xff); // batch separator
        b.extend_from_slice(&PROCID_SENTINEL.to_le_bytes());
        b.extend_from_slice(&SP_EXECUTESQL_PROCID.to_le_bytes());
        b.extend_from_slice(&0u16.to_le_bytes());
        push_nvarchar_param(&mut b, "", "SELECT 2 AS b");

        let requests = parse_rpc_requests(&b).expect("parse");
        assert_eq!(requests.len(), 2);
        for (request, want) in requests.into_iter().zip(["SELECT 1 AS a", "SELECT 2 AS b"]) {
            assert!(matches!(request.proc, RpcProc::SpExecuteSql));
            let (sql, _decls, values) = split_sp_executesql(request.params).expect("split");
            assert_eq!(sql, want);
            assert!(values.is_empty());
        }
    }

    #[test]
    fn decodes_sp_executesql_with_int_param() {
        let req = parse_rpc_request(&sample_body()).expect("parse");
        assert!(matches!(req.proc, RpcProc::SpExecuteSql));
        assert_eq!(req.params.len(), 3);

        let (sql, _decls, values) = split_sp_executesql(req.params).expect("split");
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
        let (_sql, _decls, values) = split_sp_executesql(req.params).expect("split");
        assert_eq!(values.len(), 3);
        assert!(matches!(&values[0].value, Datum::NVarChar(s) if s == "café"));
        assert!(matches!(values[1].value, Datum::BigInt(5_000_000_000)));
        assert!(matches!(values[2].value, Datum::Null));
    }

    #[test]
    fn decodes_bare_nulltype_param() {
        // go-mssqldb sends a `nil` parameter as a typeless NULL (0x1F): the
        // token alone, with no metadata and no value body.
        let mut b = Vec::new();
        b.extend_from_slice(&PROCID_SENTINEL.to_le_bytes());
        b.extend_from_slice(&SP_EXECUTESQL_PROCID.to_le_bytes());
        b.extend_from_slice(&0u16.to_le_bytes());
        push_nvarchar_param(&mut b, "", "stmt");
        push_nvarchar_param(&mut b, "@params", "decl");
        push_name(&mut b, "@p1");
        b.push(0x00); // status
        b.push(NULLTYPE); // token, nothing follows
        // A second param after it must still parse (no over-read of NULLTYPE).
        push_name(&mut b, "@p2");
        b.push(0x00);
        b.push(INTN);
        b.push(4);
        b.push(4);
        b.extend_from_slice(&7i32.to_le_bytes());

        let req = parse_rpc_request(&b).expect("parse");
        let (_sql, _decls, values) = split_sp_executesql(req.params).expect("split");
        assert_eq!(values.len(), 2);
        assert!(matches!(values[0].value, Datum::Null));
        assert!(matches!(values[1].value, Datum::Int(7)));
    }

    #[test]
    fn unknown_proc_reports_other() {
        let mut b = Vec::new();
        b.extend_from_slice(&PROCID_SENTINEL.to_le_bytes());
        b.extend_from_slice(&200u16.to_le_bytes()); // no such well-known ProcID
        b.extend_from_slice(&0u16.to_le_bytes());
        let req = parse_rpc_request(&b).expect("parse");
        assert!(matches!(req.proc, RpcProc::Other(_)));
    }

    #[test]
    fn the_handle_family_and_cursors_dispatch_by_procid_and_name() {
        let by_procid = |procid: u16| {
            let mut b = Vec::new();
            b.extend_from_slice(&PROCID_SENTINEL.to_le_bytes());
            b.extend_from_slice(&procid.to_le_bytes());
            b.extend_from_slice(&0u16.to_le_bytes());
            parse_rpc_request(&b).expect("parse").proc
        };
        assert!(matches!(by_procid(11), RpcProc::SpPrepare));
        assert!(matches!(by_procid(12), RpcProc::SpExecute));
        assert!(matches!(by_procid(13), RpcProc::SpPrepExec));
        assert!(matches!(by_procid(15), RpcProc::SpUnprepare));
        assert!(matches!(by_procid(2), RpcProc::SpCursor(_))); // sp_cursoropen

        let by_name = |name: &str| {
            let mut b = Vec::new();
            b.extend_from_slice(&(name.len() as u16).to_le_bytes());
            for unit in name.encode_utf16() {
                b.extend_from_slice(&unit.to_le_bytes());
            }
            b.extend_from_slice(&0u16.to_le_bytes());
            parse_rpc_request(&b).expect("parse").proc
        };
        assert!(matches!(by_name("sp_prepare"), RpcProc::SpPrepare));
        assert!(matches!(by_name("SP_EXECUTE"), RpcProc::SpExecute));
        assert!(matches!(by_name("sp_unprepare"), RpcProc::SpUnprepare));
        assert!(matches!(by_name("sp_cursorfetch"), RpcProc::SpCursor(_)));
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
