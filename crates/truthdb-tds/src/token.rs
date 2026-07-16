//! TDS server token-stream builders (MS-TDS 2.2.7): LOGINACK, ENVCHANGE,
//! INFO, ERROR, COLMETADATA, ROW, DONE. All strings are UCS-2LE.

use truthdb_core::rel::ResultColumn;
use truthdb_core::relstore::types::Datum;

use crate::typeinfo;

const TOKEN_COLMETADATA: u8 = 0x81;
const TOKEN_ERROR: u8 = 0xaa;
const TOKEN_INFO: u8 = 0xab;
const TOKEN_LOGINACK: u8 = 0xad;
const TOKEN_ROW: u8 = 0xd1;
const TOKEN_ENVCHANGE: u8 = 0xe3;
const TOKEN_DONE: u8 = 0xfd;
const TOKEN_DONEPROC: u8 = 0xfe;
const TOKEN_DONEINPROC: u8 = 0xff;
const TOKEN_RETURNSTATUS: u8 = 0x79;

// DONE status bits.
const DONE_FINAL: u16 = 0x0000;
const DONE_MORE: u16 = 0x0001;
const DONE_ERROR: u16 = 0x0002;
const DONE_INXACT: u16 = 0x0004;
const DONE_COUNT: u16 = 0x0010;
const DONE_ATTN: u16 = 0x0020;

// ENVCHANGE types.
const ENV_DATABASE: u8 = 1;
const ENV_PACKET_SIZE: u8 = 4;
const ENV_BEGIN_TRAN: u8 = 8;
const ENV_COMMIT_TRAN: u8 = 9;
const ENV_ROLLBACK_TRAN: u8 = 10;

/// UCS-2LE code units of `s`, truncated to at most `max` units. Truncating
/// keeps the emitted body consistent with a length prefix that can only count
/// up to `max`; without it an over-long string wraps the prefix while the full
/// body is still written, desyncing the whole token stream. A boundary split of
/// a surrogate pair is harmless for framing (the byte count still matches).
fn ucs2_capped(s: &str, max: usize) -> Vec<u16> {
    s.encode_utf16().take(max).collect()
}

fn push_units(out: &mut Vec<u8>, units: &[u16]) {
    for u in units {
        out.extend_from_slice(&u.to_le_bytes());
    }
}

/// B_VARCHAR: 1-byte character count then UCS-2LE (count capped at 255).
fn push_b_varchar(out: &mut Vec<u8>, s: &str) {
    let units = ucs2_capped(s, u8::MAX as usize);
    out.push(units.len() as u8);
    push_units(out, &units);
}

/// US_VARCHAR: 2-byte character count then UCS-2LE (count capped at 65535).
fn push_us_varchar(out: &mut Vec<u8>, s: &str) {
    let units = ucs2_capped(s, u16::MAX as usize);
    out.extend_from_slice(&(units.len() as u16).to_le_bytes());
    push_units(out, &units);
}

/// LOGINACK: interface = T-SQL, TDS 7.4, server program name + version.
pub fn loginack(out: &mut Vec<u8>) {
    let mut body = Vec::new();
    body.push(1); // interface: SQL_TSQL
    // TDS 7.4 version 0x74000004, sent so a big-endian read yields that value.
    body.extend_from_slice(&[0x74, 0x00, 0x00, 0x04]);
    push_b_varchar(&mut body, "TruthDB");
    body.extend_from_slice(&[16, 0, 0, 0]); // program version major.minor.build

    out.push(TOKEN_LOGINACK);
    out.extend_from_slice(&(body.len() as u16).to_le_bytes());
    out.extend_from_slice(&body);
}

/// ENVCHANGE for the current database (new = old = database name).
pub fn envchange_database(out: &mut Vec<u8>, database: &str) {
    let mut body = Vec::new();
    body.push(ENV_DATABASE);
    push_b_varchar(&mut body, database);
    push_b_varchar(&mut body, database);
    out.push(TOKEN_ENVCHANGE);
    out.extend_from_slice(&(body.len() as u16).to_le_bytes());
    out.extend_from_slice(&body);
}

/// ENVCHANGE for the negotiated packet size.
pub fn envchange_packet_size(out: &mut Vec<u8>, new_size: usize, old_size: usize) {
    let mut body = Vec::new();
    body.push(ENV_PACKET_SIZE);
    push_b_varchar(&mut body, &new_size.to_string());
    push_b_varchar(&mut body, &old_size.to_string());
    out.push(TOKEN_ENVCHANGE);
    out.extend_from_slice(&(body.len() as u16).to_le_bytes());
    out.extend_from_slice(&body);
}

/// A transaction descriptor as a B_VARBYTE (1-byte length + 8 LE bytes).
fn push_b_varbyte_descriptor(out: &mut Vec<u8>, descriptor: u64) {
    out.push(8);
    out.extend_from_slice(&descriptor.to_le_bytes());
}

/// ENVCHANGE type 8 (Begin Transaction): NewValue = the new descriptor,
/// OldValue empty. Drives `db.BeginTx()` on the client.
pub fn envchange_begin_tran(out: &mut Vec<u8>, descriptor: u64) {
    let mut body = Vec::new();
    body.push(ENV_BEGIN_TRAN);
    push_b_varbyte_descriptor(&mut body, descriptor); // new value
    body.push(0); // old value: empty
    out.push(TOKEN_ENVCHANGE);
    out.extend_from_slice(&(body.len() as u16).to_le_bytes());
    out.extend_from_slice(&body);
}

/// ENVCHANGE type 9 (Commit Transaction): NewValue empty, OldValue = the
/// descriptor that just committed.
pub fn envchange_commit_tran(out: &mut Vec<u8>, descriptor: u64) {
    envchange_end_tran(out, ENV_COMMIT_TRAN, descriptor);
}

/// ENVCHANGE type 10 (Rollback Transaction): NewValue empty, OldValue = the
/// descriptor that just rolled back.
pub fn envchange_rollback_tran(out: &mut Vec<u8>, descriptor: u64) {
    envchange_end_tran(out, ENV_ROLLBACK_TRAN, descriptor);
}

fn envchange_end_tran(out: &mut Vec<u8>, env_type: u8, descriptor: u64) {
    let mut body = Vec::new();
    body.push(env_type);
    body.push(0); // new value: empty
    push_b_varbyte_descriptor(&mut body, descriptor); // old value
    out.push(TOKEN_ENVCHANGE);
    out.extend_from_slice(&(body.len() as u16).to_le_bytes());
    out.extend_from_slice(&body);
}

/// INFO token (informational message, same shape as ERROR).
pub fn info(out: &mut Vec<u8>, number: i32, state: u8, class: u8, message: &str) {
    message_token(out, TOKEN_INFO, number, state, class, message);
}

/// ERROR token.
pub fn error(out: &mut Vec<u8>, number: i32, state: u8, class: u8, message: &str) {
    message_token(out, TOKEN_ERROR, number, state, class, message);
}

/// Cap on an ERROR/INFO message's character count. SQL error text embeds
/// attacker-controlled identifiers (e.g. an invalid object name) and the batch
/// text is uncapped, so a pathological message could otherwise overflow the
/// token's own u16 Length field. 32000 UCS-2 units (64000 bytes) leaves room
/// for the fixed fields to keep the whole token body under 65535.
const MAX_MESSAGE_CHARS: usize = 32000;

fn message_token(out: &mut Vec<u8>, token: u8, number: i32, state: u8, class: u8, message: &str) {
    let mut body = Vec::new();
    body.extend_from_slice(&number.to_le_bytes());
    body.push(state);
    body.push(class);
    let capped: String = message.chars().take(MAX_MESSAGE_CHARS).collect();
    push_us_varchar(&mut body, &capped);
    push_b_varchar(&mut body, "TruthDB"); // server name
    push_b_varchar(&mut body, ""); // proc name
    body.extend_from_slice(&1u32.to_le_bytes()); // line number

    out.push(token);
    out.extend_from_slice(&(body.len() as u16).to_le_bytes());
    out.extend_from_slice(&body);
}

const TOKEN_RETURNVALUE: u8 = 0xac;

/// RETURNVALUE carrying an INT output parameter — the prepared-statement
/// handle `sp_prepare`/`sp_prepexec` reports. Layout (MS-TDS 2.2.7.19):
/// ParamOrdinal, ParamName, Status (0x01 = output parameter), UserType,
/// Flags, TYPE_INFO (INTN, 4), value.
pub fn return_value_int(out: &mut Vec<u8>, name: &str, value: i32) {
    out.push(TOKEN_RETURNVALUE);
    out.extend_from_slice(&0u16.to_le_bytes()); // ParamOrdinal
    push_b_varchar(out, name);
    out.push(0x01); // Status: output parameter
    out.extend_from_slice(&0u32.to_le_bytes()); // UserType
    out.extend_from_slice(&0u16.to_le_bytes()); // Flags
    out.push(0x26); // INTN
    out.push(4);
    out.push(4); // value length
    out.extend_from_slice(&value.to_le_bytes());
}

/// COLMETADATA for a result set's columns.
pub fn colmetadata(out: &mut Vec<u8>, columns: &[ResultColumn]) {
    out.push(TOKEN_COLMETADATA);
    out.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for column in columns {
        out.extend_from_slice(&0u32.to_le_bytes()); // UserType
        out.extend_from_slice(&0x0009u16.to_le_bytes()); // flags: nullable + updatable
        out.extend_from_slice(&typeinfo::encode_type_info(&column.column_type));
        push_b_varchar(out, &column.name);
    }
}

/// ROW token for one row.
pub fn row(out: &mut Vec<u8>, values: &[Datum], columns: &[ResultColumn]) {
    out.push(TOKEN_ROW);
    for (datum, column) in values.iter().zip(columns) {
        out.extend_from_slice(&typeinfo::encode_value(datum, &column.column_type));
    }
}

/// DONE token. `more` sets DONE_MORE (another result follows); `count`
/// carries a row count (DONE_COUNT); `error` sets DONE_ERROR; `in_xact` sets
/// DONE_INXACT (a transaction is still active for the connection).
pub fn done(out: &mut Vec<u8>, more: bool, error: bool, in_xact: bool, count: Option<u64>) {
    done_kind(out, TOKEN_DONE, more, error, in_xact, count);
}

/// DONEINPROC (0xFF): a statement's DONE inside an RPC response. Same body as
/// DONE; only the token byte differs.
pub fn done_in_proc(out: &mut Vec<u8>, more: bool, error: bool, in_xact: bool, count: Option<u64>) {
    done_kind(out, TOKEN_DONEINPROC, more, error, in_xact, count);
}

/// DONEPROC (0xFE): the final DONE of an RPC response.
pub fn done_proc(out: &mut Vec<u8>, more: bool, error: bool, in_xact: bool, count: Option<u64>) {
    done_kind(out, TOKEN_DONEPROC, more, error, in_xact, count);
}

fn done_kind(
    out: &mut Vec<u8>,
    token: u8,
    more: bool,
    error: bool,
    in_xact: bool,
    count: Option<u64>,
) {
    let mut status = if more { DONE_MORE } else { DONE_FINAL };
    if error {
        status |= DONE_ERROR;
    }
    if in_xact {
        status |= DONE_INXACT;
    }
    if count.is_some() {
        status |= DONE_COUNT;
    }
    out.push(token);
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // CurCmd
    out.extend_from_slice(&count.unwrap_or(0).to_le_bytes());
}

/// RETURNSTATUS (0x79): the RETURN value of the procedure an RPC invoked.
/// The sp_* procedures here return 0 (success); a failed one sends none.
pub fn return_status(out: &mut Vec<u8>, value: i32) {
    out.push(TOKEN_RETURNSTATUS);
    out.extend_from_slice(&value.to_le_bytes());
}

/// DONE acknowledging an Attention (cancel) request.
pub fn done_attention(out: &mut Vec<u8>) {
    out.push(TOKEN_DONE);
    out.extend_from_slice(&(DONE_FINAL | DONE_ATTN).to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use truthdb_core::relstore::types::ColumnType;

    #[test]
    fn loginack_shape() {
        let mut out = Vec::new();
        loginack(&mut out);
        assert_eq!(out[0], TOKEN_LOGINACK);
        let len = u16::from_le_bytes([out[1], out[2]]) as usize;
        assert_eq!(out.len(), 3 + len);
        assert_eq!(out[3], 1); // interface
    }

    #[test]
    fn error_token_carries_number_and_message() {
        let mut out = Vec::new();
        error(&mut out, 2627, 1, 14, "PK violation");
        assert_eq!(out[0], TOKEN_ERROR);
        let number = i32::from_le_bytes([out[3], out[4], out[5], out[6]]);
        assert_eq!(number, 2627);
        assert_eq!(out[7], 1); // state
        assert_eq!(out[8], 14); // class
    }

    #[test]
    fn colmetadata_and_row_for_int_column() {
        let columns = vec![ResultColumn {
            name: "id".to_string(),
            column_type: ColumnType::Int,
        }];
        let mut meta = Vec::new();
        colmetadata(&mut meta, &columns);
        assert_eq!(meta[0], TOKEN_COLMETADATA);
        assert_eq!(u16::from_le_bytes([meta[1], meta[2]]), 1);

        let mut r = Vec::new();
        row(&mut r, &[Datum::Int(7)], &columns);
        assert_eq!(r[0], TOKEN_ROW);
        // INTN value: len 4 then LE bytes.
        assert_eq!(r[1], 4);
        assert_eq!(&r[2..6], &[7, 0, 0, 0]);
    }

    #[test]
    fn done_status_bits() {
        let mut out = Vec::new();
        done(&mut out, false, false, false, Some(3));
        assert_eq!(out[0], TOKEN_DONE);
        let status = u16::from_le_bytes([out[1], out[2]]);
        assert_eq!(status, DONE_COUNT);
        let count = u64::from_le_bytes(out[5..13].try_into().unwrap());
        assert_eq!(count, 3);
    }

    #[test]
    fn done_sets_inxact_flag() {
        let mut out = Vec::new();
        done(&mut out, false, false, true, None);
        let status = u16::from_le_bytes([out[1], out[2]]);
        assert_eq!(status & DONE_INXACT, DONE_INXACT);
    }

    #[test]
    fn envchange_begin_tran_carries_descriptor() {
        let mut out = Vec::new();
        envchange_begin_tran(&mut out, 0x1122334455667788);
        assert_eq!(out[0], TOKEN_ENVCHANGE);
        let len = u16::from_le_bytes([out[1], out[2]]) as usize;
        assert_eq!(out.len(), 3 + len);
        assert_eq!(out[3], ENV_BEGIN_TRAN);
        assert_eq!(out[4], 8); // new value length
        let desc = u64::from_le_bytes(out[5..13].try_into().unwrap());
        assert_eq!(desc, 0x1122334455667788);
        assert_eq!(out[13], 0); // old value length
    }

    #[test]
    fn envchange_commit_tran_carries_old_descriptor() {
        let mut out = Vec::new();
        envchange_commit_tran(&mut out, 0x42);
        assert_eq!(out[3], ENV_COMMIT_TRAN);
        assert_eq!(out[4], 0); // new value length
        assert_eq!(out[5], 8); // old value length
        let desc = u64::from_le_bytes(out[6..14].try_into().unwrap());
        assert_eq!(desc, 0x42);
    }

    #[test]
    fn colmetadata_long_name_count_matches_bytes() {
        // A >255-char column name must not wrap the B_VARCHAR count byte and
        // desync the stream: the count is capped at 255 and exactly that many
        // UCS-2 units follow.
        let columns = vec![ResultColumn {
            name: "a".repeat(300),
            column_type: ColumnType::Int,
        }];
        let mut out = Vec::new();
        colmetadata(&mut out, &columns);
        // token(1) + count(2) + UserType(4) + flags(2) + INTN type_info(2) = 11,
        // then the ColName B_VARCHAR count byte.
        let name_count = out[11] as usize;
        assert_eq!(name_count, 255);
        assert_eq!(out.len(), 11 + 1 + name_count * 2);
    }

    #[test]
    fn error_message_length_never_wraps() {
        // A pathological error message must keep the ERROR token's own u16
        // Length consistent with the emitted body.
        let mut out = Vec::new();
        error(&mut out, 208, 1, 16, &"x".repeat(100_000));
        let declared = u16::from_le_bytes([out[1], out[2]]) as usize;
        assert_eq!(out.len(), 3 + declared);
    }
}
