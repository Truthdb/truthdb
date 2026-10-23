//! The SQL queue: stream topics read as Amazon SQS reads a queue, through
//! built-in procedures (see `truthdb_streams::QueueMessage`):
//!
//! - `sys.sp_s3_queue_receive @queue, [@max_messages = 1], [@visibility_timeout = 30]`:
//!   up to `@max_messages` (1 to 10) visible messages, each hidden for
//!   `@visibility_timeout` seconds (0 to 43,200), as a result set with a
//!   receipt handle per message, new with each receive.
//! - `sys.sp_s3_queue_ack @queue, @receipt_handle`: the message is done. A
//!   receipt of an earlier receive changes nothing.
//! - `sys.sp_s3_queue_change_visibility @queue, @receipt_handle, @visibility_timeout`:
//!   hides the message for that long from now (0: visible at once), within
//!   12 hours of its first receive.
//!
//! Each runs in a streams transaction of its own, outside the caller's: a
//! rollback does not return a received message to the queue (its
//! visibility timeout does). Queues are server-wide: only a server
//! administrator may use them.

use std::sync::Arc;

use md5::{Digest, Md5};
use truthdb_core::storage::{Storage, StorageError};
use truthdb_relational::engine::{ExternalCaller, ExternalProcedures, RowSet};
use truthdb_relational::relstore::types::Datum;
use truthdb_sql::error::SqlError;
use truthdb_sql::value::SqlValue;
use truthdb_streams::{QueueError, QueueMessage, StreamFile};

use super::s3_sql::{bigint, datetime, datetime2, nvarchar, nvarchar_max, text};

const RECEIVE: &str = "sys.sp_s3_queue_receive";
const ACK: &str = "sys.sp_s3_queue_ack";
const CHANGE_VISIBILITY: &str = "sys.sp_s3_queue_change_visibility";

/// SQS's limits and default.
const MAX_MESSAGES: i64 = 10;
const MAX_VISIBILITY_SECONDS: i64 = 43_200;
const DEFAULT_VISIBILITY_SECONDS: i64 = 30;

/// The queue procedures, over the storage the engine runs on.
pub(crate) struct QueueProcedures {
    pub(crate) core: Arc<Storage>,
}

impl ExternalProcedures for QueueProcedures {
    fn handles(&self, name: &str) -> bool {
        [RECEIVE, ACK, CHANGE_VISIBILITY].contains(&name)
    }

    fn run(
        &self,
        name: &str,
        args: &[(Option<String>, SqlValue)],
        caller: ExternalCaller,
    ) -> Result<Option<RowSet>, SqlError> {
        if !caller.server_admin {
            return Err(SqlError::new(
                229,
                14,
                5,
                format!(
                    "The EXECUTE permission was denied on the object '{}', database \
                     'mssqlsystemresource', schema 'sys'.",
                    name.trim_start_matches("sys.")
                ),
            ));
        }
        let now_ms = super::now_ms();
        match name {
            RECEIVE => {
                let [queue, max, visibility] = bind(
                    name,
                    args,
                    &["queue", "max_messages", "visibility_timeout"],
                    1,
                )?;
                let queue = string_param(name, "queue", queue)?;
                let max = int_param(name, "max_messages", max, 1, 1, MAX_MESSAGES)?;
                let visibility = int_param(
                    name,
                    "visibility_timeout",
                    visibility,
                    DEFAULT_VISIBILITY_SECONDS,
                    0,
                    MAX_VISIBILITY_SECONDS,
                )?;
                let messages = self.queue(&queue, |tx| {
                    tx.queue_receive(&queue, max as usize, visibility as u64 * 1000, now_ms)
                })?;
                Ok(Some(rowset(messages)))
            }
            ACK => {
                let [queue, receipt] = bind(name, args, &["queue", "receipt_handle"], 2)?;
                let queue = string_param(name, "queue", queue)?;
                let receipt = string_param(name, "receipt_handle", receipt)?;
                self.queue(&queue, |tx| tx.queue_ack(&queue, &receipt))?;
                Ok(None)
            }
            _ => {
                let [queue, receipt, visibility] = bind(
                    name,
                    args,
                    &["queue", "receipt_handle", "visibility_timeout"],
                    3,
                )?;
                let queue = string_param(name, "queue", queue)?;
                let receipt = string_param(name, "receipt_handle", receipt)?;
                let visibility = int_param(
                    name,
                    "visibility_timeout",
                    visibility,
                    0,
                    0,
                    MAX_VISIBILITY_SECONDS,
                )?;
                self.queue(&queue, |tx| {
                    tx.queue_change_visibility(&queue, &receipt, visibility as u64 * 1000, now_ms)
                })?;
                Ok(None)
            }
        }
    }
}

impl QueueProcedures {
    /// Runs `op` as a streams transaction of its own.
    fn queue<T>(
        &self,
        queue: &str,
        op: impl FnOnce(
            &mut truthdb_streams::StreamTxn<'_, '_>,
        ) -> Result<Result<T, QueueError>, StorageError>,
    ) -> Result<T, SqlError> {
        let mut guard = self.core.lock();
        let mut file = StreamFile::try_new(&mut guard).ok_or_else(|| {
            SqlError::new(
                50000,
                16,
                1,
                "this database does not run the streams engine",
            )
        })?;
        file.transaction(op)
            .map_err(|err| SqlError::new(50000, 16, 1, format!("streams: {err}")))?
            .map_err(|err| queue_error(err, queue))
    }
}

fn queue_error(err: QueueError, queue: &str) -> SqlError {
    let message = match err {
        QueueError::NoSuchTopic => format!("The queue '{queue}' does not exist."),
        QueueError::InvalidReceipt => "The receipt handle is not valid.".to_string(),
        QueueError::NotInFlight => "The message is not in flight.".to_string(),
        QueueError::TooLong => {
            "The visibility timeout would end more than 12 hours after the message was first \
             received."
                .to_string()
        }
    };
    SqlError::new(50000, 16, 1, message)
}

/// `args` bound to `params` by name or position; the first `required` must
/// be given.
fn bind<'a, const N: usize>(
    name: &str,
    args: &'a [(Option<String>, SqlValue)],
    params: &[&str; N],
    required: usize,
) -> Result<[Option<&'a SqlValue>; N], SqlError> {
    let mut bound: [Option<&SqlValue>; N] = [None; N];
    for (at, (arg_name, value)) in args.iter().enumerate() {
        let slot = match arg_name {
            None if at < N => at,
            None => {
                return Err(SqlError::new(
                    8144,
                    16,
                    2,
                    format!("Procedure or function {name} has too many arguments specified."),
                ));
            }
            Some(arg_name) => params.iter().position(|p| p == arg_name).ok_or_else(|| {
                SqlError::new(
                    8145,
                    16,
                    2,
                    format!("@{arg_name} is not a parameter for procedure {name}."),
                )
            })?,
        };
        if bound[slot].is_some() {
            return Err(SqlError::new(
                8143,
                16,
                1,
                format!("Parameter '@{}' was supplied multiple times.", params[slot]),
            ));
        }
        bound[slot] = Some(value);
    }
    if let Some(missing) = (0..required).find(|&at| bound[at].is_none()) {
        return Err(SqlError::new(
            201,
            16,
            4,
            format!(
                "Procedure or function '{name}' expects parameter '@{}', which was not supplied.",
                params[missing]
            ),
        ));
    }
    Ok(bound)
}

fn string_param(name: &str, param: &str, value: Option<&SqlValue>) -> Result<String, SqlError> {
    match value {
        Some(SqlValue::Str(text)) => Ok(text.clone()),
        _ => Err(SqlError::new(
            206,
            16,
            2,
            format!("Operand type clash: @{param} of {name} must be a character string."),
        )),
    }
}

/// An integer parameter within `min..=max` (`default` when not given or
/// NULL).
fn int_param(
    name: &str,
    param: &str,
    value: Option<&SqlValue>,
    default: i64,
    min: i64,
    max: i64,
) -> Result<i64, SqlError> {
    match value {
        None | Some(SqlValue::Null) => Ok(default),
        Some(SqlValue::Int(value)) if (min..=max).contains(value) => Ok(*value),
        Some(SqlValue::Int(value)) => Err(SqlError::new(
            50000,
            16,
            1,
            format!("@{param} of {name} must be from {min} to {max}, not {value}."),
        )),
        Some(_) => Err(SqlError::new(
            206,
            16,
            2,
            format!("Operand type clash: @{param} of {name} must be an integer."),
        )),
    }
}

fn rowset(messages: Vec<QueueMessage>) -> RowSet {
    RowSet {
        columns: vec![
            nvarchar("message_id", 64),
            nvarchar("receipt_handle", 64),
            nvarchar_max("body"),
            nvarchar("md5_of_body", 32),
            bigint("receive_count"),
            datetime2("sent_time"),
            datetime2("first_receive_time"),
        ],
        rows: messages
            .into_iter()
            .map(|message| {
                let md5: String = Md5::digest(&message.value)
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect();
                vec![
                    text(&format!("{}-{}", message.partition, message.offset)),
                    text(&message.receipt),
                    text(&String::from_utf8_lossy(&message.value)),
                    text(&md5),
                    Datum::BigInt(i64::from(message.receives)),
                    datetime(message.timestamp_ms),
                    datetime(message.first_receive_ms),
                ]
            })
            .collect(),
    }
}
