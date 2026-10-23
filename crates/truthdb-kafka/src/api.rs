//! The requests the listener answers, in the non-flexible versions it
//! advertises: what a consumer needs (metadata, offsets, fetches, consumer
//! groups) and SASL PLAIN authentication. A connection is authenticated
//! before anything but ApiVersions and the SASL requests; it then sees the
//! topics its access key's S3 account owns, and its consumer groups are
//! that account's.

use std::time::{Duration, Instant};

use truthdb_engine::engine::Engine;
use truthdb_engine::session::EngineHandle;
use truthdb_object::ObjFile;
use truthdb_streams::{Partition, Record, StreamFile, Topic};

use crate::batch;
use crate::errors::*;
use crate::group::{Answer, Coordinator, Join};
use crate::wire::{Malformed, Reader, Writer};

pub const FETCH: i16 = 1;
pub const LIST_OFFSETS: i16 = 2;
pub const METADATA: i16 = 3;
pub const OFFSET_COMMIT: i16 = 8;
pub const OFFSET_FETCH: i16 = 9;
pub const FIND_COORDINATOR: i16 = 10;
pub const JOIN_GROUP: i16 = 11;
pub const HEARTBEAT: i16 = 12;
pub const LEAVE_GROUP: i16 = 13;
pub const SYNC_GROUP: i16 = 14;
pub const SASL_HANDSHAKE: i16 = 17;
pub const API_VERSIONS: i16 = 18;
pub const SASL_AUTHENTICATE: i16 = 36;

/// Each API's versions served: (key, min, max). The maximums are the last
/// versions before each API's flexible ("compact") encoding; the minimums
/// the first with the fields a consumer of record batch v2 needs.
pub const APIS: [(i16, i16, i16); 13] = [
    (FETCH, 4, 11),
    (LIST_OFFSETS, 1, 5),
    (METADATA, 0, 8),
    (OFFSET_COMMIT, 2, 7),
    (OFFSET_FETCH, 1, 5),
    (FIND_COORDINATOR, 0, 2),
    (JOIN_GROUP, 0, 5),
    (HEARTBEAT, 0, 3),
    (LEAVE_GROUP, 0, 3),
    (SYNC_GROUP, 0, 3),
    (SASL_HANDSHAKE, 1, 1),
    (API_VERSIONS, 0, 2),
    (SASL_AUTHENTICATE, 0, 1),
];

/// The broker's node id: the one broker, leader of every partition and
/// coordinator of every group.
const NODE: i32 = 0;
/// "Not requested" for the authorized-operations fields.
const NO_OPERATIONS: i32 = i32::MIN;
/// The bytes one fetch answers with at most, and the records it reads from
/// a partition at most (each partition is read in one hold of the storage
/// lock).
const MAX_FETCH_BYTES: usize = 8 << 20;
const MAX_FETCH_RECORDS: usize = 4096;
/// How long a fetch waits for records at most, whatever it asks.
const MAX_FETCH_WAIT: Duration = Duration::from_secs(30);
/// How often a waiting fetch looks for new records.
const FETCH_POLL: Duration = Duration::from_millis(100);
/// A group id's bytes at most (it is part of a stored key).
const MAX_GROUP_ID: usize = 255;
/// The partition entries one request may list (in all its topics); a
/// request with more closes the connection.
const MAX_PARTITION_ENTRIES: usize = 4096;

pub struct KafkaState {
    pub engine: EngineHandle,
    /// The host and port clients are told to connect to.
    pub host: String,
    pub port: u16,
    pub coordinator: Coordinator,
}

/// Who a connection authenticated as.
#[derive(Debug, Clone)]
pub struct Session {
    pub access_key: String,
    pub account: String,
}

/// Where a connection's authentication stands.
pub enum Auth {
    Start,
    /// SaslHandshake chose PLAIN.
    Handshaken,
    Done(Session),
}

/// What to do with a request: answer it, answer it and close the
/// connection (a failed authentication), or close it unanswered.
pub enum Outcome {
    Answer(Vec<u8>),
    AnswerAndClose(Vec<u8>),
    Close,
}

/// A request's header (v1: the non-flexible requests' and, as far as it is
/// read, the flexible ones').
pub struct Header {
    pub api_key: i16,
    pub version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

pub fn read_header(reader: &mut Reader<'_>) -> Result<Header, Malformed> {
    Ok(Header {
        api_key: reader.i16()?,
        version: reader.i16()?,
        correlation_id: reader.i32()?,
        client_id: reader.nullable_string()?.unwrap_or_default(),
    })
}

fn supported(api_key: i16, version: i16) -> bool {
    APIS.iter()
        .any(|&(key, min, max)| key == api_key && (min..=max).contains(&version))
}

/// Answers one request.
pub async fn handle(state: &KafkaState, auth: &mut Auth, header: &Header, body: &[u8]) -> Outcome {
    let version = header.version;
    if header.api_key == API_VERSIONS {
        return Outcome::Answer(api_versions(version));
    }
    if !supported(header.api_key, version) {
        return Outcome::Close;
    }
    let mut reader = Reader::new(body);
    let session = match (header.api_key, &*auth) {
        (SASL_HANDSHAKE, Auth::Start) => {
            return match sasl_handshake(&mut reader, auth) {
                Ok(answer) => Outcome::Answer(answer),
                Err(Malformed) => Outcome::Close,
            };
        }
        (SASL_AUTHENTICATE, Auth::Handshaken) => {
            return sasl_authenticate(state, &mut reader, version, auth)
                .await
                .unwrap_or(Outcome::Close);
        }
        (SASL_HANDSHAKE | SASL_AUTHENTICATE, _) | (_, Auth::Start | Auth::Handshaken) => {
            return Outcome::Close;
        }
        (_, Auth::Done(session)) => session.clone(),
    };
    // The key may have been dropped or deactivated since: the connection
    // ends with it.
    if !still_valid(state, &session).await {
        return Outcome::Close;
    }
    let answer = match header.api_key {
        METADATA => metadata(state, &session, &mut reader, version).await,
        FIND_COORDINATOR => find_coordinator(state, &mut reader, version),
        LIST_OFFSETS => list_offsets(state, &session, &mut reader, version).await,
        FETCH => fetch(state, &session, &mut reader, version).await,
        OFFSET_COMMIT => offset_commit(state, &session, &mut reader, version).await,
        OFFSET_FETCH => offset_fetch(state, &session, &mut reader, version).await,
        JOIN_GROUP => join_group(state, &session, header, &mut reader).await,
        SYNC_GROUP => sync_group(state, &session, &mut reader, version).await,
        HEARTBEAT => heartbeat(state, &session, &mut reader, version),
        LEAVE_GROUP => leave_group(state, &session, &mut reader, version),
        _ => Err(Malformed),
    };
    match answer {
        Ok(answer) => Outcome::Answer(answer),
        Err(Malformed) => Outcome::Close,
    }
}

/// ApiVersions: the APIs and versions served. A version not served is
/// answered in v0 with UNSUPPORTED_VERSION (the client retries with one
/// that is).
fn api_versions(version: i16) -> Vec<u8> {
    let served = supported(API_VERSIONS, version);
    let mut out = Writer::default();
    out.i16(if served { NONE } else { UNSUPPORTED_VERSION });
    out.array(&APIS, |out, &(key, min, max)| {
        out.i16(key).i16(min).i16(max);
    });
    if served && version >= 1 {
        out.i32(0);
    }
    out.bytes
}

fn sasl_handshake(reader: &mut Reader<'_>, auth: &mut Auth) -> Result<Vec<u8>, Malformed> {
    let mechanism = reader.string()?;
    let mut out = Writer::default();
    if mechanism == "PLAIN" {
        *auth = Auth::Handshaken;
        out.i16(NONE);
    } else {
        out.i16(UNSUPPORTED_SASL_MECHANISM);
    }
    out.array(&["PLAIN"], |out, name| {
        out.string(name);
    });
    Ok(out.bytes)
}

/// SaslAuthenticate with a PLAIN token (RFC 4616): an authorization id
/// (empty, or the same as the authentication id), an S3 access key id and
/// its secret. Only an active key of an account's root is accepted.
async fn sasl_authenticate(
    state: &KafkaState,
    reader: &mut Reader<'_>,
    version: i16,
    auth: &mut Auth,
) -> Result<Outcome, Malformed> {
    let token = reader.bytes()?;
    let session = match plain_token(&token) {
        Some((key_id, secret)) => check_secret(state, key_id, secret).await,
        None => None,
    };
    let mut out = Writer::default();
    let accepted = session.is_some();
    match session {
        Some(session) => {
            *auth = Auth::Done(session);
            out.i16(NONE).nullable_string(None);
        }
        None => {
            out.i16(SASL_AUTHENTICATION_FAILED)
                .nullable_string(Some("Authentication failed: Invalid username or password"));
        }
    }
    out.bytes(&[]);
    if version >= 1 {
        out.i64(0); // no session lifetime: the key is checked on every request
    }
    Ok(if accepted {
        Outcome::Answer(out.bytes)
    } else {
        Outcome::AnswerAndClose(out.bytes)
    })
}

fn plain_token(token: &[u8]) -> Option<(&str, &[u8])> {
    let mut parts = token.split(|&byte| byte == 0);
    let (authzid, authcid, password) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || authcid.is_empty() || !(authzid.is_empty() || authzid == authcid) {
        return None;
    }
    Some((std::str::from_utf8(authcid).ok()?, password))
}

async fn check_secret(state: &KafkaState, key_id: &str, secret: &[u8]) -> Option<Session> {
    let id = key_id.to_string();
    let (key, stored) = state
        .engine
        .run_object(move |file: &mut ObjFile<'_>| file.access_key(&id))
        .await
        .ok()??;
    if !key.active || key.user.is_some() {
        return None;
    }
    // Compared in constant time: an HMAC under a one-off key of the stored
    // secret, verified against the one given.
    let rng = ring::rand::SystemRandom::new();
    let mac = ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &rng).ok()?;
    let tag = ring::hmac::sign(&mac, stored.as_bytes());
    ring::hmac::verify(&mac, secret, tag.as_ref()).ok()?;
    Some(Session {
        access_key: key.id,
        account: key.account,
    })
}

async fn still_valid(state: &KafkaState, session: &Session) -> bool {
    let id = session.access_key.clone();
    let account = session.account.clone();
    matches!(
        state
            .engine
            .run_object(move |file: &mut ObjFile<'_>| file.access_key(&id))
            .await,
        Ok(Some((key, _))) if key.active && key.user.is_none() && key.account == account
    )
}

/// Runs `op` on the streams engine on a worker (`None`: it failed).
async fn streams<T, F>(engine: &EngineHandle, op: F) -> Option<T>
where
    T: Send + 'static,
    F: FnOnce(&mut StreamFile<'_>) -> Result<T, truthdb_core::storage::StorageError>
        + Send
        + 'static,
{
    engine
        .run_engine(move |engine: &Engine| engine.streams(op))
        .await
        .ok()
}

/// The account's topics.
async fn owned_topics(state: &KafkaState, account: &str) -> Option<Vec<(Topic, Vec<Partition>)>> {
    let account = account.to_string();
    streams(&state.engine, move |file| {
        Ok(file
            .topics()?
            .into_iter()
            .filter(|(topic, _)| topic.account.as_deref() == Some(account.as_str()))
            .collect())
    })
    .await
}

/// A topic of the account, with its partitions (in the caller's hold of the
/// storage lock, so it cannot change before what follows).
fn owned_topic(
    file: &mut StreamFile<'_>,
    account: &str,
    name: &str,
) -> Result<Option<(Topic, Vec<Partition>)>, truthdb_core::storage::StorageError> {
    Ok(file
        .topic(name)?
        .filter(|(topic, _)| topic.account.as_deref() == Some(account)))
}

/// The group's name in the streams engine: the account's namespace.
fn stored_group(session: &Session, group: &str) -> String {
    format!("{}:{group}", session.account)
}

fn entries_within_limit<T>(topics: &[(String, Vec<T>)]) -> Result<(), Malformed> {
    let entries: usize = topics
        .iter()
        .map(|(_, partitions)| partitions.len().max(1))
        .sum();
    if entries > MAX_PARTITION_ENTRIES {
        return Err(Malformed);
    }
    Ok(())
}

fn valid_group(group: &str) -> bool {
    !group.is_empty() && group.len() <= MAX_GROUP_ID
}

async fn metadata(
    state: &KafkaState,
    session: &Session,
    reader: &mut Reader<'_>,
    version: i16,
) -> Result<Vec<u8>, Malformed> {
    let names = if version == 0 {
        Some(reader.array(|r| r.string())?).filter(|names| !names.is_empty())
    } else {
        reader.nullable_array(|r| r.string())?
    };
    // AllowAutoTopicCreation (v4+, topics are never created here) and the
    // authorized-operations flags (v8+) need no reading.
    let topics = owned_topics(state, &session.account)
        .await
        .unwrap_or_default();
    let listed: Vec<(String, Option<&Vec<Partition>>)> = match &names {
        None => topics
            .iter()
            .map(|(topic, partitions)| (topic.name.clone(), Some(partitions)))
            .collect(),
        Some(names) => names
            .iter()
            .map(|name| {
                let found = topics.iter().find(|(topic, _)| topic.name == *name);
                (name.clone(), found.map(|(_, partitions)| partitions))
            })
            .collect(),
    };
    let mut out = Writer::default();
    if version >= 3 {
        out.i32(0);
    }
    out.array(&[()], |out, ()| {
        out.i32(NODE).string(&state.host).i32(i32::from(state.port));
        if version >= 1 {
            out.nullable_string(None);
        }
    });
    if version >= 2 {
        out.nullable_string(Some("truthdb"));
    }
    if version >= 1 {
        out.i32(NODE);
    }
    out.array(&listed, |out, (name, partitions)| {
        out.i16(if partitions.is_some() {
            NONE
        } else {
            UNKNOWN_TOPIC_OR_PARTITION
        })
        .string(name);
        if version >= 1 {
            out.bool(false);
        }
        let indexes: Vec<i32> = (0..partitions.map_or(0, |p| p.len() as i32)).collect();
        out.array(&indexes, |out, &index| {
            out.i16(NONE).i32(index).i32(NODE);
            if version >= 7 {
                out.i32(-1);
            }
            out.array(&[NODE], |out, &node| {
                out.i32(node);
            });
            out.array(&[NODE], |out, &node| {
                out.i32(node);
            });
            if version >= 5 {
                out.i32(0);
            }
        });
        if version >= 8 {
            out.i32(NO_OPERATIONS);
        }
    });
    if version == 8 {
        out.i32(NO_OPERATIONS);
    }
    Ok(out.bytes)
}

fn find_coordinator(
    state: &KafkaState,
    reader: &mut Reader<'_>,
    version: i16,
) -> Result<Vec<u8>, Malformed> {
    let _key = reader.string()?;
    // Key type 0: a consumer group; 1, a transaction (none here).
    let key_type = if version >= 1 { reader.i8()? } else { 0 };
    let mut out = Writer::default();
    if version >= 1 {
        out.i32(0);
    }
    if key_type == 0 {
        out.i16(NONE);
        if version >= 1 {
            out.nullable_string(None);
        }
        out.i32(NODE).string(&state.host).i32(i32::from(state.port));
    } else {
        out.i16(COORDINATOR_NOT_AVAILABLE);
        if version >= 1 {
            out.nullable_string(Some("only consumer groups have a coordinator"));
        }
        out.i32(-1).string("").i32(-1);
    }
    Ok(out.bytes)
}

/// ListOffsets: per partition, the latest offset (timestamp -1: the next
/// one to be written), the earliest (-2), or the first whose record's
/// timestamp is at or after the one given (none: -1).
async fn list_offsets(
    state: &KafkaState,
    session: &Session,
    reader: &mut Reader<'_>,
    version: i16,
) -> Result<Vec<u8>, Malformed> {
    let _replica = reader.i32()?;
    if version >= 2 {
        let _isolation = reader.i8()?;
    }
    let topics = reader.array(|r| {
        let name = r.string()?;
        let partitions = r.array(|r| {
            let index = r.i32()?;
            if version >= 4 {
                let _epoch = r.i32()?;
            }
            Ok((index, r.i64()?))
        })?;
        Ok((name, partitions))
    })?;
    entries_within_limit(&topics)?;
    let mut answers = Vec::new();
    for (name, partitions) in topics {
        let account = session.account.clone();
        let (topic, indexes) = (
            name.clone(),
            partitions.iter().map(|p| p.0).collect::<Vec<_>>(),
        );
        let answered = streams(&state.engine, move |file| {
            let owned = owned_topic(file, &account, &name)?;
            let mut answers = Vec::new();
            for (index, timestamp) in partitions {
                let state = owned.as_ref().and_then(|(_, states)| {
                    usize::try_from(index).ok().and_then(|i| states.get(i))
                });
                let answer = match state {
                    None => (index, UNKNOWN_TOPIC_OR_PARTITION, -1, -1),
                    Some(state) if timestamp == -1 => (index, NONE, -1, state.next as i64),
                    Some(state) if timestamp == -2 => (index, NONE, -1, state.start as i64),
                    Some(_) => {
                        let at = file
                            .offset_for_time(&name, index as u32, timestamp.max(0) as u64)?
                            .ok();
                        let found = match at {
                            Some(at) => file
                                .read(&name, index as u32, at, 1, 0)?
                                .ok()
                                .and_then(|(records, _)| records.into_iter().next()),
                            None => None,
                        };
                        match found {
                            Some(record) => (
                                index,
                                NONE,
                                record.timestamp_ms as i64,
                                record.offset as i64,
                            ),
                            None => (index, NONE, -1, -1),
                        }
                    }
                };
                answers.push(answer);
            }
            Ok(answers)
        })
        .await
        .unwrap_or_else(|| {
            indexes
                .into_iter()
                .map(|index| (index, UNKNOWN_SERVER_ERROR, -1, -1))
                .collect()
        });
        answers.push((topic, answered));
    }
    let mut out = Writer::default();
    if version >= 2 {
        out.i32(0);
    }
    out.array(&answers, |out, (name, partitions)| {
        out.string(name);
        out.array(partitions, |out, &(index, error, timestamp, offset)| {
            out.i32(index).i16(error).i64(timestamp).i64(offset);
            if version >= 4 {
                out.i32(-1);
            }
        });
    });
    Ok(out.bytes)
}

/// One partition a fetch asks for.
struct FetchPartition {
    index: i32,
    offset: i64,
    max_bytes: i32,
}

/// One partition's answer to a fetch.
struct Fetched {
    index: i32,
    error: i16,
    state: Partition,
    records: Vec<u8>,
}

/// Fetch: from each partition, the records from the offset asked for, as a
/// record batch. When fewer than the minimum bytes are there, the fetch
/// waits for more until its maximum wait. Fetch sessions (v7+) are not
/// kept: every fetch is a full one, answered with session id 0.
async fn fetch(
    state: &KafkaState,
    session: &Session,
    reader: &mut Reader<'_>,
    version: i16,
) -> Result<Vec<u8>, Malformed> {
    let _replica = reader.i32()?;
    let max_wait = Duration::from_millis(reader.i32()?.max(0) as u64).min(MAX_FETCH_WAIT);
    let min_bytes = reader.i32()?.max(0) as usize;
    let max_bytes = (reader.i32()?.max(0) as usize).min(MAX_FETCH_BYTES);
    let _isolation = reader.i8()?;
    let (session_id, epoch) = if version >= 7 {
        (reader.i32()?, reader.i32()?)
    } else {
        (0, -1)
    };
    let topics = reader.array(|r| {
        let name = r.string()?;
        let partitions = r.array(|r| {
            let index = r.i32()?;
            if version >= 9 {
                let _epoch = r.i32()?;
            }
            let offset = r.i64()?;
            if version >= 5 {
                let _log_start = r.i64()?;
            }
            Ok(FetchPartition {
                index,
                offset,
                max_bytes: r.i32()?,
            })
        })?;
        Ok((name, partitions))
    })?;
    entries_within_limit(&topics)?;
    // Forgotten topics (v7+) and the rack (v11) only matter to sessions.
    let mut out = Writer::default();
    out.i32(0);
    if version >= 7 {
        let error = if session_id != 0 {
            FETCH_SESSION_ID_NOT_FOUND
        } else if epoch != 0 && epoch != -1 {
            INVALID_FETCH_SESSION_EPOCH
        } else {
            NONE
        };
        out.i16(error).i32(0);
        if error != NONE {
            out.i32(0);
            return Ok(out.bytes);
        }
    }
    let topics = std::sync::Arc::new(topics);
    let deadline = Instant::now() + max_wait;
    let answers = loop {
        let answers = fetch_round(state, session, &topics, max_bytes).await;
        let bytes: usize = answers
            .iter()
            .flat_map(|(_, partitions)| partitions)
            .map(|fetched| fetched.records.len())
            .sum();
        let failed = answers
            .iter()
            .flat_map(|(_, partitions)| partitions)
            .any(|fetched| fetched.error != NONE);
        let now = Instant::now();
        if failed || bytes >= min_bytes || now >= deadline {
            break answers;
        }
        tokio::time::sleep(FETCH_POLL.min(deadline - now)).await;
    };
    out.array(&answers, |out, (name, partitions)| {
        out.string(name);
        out.array(partitions, |out, fetched| {
            out.i32(fetched.index)
                .i16(fetched.error)
                .i64(fetched.state.next as i64)
                .i64(fetched.state.next as i64);
            if version >= 5 {
                out.i64(fetched.state.start as i64);
            }
            out.i32(0); // aborted transactions: none
            if version >= 11 {
                out.i32(-1); // preferred read replica: none
            }
            out.bytes(&fetched.records);
        });
    });
    Ok(out.bytes)
}

async fn fetch_round(
    state: &KafkaState,
    session: &Session,
    topics: &std::sync::Arc<Vec<(String, Vec<FetchPartition>)>>,
    max_bytes: usize,
) -> Vec<(String, Vec<Fetched>)> {
    let mut left = max_bytes;
    let mut returned_any = false;
    let mut out = Vec::new();
    for (name, partitions) in topics.iter() {
        let mut answers = Vec::new();
        for partition in partitions {
            let want = (partition.max_bytes.max(0) as usize).min(left);
            let (account, topic, index, offset) = (
                session.account.clone(),
                name.clone(),
                partition.index,
                partition.offset,
            );
            // The first partition with records gets at least one record,
            // whatever the limits (so a large record is not stuck); after
            // it, the limits hold.
            let first = !returned_any;
            let fetched = streams(&state.engine, move |file| {
                read_partition(file, &account, &topic, index, offset, want, first)
            })
            .await
            .unwrap_or(Fetched {
                index,
                error: UNKNOWN_SERVER_ERROR,
                state: Partition::default(),
                records: Vec::new(),
            });
            left = left.saturating_sub(fetched.records.len());
            returned_any |= !fetched.records.is_empty();
            answers.push(fetched);
        }
        out.push((name.clone(), answers));
    }
    out
}

fn read_partition(
    file: &mut StreamFile<'_>,
    account: &str,
    topic: &str,
    index: i32,
    offset: i64,
    max_bytes: usize,
    first: bool,
) -> Result<Fetched, truthdb_core::storage::StorageError> {
    let fetched = |error, state, records| Fetched {
        index,
        error,
        state,
        records,
    };
    let partition = owned_topic(file, account, topic)?.and_then(|(_, states)| {
        usize::try_from(index)
            .ok()
            .and_then(|i| states.get(i).copied())
    });
    let Some(state) = partition else {
        return Ok(fetched(
            UNKNOWN_TOPIC_OR_PARTITION,
            Partition::default(),
            Vec::new(),
        ));
    };
    if offset < state.start as i64 || offset > state.next as i64 {
        return Ok(fetched(OFFSET_OUT_OF_RANGE, state, Vec::new()));
    }
    if offset == state.next as i64 || (max_bytes == 0 && !first) {
        return Ok(fetched(NONE, state, Vec::new()));
    }
    let Ok((records, state)) = file.read(
        topic,
        index as u32,
        offset as u64,
        MAX_FETCH_RECORDS,
        max_bytes,
    )?
    else {
        return Ok(fetched(
            UNKNOWN_TOPIC_OR_PARTITION,
            Partition::default(),
            Vec::new(),
        ));
    };
    // Trimmed since the partition was looked at: the offset is gone.
    if records
        .first()
        .is_some_and(|record: &Record| record.offset != offset as u64)
    {
        return Ok(fetched(OFFSET_OUT_OF_RANGE, state, Vec::new()));
    }
    let bytes = if records.is_empty() {
        Vec::new()
    } else {
        batch::encode(&records)
    };
    Ok(fetched(NONE, state, bytes))
}

/// OffsetCommit: accepted from the group's current members (or, for a
/// group with none, from anyone), stored durably per partition. Commit
/// metadata is not kept (OffsetFetch answers it empty).
async fn offset_commit(
    state: &KafkaState,
    session: &Session,
    reader: &mut Reader<'_>,
    version: i16,
) -> Result<Vec<u8>, Malformed> {
    let group = reader.string()?;
    let generation = reader.i32()?;
    let member_id = reader.string()?;
    if version >= 7 {
        let _instance = reader.nullable_string()?;
    }
    if version <= 4 {
        let _retention = reader.i64()?;
    }
    let topics = reader.array(|r| {
        let name = r.string()?;
        let partitions = r.array(|r| {
            let index = r.i32()?;
            let offset = r.i64()?;
            if version >= 6 {
                let _epoch = r.i32()?;
            }
            let _metadata = r.nullable_string()?;
            Ok((index, offset))
        })?;
        Ok((name, partitions))
    })?;
    entries_within_limit(&topics)?;
    let refused = if !valid_group(&group) {
        INVALID_GROUP_ID
    } else {
        state.coordinator.may_commit(
            &stored_group(session, &group),
            generation,
            &member_id,
            Instant::now(),
        )
    };
    let mut answers: Vec<(String, Vec<(i32, i16)>)> = Vec::new();
    for (name, partitions) in topics {
        let failed = |error| {
            partitions
                .iter()
                .map(|&(index, _)| (index, error))
                .collect()
        };
        if refused != NONE {
            answers.push((name, failed(refused)));
            continue;
        }
        let (account, stored, topic) = (
            session.account.clone(),
            stored_group(session, &group),
            name.clone(),
        );
        let asked = partitions.clone();
        let answered = streams(&state.engine, move |file| {
            commit_topic(file, &account, &stored, &topic, &asked)
        })
        .await
        .unwrap_or_else(|| failed(UNKNOWN_SERVER_ERROR));
        answers.push((name, answered));
    }
    let mut out = Writer::default();
    if version >= 3 {
        out.i32(0);
    }
    out.array(&answers, |out, (name, partitions)| {
        out.string(name);
        out.array(partitions, |out, &(index, error)| {
            out.i32(index).i16(error);
        });
    });
    Ok(out.bytes)
}

/// Commits a topic's offsets in one transaction (a topic has at most
/// `MAX_PARTITIONS` partitions, so at most that many commits), if the
/// topic is the account's; the error for each partition asked.
fn commit_topic(
    file: &mut StreamFile<'_>,
    account: &str,
    group: &str,
    topic: &str,
    partitions: &[(i32, i64)],
) -> Result<Vec<(i32, i16)>, truthdb_core::storage::StorageError> {
    let count = owned_topic(file, account, topic)?.map_or(0, |(topic, _)| topic.partitions);
    let mut accepted = std::collections::BTreeMap::new();
    let errors = partitions
        .iter()
        .map(|&(index, offset)| {
            let error = if index < 0 || index as u32 >= count {
                UNKNOWN_TOPIC_OR_PARTITION
            } else if offset < 0 {
                OFFSET_OUT_OF_RANGE
            } else {
                // A partition listed twice: the last offset counts.
                accepted.insert(index as u32, offset as u64);
                NONE
            };
            (index, error)
        })
        .collect();
    if !accepted.is_empty() {
        file.transaction(|tx| {
            for (&index, &offset) in &accepted {
                // Owned and in range: checked above in this hold of the lock.
                let _ = tx.commit_offset(group, topic, index, offset)?;
            }
            Ok(())
        })?;
    }
    Ok(errors)
}

/// OffsetFetch: the group's committed offsets (-1: none), of the
/// partitions asked for or (a null list, v2+) of every partition of the
/// account's topics that has one.
async fn offset_fetch(
    state: &KafkaState,
    session: &Session,
    reader: &mut Reader<'_>,
    version: i16,
) -> Result<Vec<u8>, Malformed> {
    let group = reader.string()?;
    let topics = if version >= 2 {
        reader.nullable_array(|r| Ok((r.string()?, r.array(|r| r.i32())?)))?
    } else {
        Some(reader.array(|r| Ok((r.string()?, r.array(|r| r.i32())?)))?)
    };
    let mut out = Writer::default();
    if version >= 3 {
        out.i32(0);
    }
    if !valid_group(&group) {
        out.i32(0);
        if version >= 2 {
            out.i16(INVALID_GROUP_ID);
        }
        return Ok(out.bytes);
    }
    let every = topics.is_none();
    let asked: Vec<(String, Vec<i32>)> = match topics {
        Some(topics) => {
            entries_within_limit(&topics)?;
            topics
        }
        None => {
            let Some(owned) = owned_topics(state, &session.account).await else {
                out.i32(0);
                if version >= 2 {
                    out.i16(UNKNOWN_SERVER_ERROR);
                }
                return Ok(out.bytes);
            };
            owned
                .into_iter()
                .map(|(topic, _)| (topic.name, (0..topic.partitions as i32).collect()))
                .collect()
        }
    };
    let mut answers = Vec::new();
    for (name, indexes) in asked {
        let (account, stored, topic) = (
            session.account.clone(),
            stored_group(session, &group),
            name.clone(),
        );
        let wanted = indexes.clone();
        let answered = streams(&state.engine, move |file| {
            let count =
                owned_topic(file, &account, &topic)?.map_or(0, |(topic, _)| topic.partitions);
            let mut answers = Vec::new();
            for index in wanted {
                if index < 0 || index as u32 >= count {
                    answers.push((index, -1, UNKNOWN_TOPIC_OR_PARTITION));
                    continue;
                }
                let committed = file.committed_offset(&stored, &topic, index as u32)?;
                answers.push((index, committed.map_or(-1, |offset| offset as i64), NONE));
            }
            Ok(answers)
        })
        .await
        .unwrap_or_else(|| {
            indexes
                .into_iter()
                .map(|index| (index, -1, UNKNOWN_SERVER_ERROR))
                .collect()
        });
        answers.push((name, answered));
    }
    if every {
        // Asked for everything: only the partitions with a commit.
        for (_, partitions) in &mut answers {
            partitions.retain(|&(_, offset, _)| offset >= 0);
        }
        answers.retain(|(_, partitions)| !partitions.is_empty());
    }
    out.array(&answers, |out, (name, partitions)| {
        out.string(name);
        out.array(partitions, |out, &(index, offset, error)| {
            out.i32(index).i64(offset);
            if version >= 5 {
                out.i32(-1);
            }
            out.string("").i16(error);
        });
    });
    if version >= 2 {
        out.i16(NONE);
    }
    Ok(out.bytes)
}

async fn join_group(
    state: &KafkaState,
    session: &Session,
    header: &Header,
    reader: &mut Reader<'_>,
) -> Result<Vec<u8>, Malformed> {
    let version = header.version;
    let group = reader.string()?;
    let session_timeout_ms = reader.i32()?;
    let rebalance_timeout_ms = if version >= 1 { reader.i32()? } else { -1 };
    let member_id = reader.string()?;
    if version >= 5 {
        // Static membership is not kept: such a member is a dynamic one.
        let _instance = reader.nullable_string()?;
    }
    let protocol_type = reader.string()?;
    let protocols = reader.array(|r| Ok((r.string()?, r.bytes()?)))?;
    let joined = if !valid_group(&group) {
        crate::group::Joined {
            error: INVALID_GROUP_ID,
            generation: -1,
            protocol: String::new(),
            leader: String::new(),
            member_id: member_id.clone(),
            members: Vec::new(),
        }
    } else {
        let request = Join {
            member_id: member_id.clone(),
            client_id: header.client_id.clone(),
            session_timeout_ms,
            rebalance_timeout_ms,
            protocol_type,
            protocols,
            require_member_id: version >= 4,
        };
        let answer = state.coordinator.join(
            &session.account,
            &stored_group(session, &group),
            request,
            Instant::now(),
        );
        match answer {
            Answer::Now(joined) => joined,
            Answer::Later(rx) => rx.await.unwrap_or(crate::group::Joined {
                error: UNKNOWN_MEMBER_ID,
                generation: -1,
                protocol: String::new(),
                leader: String::new(),
                member_id,
                members: Vec::new(),
            }),
        }
    };
    let mut out = Writer::default();
    if version >= 2 {
        out.i32(0);
    }
    out.i16(joined.error)
        .i32(joined.generation)
        .string(&joined.protocol)
        .string(&joined.leader)
        .string(&joined.member_id);
    out.array(&joined.members, |out, (id, metadata)| {
        out.string(id);
        if version >= 5 {
            out.nullable_string(None);
        }
        out.bytes(metadata);
    });
    Ok(out.bytes)
}

async fn sync_group(
    state: &KafkaState,
    session: &Session,
    reader: &mut Reader<'_>,
    version: i16,
) -> Result<Vec<u8>, Malformed> {
    let group = reader.string()?;
    let generation = reader.i32()?;
    let member_id = reader.string()?;
    if version >= 3 {
        let _instance = reader.nullable_string()?;
    }
    let assignments = reader.array(|r| Ok((r.string()?, r.bytes()?)))?;
    let synced = if !valid_group(&group) {
        crate::group::Synced {
            error: INVALID_GROUP_ID,
            assignment: Vec::new(),
        }
    } else {
        match state.coordinator.sync(
            &stored_group(session, &group),
            generation,
            &member_id,
            assignments,
            Instant::now(),
        ) {
            Answer::Now(synced) => synced,
            Answer::Later(rx) => rx.await.unwrap_or(crate::group::Synced {
                error: REBALANCE_IN_PROGRESS,
                assignment: Vec::new(),
            }),
        }
    };
    let mut out = Writer::default();
    if version >= 1 {
        out.i32(0);
    }
    out.i16(synced.error).bytes(&synced.assignment);
    Ok(out.bytes)
}

fn heartbeat(
    state: &KafkaState,
    session: &Session,
    reader: &mut Reader<'_>,
    version: i16,
) -> Result<Vec<u8>, Malformed> {
    let group = reader.string()?;
    let generation = reader.i32()?;
    let member_id = reader.string()?;
    if version >= 3 {
        let _instance = reader.nullable_string()?;
    }
    let error = if valid_group(&group) {
        state.coordinator.heartbeat(
            &stored_group(session, &group),
            generation,
            &member_id,
            Instant::now(),
        )
    } else {
        INVALID_GROUP_ID
    };
    let mut out = Writer::default();
    if version >= 1 {
        out.i32(0);
    }
    out.i16(error);
    Ok(out.bytes)
}

fn leave_group(
    state: &KafkaState,
    session: &Session,
    reader: &mut Reader<'_>,
    version: i16,
) -> Result<Vec<u8>, Malformed> {
    let group = reader.string()?;
    let members: Vec<String> = if version >= 3 {
        reader.array(|r| {
            let id = r.string()?;
            let _instance = r.nullable_string()?;
            Ok(id)
        })?
    } else {
        vec![reader.string()?]
    };
    let errors = if valid_group(&group) {
        state
            .coordinator
            .leave(&stored_group(session, &group), &members, Instant::now())
    } else {
        vec![INVALID_GROUP_ID; members.len()]
    };
    let mut out = Writer::default();
    if version >= 1 {
        out.i32(0);
    }
    if version >= 3 {
        let error = if valid_group(&group) {
            NONE
        } else {
            INVALID_GROUP_ID
        };
        out.i16(error);
        let answered: Vec<(&String, i16)> = members.iter().zip(errors).collect();
        out.array(&answered, |out, &(id, error)| {
            out.string(id).nullable_string(None).i16(error);
        });
    } else {
        out.i16(errors[0]);
    }
    Ok(out.bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_tokens() {
        assert_eq!(
            plain_token(b"\0AKID\0secret"),
            Some(("AKID", &b"secret"[..]))
        );
        assert_eq!(
            plain_token(b"AKID\0AKID\0secret"),
            Some(("AKID", &b"secret"[..]))
        );
        assert_eq!(
            plain_token(b"other\0AKID\0secret"),
            None,
            "acting as another"
        );
        assert_eq!(plain_token(b"\0\0secret"), None, "no user");
        assert_eq!(plain_token(b"\0AKID"), None, "no password");
        assert_eq!(plain_token(b"\0AKID\0se\0cret"), None, "a fourth part");
    }

    #[test]
    fn api_versions_answers_an_unserved_version_in_v0() {
        let answer = api_versions(4);
        let mut reader = Reader::new(&answer);
        assert_eq!(reader.i16(), Ok(UNSUPPORTED_VERSION));
        let apis = reader
            .array(|r| Ok((r.i16()?, r.i16()?, r.i16()?)))
            .unwrap();
        assert_eq!(apis, APIS.to_vec());
        assert_eq!(reader.i8(), Err(Malformed), "no throttle time in v0");

        let answer = api_versions(2);
        let mut reader = Reader::new(&answer);
        assert_eq!(reader.i16(), Ok(NONE));
        reader
            .array(|r| Ok((r.i16()?, r.i16()?, r.i16()?)))
            .unwrap();
        assert_eq!(reader.i32(), Ok(0), "throttle time");
    }
}
