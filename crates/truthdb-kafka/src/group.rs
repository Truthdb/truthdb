//! The group coordinator: Kafka's classic consumer group protocol, in
//! memory (a restart forgets the groups; their members rejoin, and the
//! offsets they committed are durable in the streams engine).
//!
//! A group rebalances whenever a member joins or leaves: every member
//! rejoins (JoinGroup waits until all have, or the rebalance timeout
//! drops those that have not), the leader is sent the members and their
//! metadata, and its SyncGroup hands every member its assignment. A
//! member that sends no heartbeat within its session timeout is dropped,
//! which rebalances the group again.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::errors::{
    ILLEGAL_GENERATION, INCONSISTENT_GROUP_PROTOCOL, INVALID_SESSION_TIMEOUT, MEMBER_ID_REQUIRED,
    NONE, REBALANCE_IN_PROGRESS, UNKNOWN_MEMBER_ID,
};

/// Kafka's `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
const MIN_SESSION_TIMEOUT: Duration = Duration::from_secs(6);
const MAX_SESSION_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Empty,
    /// Waiting for every member to (re)join, until the deadline.
    Preparing {
        deadline: Instant,
    },
    /// Joined: waiting for the leader's assignments.
    Completing,
    Stable,
}

struct Member {
    /// The protocols it supports, by preference, and its metadata for each.
    protocols: Vec<(String, Vec<u8>)>,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    last_seen: Instant,
    /// Its JoinGroup, waiting for the rebalance to complete.
    join: Option<oneshot::Sender<Joined>>,
    /// Its SyncGroup, waiting for the leader's assignments.
    sync: Option<oneshot::Sender<Synced>>,
    assignment: Vec<u8>,
}

impl Member {
    fn awaiting_join(&self) -> bool {
        self.join.as_ref().is_some_and(|join| !join.is_closed())
    }

    fn awaiting_sync(&self) -> bool {
        self.sync.as_ref().is_some_and(|sync| !sync.is_closed())
    }
}

struct Group {
    state: State,
    generation: i32,
    protocol_type: String,
    protocol: String,
    leader: String,
    members: BTreeMap<String, Member>,
    /// Member ids handed out with MEMBER_ID_REQUIRED, until they expire.
    pending: HashMap<String, Instant>,
}

/// A JoinGroup's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Joined {
    pub error: i16,
    pub generation: i32,
    pub protocol: String,
    pub leader: String,
    pub member_id: String,
    /// Every member and its metadata for the chosen protocol, for the
    /// leader only.
    pub members: Vec<(String, Vec<u8>)>,
}

impl Joined {
    fn error(error: i16, member_id: &str) -> Self {
        Joined {
            error,
            generation: -1,
            protocol: String::new(),
            leader: String::new(),
            member_id: member_id.to_string(),
            members: Vec::new(),
        }
    }
}

/// A SyncGroup's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Synced {
    pub error: i16,
    pub assignment: Vec<u8>,
}

impl Synced {
    fn error(error: i16) -> Self {
        Synced {
            error,
            assignment: Vec::new(),
        }
    }
}

/// A JoinGroup request.
pub struct Join {
    pub member_id: String,
    pub client_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    pub protocols: Vec<(String, Vec<u8>)>,
    /// Whether a member with no id gets one with MEMBER_ID_REQUIRED and
    /// must join again with it (JoinGroup v4 and later).
    pub require_member_id: bool,
}

/// An answer now, or one to wait for.
pub enum Answer<T> {
    Now(T),
    Later(oneshot::Receiver<T>),
}

#[derive(Default)]
pub struct Coordinator {
    groups: Mutex<HashMap<String, Group>>,
}

impl Coordinator {
    pub fn join(&self, group_id: &str, request: Join, now: Instant) -> Answer<Joined> {
        let session_timeout = Duration::from_millis(request.session_timeout_ms.max(0) as u64);
        if !(MIN_SESSION_TIMEOUT..=MAX_SESSION_TIMEOUT).contains(&session_timeout) {
            return Answer::Now(Joined::error(INVALID_SESSION_TIMEOUT, &request.member_id));
        }
        // JoinGroup v0 has no rebalance timeout (-1): the session timeout's.
        let rebalance_timeout = if request.rebalance_timeout_ms < 0 {
            session_timeout
        } else {
            Duration::from_millis(request.rebalance_timeout_ms as u64)
        };
        if request.protocol_type.is_empty() || request.protocols.is_empty() {
            return Answer::Now(Joined::error(
                INCONSISTENT_GROUP_PROTOCOL,
                &request.member_id,
            ));
        }
        let mut groups = self.groups.lock().expect("groups poisoned");
        let group = groups.entry(group_id.to_string()).or_insert_with(|| Group {
            state: State::Empty,
            generation: 0,
            protocol_type: String::new(),
            protocol: String::new(),
            leader: String::new(),
            members: BTreeMap::new(),
            pending: HashMap::new(),
        });
        let others = group
            .members
            .iter()
            .filter(|(id, _)| **id != request.member_id);
        let mut others = others.peekable();
        if others.peek().is_some()
            && (group.protocol_type != request.protocol_type
                || !request.protocols.iter().any(|(name, _)| {
                    group.members.iter().all(|(id, member)| {
                        *id == request.member_id || member.protocols.iter().any(|(n, _)| n == name)
                    })
                }))
        {
            return Answer::Now(Joined::error(
                INCONSISTENT_GROUP_PROTOCOL,
                &request.member_id,
            ));
        }
        let member_id = if request.member_id.is_empty() {
            let id = new_member_id(&request.client_id);
            if request.require_member_id {
                group.pending.insert(id.clone(), now + session_timeout);
                return Answer::Now(Joined::error(MEMBER_ID_REQUIRED, &id));
            }
            id
        } else if group.pending.remove(&request.member_id).is_some()
            || group.members.contains_key(&request.member_id)
        {
            request.member_id
        } else {
            return Answer::Now(Joined::error(UNKNOWN_MEMBER_ID, &request.member_id));
        };
        let (reply, answer) = oneshot::channel();
        let member = group.members.entry(member_id).or_insert_with(|| Member {
            protocols: Vec::new(),
            session_timeout,
            rebalance_timeout,
            last_seen: now,
            join: None,
            sync: None,
            assignment: Vec::new(),
        });
        member.protocols = request.protocols;
        member.session_timeout = session_timeout;
        member.rebalance_timeout = rebalance_timeout;
        member.last_seen = now;
        member.join = Some(reply);
        group.protocol_type = request.protocol_type;
        if !matches!(group.state, State::Preparing { .. }) {
            prepare_rebalance(group, now);
        }
        try_complete_join(group, now);
        Answer::Later(answer)
    }

    pub fn sync(
        &self,
        group_id: &str,
        generation: i32,
        member_id: &str,
        assignments: Vec<(String, Vec<u8>)>,
        now: Instant,
    ) -> Answer<Synced> {
        let mut groups = self.groups.lock().expect("groups poisoned");
        let Some(group) = groups.get_mut(group_id) else {
            return Answer::Now(Synced::error(UNKNOWN_MEMBER_ID));
        };
        if !group.members.contains_key(member_id) {
            return Answer::Now(Synced::error(UNKNOWN_MEMBER_ID));
        }
        if generation != group.generation {
            return Answer::Now(Synced::error(ILLEGAL_GENERATION));
        }
        let member = group.members.get_mut(member_id).expect("checked above");
        member.last_seen = now;
        match group.state {
            State::Empty => Answer::Now(Synced::error(UNKNOWN_MEMBER_ID)),
            State::Preparing { .. } => Answer::Now(Synced::error(REBALANCE_IN_PROGRESS)),
            State::Stable => Answer::Now(Synced {
                error: NONE,
                assignment: member.assignment.clone(),
            }),
            State::Completing if group.leader != member_id => {
                let (reply, answer) = oneshot::channel();
                member.sync = Some(reply);
                Answer::Later(answer)
            }
            State::Completing => {
                let mut assignments: HashMap<String, Vec<u8>> = assignments.into_iter().collect();
                for (id, member) in &mut group.members {
                    member.assignment = assignments.remove(id).unwrap_or_default();
                    if let Some(reply) = member.sync.take() {
                        let _ = reply.send(Synced {
                            error: NONE,
                            assignment: member.assignment.clone(),
                        });
                    }
                }
                group.state = State::Stable;
                Answer::Now(Synced {
                    error: NONE,
                    assignment: group.members[member_id].assignment.clone(),
                })
            }
        }
    }

    pub fn heartbeat(&self, group_id: &str, generation: i32, member_id: &str, now: Instant) -> i16 {
        let mut groups = self.groups.lock().expect("groups poisoned");
        let Some(group) = groups.get_mut(group_id) else {
            return UNKNOWN_MEMBER_ID;
        };
        let Some(member) = group.members.get_mut(member_id) else {
            return UNKNOWN_MEMBER_ID;
        };
        if generation != group.generation {
            return ILLEGAL_GENERATION;
        }
        member.last_seen = now;
        match group.state {
            State::Preparing { .. } => REBALANCE_IN_PROGRESS,
            State::Empty => UNKNOWN_MEMBER_ID,
            State::Completing | State::Stable => NONE,
        }
    }

    /// Removes members; the error for each.
    pub fn leave(&self, group_id: &str, member_ids: &[String], now: Instant) -> Vec<i16> {
        let mut groups = self.groups.lock().expect("groups poisoned");
        let Some(group) = groups.get_mut(group_id) else {
            return vec![UNKNOWN_MEMBER_ID; member_ids.len()];
        };
        let errors = member_ids
            .iter()
            .map(|id| match group.members.remove(id) {
                Some(_) => NONE,
                None => UNKNOWN_MEMBER_ID,
            })
            .collect::<Vec<_>>();
        if errors.contains(&NONE) {
            member_left(group, now);
        }
        errors
    }

    /// Whether an OffsetCommit by this member at this generation is
    /// accepted (NONE), as Kafka's coordinator decides it: a commit outside
    /// any generation (-1, no member) only to a group with no members. A
    /// member's accepted commit counts as its heartbeat.
    pub fn may_commit(
        &self,
        group_id: &str,
        generation: i32,
        member_id: &str,
        now: Instant,
    ) -> i16 {
        let mut groups = self.groups.lock().expect("groups poisoned");
        let Some(group) = groups.get_mut(group_id) else {
            return if generation < 0 {
                NONE
            } else {
                ILLEGAL_GENERATION
            };
        };
        if generation < 0 && group.state == State::Empty {
            return NONE;
        }
        let Some(member) = group.members.get_mut(member_id) else {
            return UNKNOWN_MEMBER_ID;
        };
        if generation != group.generation {
            return ILLEGAL_GENERATION;
        }
        match group.state {
            State::Completing => REBALANCE_IN_PROGRESS,
            _ => {
                member.last_seen = now;
                NONE
            }
        }
    }

    /// Drops members past their session timeout and pending member ids
    /// past theirs, completes rebalances past their deadline, and forgets
    /// groups left with nothing.
    pub fn expire(&self, now: Instant) {
        let mut groups = self.groups.lock().expect("groups poisoned");
        for group in groups.values_mut() {
            group.pending.retain(|_, until| *until > now);
            let before = group.members.len();
            group.members.retain(|_, member| {
                member.awaiting_join()
                    || member.awaiting_sync()
                    || member.last_seen + member.session_timeout > now
            });
            if group.members.len() < before {
                member_left(group, now);
            }
            // A pending member id that expired may be what it waited for.
            if let State::Preparing { deadline } = group.state {
                if now >= deadline {
                    complete_join(group, now);
                } else {
                    try_complete_join(group, now);
                }
            }
        }
        groups.retain(|_, group| !(group.members.is_empty() && group.pending.is_empty()));
    }
}

fn new_member_id(client_id: &str) -> String {
    use ring::rand::SecureRandom;
    let mut bytes = [0u8; 16];
    // Without randomness, ids only need to differ: the clock's nanoseconds.
    if ring::rand::SystemRandom::new().fill(&mut bytes).is_err() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        bytes = nanos.to_le_bytes();
    }
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{client_id}-{hex}")
}

/// Starts a rebalance: every member is to rejoin by the deadline (the
/// longest rebalance timeout of its members); a member waiting for its
/// assignment is told to rejoin.
fn prepare_rebalance(group: &mut Group, now: Instant) {
    let timeout = group
        .members
        .values()
        .map(|member| member.rebalance_timeout)
        .max()
        .unwrap_or_default();
    for member in group.members.values_mut() {
        if let Some(reply) = member.sync.take() {
            let _ = reply.send(Synced::error(REBALANCE_IN_PROGRESS));
        }
    }
    group.state = State::Preparing {
        deadline: now + timeout,
    };
}

fn member_left(group: &mut Group, now: Instant) {
    match group.state {
        State::Preparing { .. } => try_complete_join(group, now),
        State::Completing | State::Stable => {
            prepare_rebalance(group, now);
            try_complete_join(group, now);
        }
        State::Empty => {}
    }
}

/// Completes the rebalance once every member has rejoined and no member
/// id handed out is still to join.
fn try_complete_join(group: &mut Group, now: Instant) {
    if group.pending.is_empty() && group.members.values().all(Member::awaiting_join) {
        complete_join(group, now);
    }
}

/// Completes the rebalance with the members that rejoined: a new
/// generation, its protocol (the leader's most preferred one every member
/// supports) and its leader, sent to every member.
fn complete_join(group: &mut Group, now: Instant) {
    group.members.retain(|_, member| member.awaiting_join());
    group.generation = group.generation.wrapping_add(1);
    if group.members.is_empty() {
        group.state = State::Empty;
        group.protocol.clear();
        group.leader.clear();
        return;
    }
    if !group.members.contains_key(&group.leader) {
        group.leader = group.members.keys().next().expect("not empty").clone();
    }
    let leader = &group.members[&group.leader];
    group.protocol = leader
        .protocols
        .iter()
        .map(|(name, _)| name)
        .find(|name| {
            group
                .members
                .values()
                .all(|member| member.protocols.iter().any(|(n, _)| n == *name))
        })
        .expect("a member joins only with a protocol every member supports")
        .clone();
    let metadata: Vec<(String, Vec<u8>)> = group
        .members
        .iter()
        .map(|(id, member)| {
            let (_, metadata) = member
                .protocols
                .iter()
                .find(|(name, _)| *name == group.protocol)
                .expect("chosen from protocols every member supports");
            (id.clone(), metadata.clone())
        })
        .collect();
    for (id, member) in &mut group.members {
        member.last_seen = now;
        let joined = Joined {
            error: NONE,
            generation: group.generation,
            protocol: group.protocol.clone(),
            leader: group.leader.clone(),
            member_id: id.clone(),
            members: if *id == group.leader {
                metadata.clone()
            } else {
                Vec::new()
            },
        };
        let _ = member
            .join
            .take()
            .expect("every member rejoined")
            .send(joined);
    }
    group.state = State::Completing;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn join(member_id: &str, protocols: &[&str]) -> Join {
        Join {
            member_id: member_id.to_string(),
            client_id: "client".to_string(),
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 30_000,
            protocol_type: "consumer".to_string(),
            protocols: protocols
                .iter()
                .map(|name| (name.to_string(), name.as_bytes().to_vec()))
                .collect(),
            require_member_id: true,
        }
    }

    fn now_answer<T: std::fmt::Debug>(answer: Answer<T>) -> T {
        match answer {
            Answer::Now(value) => value,
            Answer::Later(_) => panic!("expected an answer now"),
        }
    }

    fn later<T>(answer: Answer<T>) -> oneshot::Receiver<T> {
        match answer {
            Answer::Later(rx) => rx,
            Answer::Now(_) => panic!("expected to wait"),
        }
    }

    /// Joins a new member through MEMBER_ID_REQUIRED; its id and the
    /// receiver of its join.
    fn join_new(
        coordinator: &Coordinator,
        protocols: &[&str],
        now: Instant,
    ) -> (String, oneshot::Receiver<Joined>) {
        let first = now_answer(coordinator.join("g", join("", protocols), now));
        assert_eq!(first.error, MEMBER_ID_REQUIRED);
        let rx = later(coordinator.join("g", join(&first.member_id, protocols), now));
        (first.member_id, rx)
    }

    #[test]
    fn two_members_join_sync_and_rebalance_when_one_leaves() {
        let coordinator = Coordinator::default();
        let t = Instant::now();
        let (a, mut a_join) = join_new(&coordinator, &["range"], t);
        let joined = a_join.try_recv().expect("a lone member joins at once");
        assert_eq!((joined.generation, joined.leader.as_str()), (1, a.as_str()));
        assert_eq!(joined.members, vec![(a.clone(), b"range".to_vec())]);
        let synced =
            now_answer(coordinator.sync("g", 1, &a, vec![(a.clone(), b"all".to_vec())], t));
        assert_eq!(synced.assignment, b"all");

        // A second member: the first learns of the rebalance from its
        // heartbeat and rejoins; the join completes with both.
        let first = now_answer(coordinator.join("g", join("", &["range"]), t));
        let b = first.member_id;
        let mut b_join = later(coordinator.join("g", join(&b, &["range"]), t));
        assert!(b_join.try_recv().is_err(), "waits for the first member");
        assert_eq!(coordinator.heartbeat("g", 1, &a, t), REBALANCE_IN_PROGRESS);
        let mut a_join = later(coordinator.join("g", join(&a, &["range"]), t));
        let (a_joined, b_joined) = (a_join.try_recv().unwrap(), b_join.try_recv().unwrap());
        assert_eq!(a_joined.generation, 2);
        assert_eq!(a_joined.leader, a, "the leader stays");
        assert_eq!(a_joined.members.len(), 2);
        assert!(
            b_joined.members.is_empty(),
            "only the leader gets the members"
        );

        // The follower's sync waits for the leader's.
        let mut b_sync = later(coordinator.sync("g", 2, &b, Vec::new(), t));
        assert_eq!(coordinator.may_commit("g", 2, &b, t), REBALANCE_IN_PROGRESS);
        now_answer(coordinator.sync(
            "g",
            2,
            &a,
            vec![(a.clone(), b"p0".to_vec()), (b.clone(), b"p1".to_vec())],
            t,
        ));
        assert_eq!(b_sync.try_recv().unwrap().assignment, b"p1");
        assert_eq!(coordinator.may_commit("g", 2, &b, t), NONE);
        assert_eq!(coordinator.may_commit("g", 1, &b, t), ILLEGAL_GENERATION);
        assert_eq!(coordinator.may_commit("g", -1, "", t), UNKNOWN_MEMBER_ID);

        // The leader leaves: the follower rejoins alone and leads.
        assert_eq!(
            coordinator.leave("g", &[a.clone(), "x".into()], t),
            vec![NONE, UNKNOWN_MEMBER_ID]
        );
        assert_eq!(coordinator.heartbeat("g", 2, &b, t), REBALANCE_IN_PROGRESS);
        let mut b_join = later(coordinator.join("g", join(&b, &["range"]), t));
        let joined = b_join.try_recv().unwrap();
        assert_eq!((joined.generation, joined.leader), (3, b.clone()));
        assert_eq!(coordinator.heartbeat("g", 3, &a, t), UNKNOWN_MEMBER_ID);
    }

    #[test]
    fn a_silent_member_is_dropped_and_a_late_one_misses_the_rebalance() {
        let coordinator = Coordinator::default();
        let t = Instant::now();
        let (a, mut a_join) = join_new(&coordinator, &["range"], t);
        a_join.try_recv().unwrap();
        now_answer(coordinator.sync("g", 1, &a, Vec::new(), t));
        let (b, mut b_join) = join_new(&coordinator, &["range"], t);
        // The first member keeps sending heartbeats but never rejoins: at
        // the rebalance deadline (its 30 s rebalance timeout) the join
        // completes without it.
        coordinator.expire(t + Duration::from_secs(5));
        assert!(b_join.try_recv().is_err());
        let beat = t + Duration::from_secs(25);
        assert_eq!(
            coordinator.heartbeat("g", 1, &a, beat),
            REBALANCE_IN_PROGRESS
        );
        coordinator.expire(t + Duration::from_secs(29));
        assert!(b_join.try_recv().is_err());
        coordinator.expire(t + Duration::from_secs(31));
        let joined = b_join.try_recv().unwrap();
        assert_eq!((joined.generation, joined.leader.as_str()), (2, b.as_str()));
        assert_eq!(coordinator.heartbeat("g", 2, &a, t), UNKNOWN_MEMBER_ID);

        // Stable, then silent past its session timeout: dropped, and the
        // group forgotten.
        now_answer(coordinator.sync("g", 2, &b, Vec::new(), t));
        let late = t + Duration::from_secs(31);
        assert_eq!(coordinator.heartbeat("g", 2, &b, late), NONE);
        coordinator.expire(late + Duration::from_secs(9));
        assert_eq!(coordinator.heartbeat("g", 2, &b, late), NONE, "within 10 s");
        coordinator.expire(late + Duration::from_secs(11));
        assert_eq!(coordinator.heartbeat("g", 2, &b, late), UNKNOWN_MEMBER_ID);
        assert_eq!(
            coordinator.may_commit("g", -1, "", t),
            NONE,
            "group forgotten"
        );
        assert_eq!(coordinator.may_commit("g", 2, &b, t), ILLEGAL_GENERATION);
    }

    #[test]
    fn joins_are_refused_as_kafka_refuses_them() {
        let coordinator = Coordinator::default();
        let t = Instant::now();
        let mut short = join("", &["range"]);
        short.session_timeout_ms = 1000;
        assert_eq!(
            now_answer(coordinator.join("g", short, t)).error,
            INVALID_SESSION_TIMEOUT
        );
        assert_eq!(
            now_answer(coordinator.join("g", join("unknown", &["range"]), t)).error,
            UNKNOWN_MEMBER_ID
        );
        let (_, mut a_join) = join_new(&coordinator, &["range", "roundrobin"], t);
        a_join.try_recv().unwrap();
        let first = now_answer(coordinator.join("g", join("", &["sticky"]), t));
        assert_eq!(
            first.error, INCONSISTENT_GROUP_PROTOCOL,
            "no common protocol"
        );
        // A pending member id expires after the session timeout.
        let first = now_answer(coordinator.join("g", join("", &["roundrobin"]), t));
        coordinator.expire(t + Duration::from_secs(11));
        assert_eq!(
            now_answer(coordinator.join("g", join(&first.member_id, &["roundrobin"]), t)).error,
            UNKNOWN_MEMBER_ID
        );
        // Before v4 a member gets its id with its first join.
        let mut old = join("", &["range"]);
        old.require_member_id = false;
        later(coordinator.join("h", old, t)).try_recv().unwrap();
    }

    #[test]
    fn the_protocol_is_the_leaders_most_preferred_one_all_support() {
        let coordinator = Coordinator::default();
        let t = Instant::now();
        let (a, mut a_join) = join_new(&coordinator, &["sticky", "range", "roundrobin"], t);
        a_join.try_recv().unwrap();
        let (_, mut b_join) = join_new(&coordinator, &["roundrobin", "range"], t);
        let mut a_join =
            later(coordinator.join("g", join(&a, &["sticky", "range", "roundrobin"]), t));
        let joined = a_join.try_recv().unwrap();
        assert_eq!(joined.protocol, "range");
        assert_eq!(b_join.try_recv().unwrap().protocol, "range");
        assert!(
            joined
                .members
                .iter()
                .all(|(_, metadata)| metadata == b"range")
        );
    }
}
