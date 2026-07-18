//! In-memory brute-force throttle for the TDS login path.
//!
//! Each LOGIN7 attempt for a `(login, client IP)` pair is *counted up front*
//! ([`note_attempt`], atomically under the map lock) and, once past a free
//! window, delayed by a growing exponential backoff before the server even
//! looks up the credential. Past a hard [`LOCKOUT_AFTER`] count the attempt is
//! [`Decision::Reject`]ed without checking the credential at all. Counting
//! before the (concurrent) verify is what makes this resist a *parallel* burst:
//! N connections opened in one window observe N distinct counts — not the same
//! stale one — so at most [`LOCKOUT_AFTER`] guesses per pair are ever verified,
//! regardless of concurrency. A successful login clears the pair; a pair idle
//! past [`FORGET_AFTER`] is forgotten. The map is bounded ([`MAX_ENTRIES`]) so a
//! username/IP flood cannot exhaust memory.
//!
//! State is per-process and in memory only: a restart resets every counter, and
//! the plan scopes it to a single instance (no shared/distributed throttle).

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A pair is not delayed until its attempt count exceeds this (room for typos).
const FREE_ATTEMPTS: u32 = 3;
/// Delay imposed on the first throttled attempt; doubles per further failure.
const BASE_DELAY: Duration = Duration::from_millis(100);
/// The delay never exceeds this, so a client cannot pin a connection open.
const MAX_DELAY: Duration = Duration::from_secs(5);
/// Past this many attempts in a window, a pair is refused *without* a credential
/// check — a hard ceiling on guesses per pair that concurrency cannot beat.
const LOCKOUT_AFTER: u32 = 50;
/// A pair with no attempt for this long is forgotten (its counter resets).
const FORGET_AFTER: Duration = Duration::from_secs(900);
/// Hard cap on tracked pairs; a flood evicts the least-recently-seen one.
const MAX_ENTRIES: usize = 4096;

/// What the login path should do with an attempt, per [`LoginThrottle::note_attempt`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Proceed to check the credential after sleeping this long (may be zero).
    Proceed(Duration),
    /// Refuse immediately without checking the credential (too many attempts).
    Reject,
}

/// A cloneable handle to the shared throttle map (one per listener).
#[derive(Clone, Default)]
pub struct LoginThrottle {
    inner: Arc<Mutex<HashMap<(String, IpAddr), Entry>>>,
}

struct Entry {
    attempts: u32,
    last_seen: Instant,
}

impl LoginThrottle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Counts an attempt for the pair and returns what to do with it. The count
    /// is incremented BEFORE the (concurrent, ~30 ms) credential check — the
    /// whole read-decide-increment happens under one lock — so parallel attempts
    /// see distinct counts and cannot all slip through the free window together.
    /// Prunes forgotten pairs and bounds the map as side effects.
    pub fn note_attempt(&self, login: &str, ip: IpAddr) -> Decision {
        let now = Instant::now();
        let key = (login.to_ascii_lowercase(), ip);
        let mut map = self.inner.lock().expect("throttle poisoned");
        map.retain(|_, e| now.saturating_duration_since(e.last_seen) < FORGET_AFTER);
        if !map.contains_key(&key) && map.len() >= MAX_ENTRIES {
            evict_oldest(&mut map);
        }
        let entry = map.entry(key).or_insert(Entry {
            attempts: 0,
            last_seen: now,
        });
        let count = entry.attempts;
        entry.attempts = count.saturating_add(1);
        entry.last_seen = now;
        if count >= LOCKOUT_AFTER {
            Decision::Reject
        } else if count > FREE_ATTEMPTS {
            Decision::Proceed(backoff(count - FREE_ATTEMPTS))
        } else {
            Decision::Proceed(Duration::ZERO)
        }
    }

    /// Clears the pair's history after a successful login.
    pub fn record_success(&self, login: &str, ip: IpAddr) {
        self.inner
            .lock()
            .expect("throttle poisoned")
            .remove(&(login.to_ascii_lowercase(), ip));
    }
}

/// `BASE_DELAY * 2^(steps-1)`, saturating and capped at `MAX_DELAY`.
fn backoff(steps: u32) -> Duration {
    let shift = steps.saturating_sub(1).min(20);
    BASE_DELAY.saturating_mul(1u32 << shift).min(MAX_DELAY)
}

fn evict_oldest(map: &mut HashMap<(String, IpAddr), Entry>) {
    if let Some(key) = map
        .iter()
        .min_by_key(|(_, e)| e.last_seen)
        .map(|(k, _)| k.clone())
    {
        map.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn ip(n: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, n))
    }

    fn proceed(d: Decision) -> Duration {
        match d {
            Decision::Proceed(delay) => delay,
            Decision::Reject => panic!("unexpected Reject"),
        }
    }

    #[test]
    fn backoff_is_monotonic_and_capped() {
        // Called only with steps >= 1. First few steps grow, then it pins at the
        // cap and never exceeds it.
        assert_eq!(backoff(1), BASE_DELAY);
        assert_eq!(backoff(2), BASE_DELAY * 2);
        assert_eq!(backoff(3), BASE_DELAY * 4);
        assert_eq!(backoff(100), MAX_DELAY);
        assert!(backoff(u32::MAX) <= MAX_DELAY);
    }

    #[test]
    fn free_attempts_are_not_delayed_then_backoff_grows() {
        let t = LoginThrottle::new();
        let (login, addr) = ("sa", ip(1));
        // Counts 0..=FREE_ATTEMPTS proceed with no delay (each note_attempt
        // consumes one count), then the delay grows.
        for _ in 0..=FREE_ATTEMPTS {
            assert_eq!(proceed(t.note_attempt(login, addr)), Duration::ZERO);
        }
        // count == FREE_ATTEMPTS + 1: the first delayed attempt.
        assert_eq!(proceed(t.note_attempt(login, addr)), backoff(1));
        assert_eq!(proceed(t.note_attempt(login, addr)), backoff(2));
    }

    #[test]
    fn success_clears_the_pair() {
        let t = LoginThrottle::new();
        let (login, addr) = ("sa", ip(1));
        for _ in 0..(FREE_ATTEMPTS + 3) {
            t.note_attempt(login, addr);
        }
        assert!(proceed(t.note_attempt(login, addr)) > Duration::ZERO);
        t.record_success(login, addr);
        assert_eq!(proceed(t.note_attempt(login, addr)), Duration::ZERO);
    }

    #[test]
    fn pairs_are_independent_by_login_and_ip() {
        let t = LoginThrottle::new();
        for _ in 0..(FREE_ATTEMPTS + 1) {
            t.note_attempt("sa", ip(1));
        }
        assert!(proceed(t.note_attempt("sa", ip(1))) > Duration::ZERO);
        // Different login, same IP — untouched.
        assert_eq!(proceed(t.note_attempt("dbo", ip(1))), Duration::ZERO);
        // Same login, different IP — untouched.
        assert_eq!(proceed(t.note_attempt("sa", ip(2))), Duration::ZERO);
        // Login match is case-insensitive (this attempt shares sa/ip(1)'s count).
        assert!(proceed(t.note_attempt("SA", ip(1))) > Duration::ZERO);
    }

    #[test]
    fn a_hard_cap_rejects_without_a_credential_check_no_matter_the_concurrency() {
        // Counting up front means a parallel burst cannot exceed the cap: the
        // mutex serializes note_attempt, so exactly LOCKOUT_AFTER attempts
        // Proceed (are ever verified) before every further one is Rejected.
        let t = LoginThrottle::new();
        let (login, addr) = ("sa", ip(1));
        let proceeds = (0..(LOCKOUT_AFTER + 25))
            .filter(|_| matches!(t.note_attempt(login, addr), Decision::Proceed(_)))
            .count();
        assert_eq!(proceeds as u32, LOCKOUT_AFTER);
        assert_eq!(t.note_attempt(login, addr), Decision::Reject);
    }

    #[test]
    fn the_map_is_bounded_by_eviction() {
        let t = LoginThrottle::new();
        for n in 0..(MAX_ENTRIES + 100) {
            t.note_attempt(&format!("user{n}"), ip((n % 251) as u8));
        }
        let len = t.inner.lock().unwrap().len();
        assert!(len <= MAX_ENTRIES, "map grew to {len}, past the cap");
    }
}
