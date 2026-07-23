//! The replication handshake. A standby proves membership by sending, in its
//! [`Hello`], an HMAC-SHA256 of its identity under the cluster's shared secret;
//! the primary verifies it in constant time, epoch-fences the standby, and
//! answers with a [`HelloAck`]. Pure logic — the listener slice runs it after
//! the TLS accept (TLS authenticates the primary to the standby and secures the
//! channel; the shared-secret proof authenticates the standby to the primary).
//!
//! The secret is never sent on the wire — only an HMAC over the Hello's identity
//! fields, which binds the proof to that Hello so a captured proof cannot be
//! replayed with different fields. TLS already prevents on-wire capture/replay,
//! so no challenge-nonce round-trip is needed before the Hello.
//!
//! The whole scheme's secrecy rests on that mandatory TLS: the listener slice
//! MUST complete the TLS accept before calling [`evaluate_hello`], never on a
//! plaintext socket. A pre-auth rejection reveals nothing (no real epoch), and
//! an empty shared secret is refused outright (it would otherwise fail open).

use ring::hmac;

use super::{Hello, HelloAck, REPL_PROTOCOL_VERSION};

/// What the primary checks a standby's [`Hello`] against.
pub struct HandshakeParams<'a> {
    /// The cluster's shared secret (identical on every node). Never sent on the
    /// wire.
    pub shared_secret: &'a [u8],
    /// This cluster's uuid.
    pub cluster_uuid: [u8; 16],
    /// The primary's current replication epoch (bumped on promotion).
    pub primary_epoch: u64,
    /// The primary's current durable WAL tail — the standby cannot request log
    /// beyond it.
    pub primary_flushed_lsn: u64,
}

/// The identity bytes the HMAC binds: `node_id ‖ cluster_uuid ‖ epoch ‖
/// last_received_lsn`.
fn auth_message(
    node_id: u64,
    cluster_uuid: &[u8; 16],
    epoch: u64,
    last_received_lsn: u64,
) -> Vec<u8> {
    let mut msg = Vec::with_capacity(8 + 16 + 8 + 8);
    msg.extend_from_slice(&node_id.to_le_bytes());
    msg.extend_from_slice(cluster_uuid);
    msg.extend_from_slice(&epoch.to_le_bytes());
    msg.extend_from_slice(&last_received_lsn.to_le_bytes());
    msg
}

/// Computes the proof a standby sends in `Hello.auth`: the HMAC-SHA256 of its
/// identity under the shared secret.
pub fn compute_auth(
    shared_secret: &[u8],
    node_id: u64,
    cluster_uuid: &[u8; 16],
    epoch: u64,
    last_received_lsn: u64,
) -> Vec<u8> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, shared_secret);
    let msg = auth_message(node_id, cluster_uuid, epoch, last_received_lsn);
    hmac::sign(&key, &msg).as_ref().to_vec()
}

fn reject(primary_epoch: u64, message: &str) -> HelloAck {
    HelloAck {
        protocol_version: REPL_PROTOCOL_VERSION,
        accepted: false,
        primary_epoch,
        primary_flushed_lsn: 0,
        message: message.to_string(),
    }
}

/// Authenticates and fences a standby's [`Hello`], returning the [`HelloAck`] to
/// send back. The shared-secret proof is verified FIRST (constant-time), so an
/// unauthenticated peer learns nothing about the version / cluster / epoch state
/// (every pre-auth failure is one generic rejection).
pub fn evaluate_hello(hello: &Hello, params: &HandshakeParams) -> HelloAck {
    // An empty shared secret is a misconfiguration that fails OPEN — anyone could
    // compute `compute_auth(b"", …)`. Fail closed. (Config load should also
    // refuse to start a replication node with an empty secret.)
    if params.shared_secret.is_empty() {
        return reject(0, "replication handshake rejected");
    }
    // 1. Shared-secret proof — constant-time HMAC verify, before anything else.
    let key = hmac::Key::new(hmac::HMAC_SHA256, params.shared_secret);
    let msg = auth_message(
        hello.node_id,
        &hello.cluster_uuid,
        hello.epoch,
        hello.last_received_lsn,
    );
    if hmac::verify(&key, &msg, &hello.auth).is_err() {
        // Pre-auth: reveal nothing about the primary's epoch to an
        // unauthenticated peer (0, not the real epoch).
        return reject(0, "replication handshake rejected");
    }
    // The peer holds the shared secret; specific diagnostics are safe now.
    // 2. Protocol version.
    if hello.protocol_version != REPL_PROTOCOL_VERSION {
        return reject(
            params.primary_epoch,
            "replication protocol version mismatch",
        );
    }
    // 3. Cluster identity (defense in depth — the HMAC already bound the uuid, so
    //    this only differs on a shared-secret misconfiguration across clusters).
    if hello.cluster_uuid != params.cluster_uuid {
        return reject(params.primary_epoch, "cluster uuid mismatch");
    }
    // 4. Epoch fence, BOTH directions. A standby AHEAD of us means we are a
    //    stale (demoted) primary — refuse to feed it. A standby BEHIND us is
    //    from an older timeline (it was seeded before a failover, or it IS the
    //    old primary rejoining): its log may contain records the new timeline
    //    never had, and LSNs alone cannot detect that divergence — only an
    //    EQUAL epoch guarantees the standby's log is a prefix of ours (its
    //    seed came from this timeline's backup and it only ever applied this
    //    timeline's stream). Anything else must reseed.
    if hello.epoch > params.primary_epoch {
        return reject(
            params.primary_epoch,
            "this primary's epoch is behind the standby",
        );
    }
    if hello.epoch < params.primary_epoch {
        return reject(
            params.primary_epoch,
            "standby epoch is behind this primary (an older timeline): reseed the \
             standby from a fresh backup",
        );
    }
    HelloAck {
        protocol_version: REPL_PROTOCOL_VERSION,
        accepted: true,
        primary_epoch: params.primary_epoch,
        primary_flushed_lsn: params.primary_flushed_lsn,
        message: "ok".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"a-shared-cluster-secret";
    const UUID: [u8; 16] = [3u8; 16];

    fn params() -> HandshakeParams<'static> {
        HandshakeParams {
            shared_secret: SECRET,
            cluster_uuid: UUID,
            primary_epoch: 4,
            primary_flushed_lsn: 100_000,
        }
    }

    fn hello_with(node_id: u64, epoch: u64, last_received_lsn: u64, auth: Vec<u8>) -> Hello {
        Hello {
            protocol_version: REPL_PROTOCOL_VERSION,
            node_id,
            cluster_uuid: UUID,
            epoch,
            last_received_lsn,
            auth,
        }
    }

    /// A correctly-signed, in-epoch Hello is accepted, and the ack carries the
    /// primary's epoch + durable tail.
    #[test]
    fn a_valid_hello_is_accepted() {
        let auth = compute_auth(SECRET, 9, &UUID, 4, 50_000);
        let hello = hello_with(9, 4, 50_000, auth);
        let ack = evaluate_hello(&hello, &params());
        assert!(ack.accepted, "{}", ack.message);
        assert_eq!(ack.primary_epoch, 4);
        assert_eq!(ack.primary_flushed_lsn, 100_000);
    }

    #[test]
    fn a_wrong_secret_is_rejected_generically() {
        let auth = compute_auth(b"the-wrong-secret", 9, &UUID, 2, 50_000);
        let hello = hello_with(9, 2, 50_000, auth);
        let ack = evaluate_hello(&hello, &params());
        assert!(!ack.accepted);
        assert_eq!(ack.message, "replication handshake rejected");
        // A pre-auth rejection leaks nothing about the primary's epoch.
        assert_eq!(ack.primary_epoch, 0);
    }

    /// An empty shared secret must fail CLOSED — otherwise anyone could compute
    /// the proof under the empty key.
    #[test]
    fn an_empty_shared_secret_fails_closed() {
        let p = HandshakeParams {
            shared_secret: b"",
            cluster_uuid: UUID,
            primary_epoch: 4,
            primary_flushed_lsn: 100_000,
        };
        // Even a proof computed under the empty secret is refused.
        let auth = compute_auth(b"", 9, &UUID, 2, 50_000);
        let hello = hello_with(9, 2, 50_000, auth);
        let ack = evaluate_hello(&hello, &p);
        assert!(!ack.accepted);
        assert_eq!(ack.primary_epoch, 0);
    }

    /// A proof signed for one identity cannot be replayed with different fields:
    /// the HMAC is over the identity, so tampering breaks it.
    #[test]
    fn a_tampered_identity_breaks_the_proof() {
        let auth = compute_auth(SECRET, 9, &UUID, 2, 50_000);
        // Same auth, but the Hello now claims a different node_id.
        let hello = hello_with(10, 2, 50_000, auth);
        let ack = evaluate_hello(&hello, &params());
        assert!(!ack.accepted);
        assert_eq!(ack.message, "replication handshake rejected");
    }

    #[test]
    fn a_version_mismatch_is_rejected_only_after_auth() {
        let auth = compute_auth(SECRET, 9, &UUID, 2, 50_000);
        let mut hello = hello_with(9, 2, 50_000, auth);
        hello.protocol_version = REPL_PROTOCOL_VERSION + 1;
        let ack = evaluate_hello(&hello, &params());
        assert!(!ack.accepted);
        assert_eq!(ack.message, "replication protocol version mismatch");
    }

    #[test]
    fn a_cluster_uuid_mismatch_is_rejected() {
        // A peer that knows the secret but signs a different cluster uuid.
        let other = [9u8; 16];
        let auth = compute_auth(SECRET, 9, &other, 2, 50_000);
        let mut hello = hello_with(9, 2, 50_000, auth);
        hello.cluster_uuid = other;
        let ack = evaluate_hello(&hello, &params());
        assert!(!ack.accepted);
        assert_eq!(ack.message, "cluster uuid mismatch");
    }

    /// A standby at a higher epoch means this primary was demoted (fencing).
    #[test]
    fn a_standby_ahead_in_epoch_fences_a_stale_primary() {
        let auth = compute_auth(SECRET, 9, &UUID, 7, 50_000);
        let hello = hello_with(9, 7, 50_000, auth); // epoch 7 > primary 4
        let ack = evaluate_hello(&hello, &params());
        assert!(!ack.accepted);
        assert_eq!(ack.message, "this primary's epoch is behind the standby");
    }

    #[test]
    fn only_a_standby_at_the_primary_epoch_is_accepted() {
        // Equal epoch = same timeline = a guaranteed log prefix.
        let auth = compute_auth(SECRET, 9, &UUID, 4, 50_000);
        let hello = hello_with(9, 4, 50_000, auth);
        assert!(evaluate_hello(&hello, &params()).accepted);
        // A LOWER epoch is an older timeline — possibly diverged; reseed.
        for epoch in [0u64, 3] {
            let auth = compute_auth(SECRET, 9, &UUID, epoch, 50_000);
            let hello = hello_with(9, epoch, 50_000, auth);
            let ack = evaluate_hello(&hello, &params());
            assert!(!ack.accepted, "epoch {epoch}");
            assert!(
                ack.message.contains("reseed"),
                "epoch {epoch}: {}",
                ack.message
            );
        }
    }
}
