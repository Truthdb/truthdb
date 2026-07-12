//! The engine actor: a dedicated OS thread owns the [`Engine`] and a
//! [`SessionManager`], and serves [`EngineCall`]s over a channel. This replaces
//! the shared `Arc<Mutex<Engine>>` — it serializes engine access (as the mutex
//! did) but adds per-connection sessions (transaction state lands in Stage 6's
//! later milestones) and moves the engine's synchronous io_uring work off the
//! async reactor onto its own thread.

use std::collections::HashMap;
use std::sync::mpsc;
use std::thread::JoinHandle;

use tokio::sync::oneshot;

use crate::engine::{Engine, EngineError};
use crate::rel::BatchOutcome;

/// Identifies a connection's session on the engine thread.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SessionId(u64);

/// Per-connection engine-side state. Transaction/isolation/SET state is added
/// in later Stage 6 milestones; for now a session is just an identity.
#[derive(Default)]
struct Session {}

struct SessionManager {
    sessions: HashMap<SessionId, Session>,
    next_id: u64,
}

impl SessionManager {
    fn new() -> Self {
        SessionManager {
            sessions: HashMap::new(),
            next_id: 1,
        }
    }

    fn open(&mut self) -> SessionId {
        let id = SessionId(self.next_id);
        self.next_id += 1;
        self.sessions.insert(id, Session::default());
        id
    }

    fn close(&mut self, id: SessionId) -> Option<Session> {
        self.sessions.remove(&id)
    }
}

/// A message to the engine thread. Each carries a one-shot reply channel the
/// async caller awaits.
enum EngineCall {
    OpenSession {
        reply: oneshot::Sender<SessionId>,
    },
    /// A SQL batch on behalf of a session (TDS path): typed results.
    RunBatch {
        session: SessionId,
        sql: String,
        reply: oneshot::Sender<Result<BatchOutcome, EngineError>>,
    },
    /// A native-protocol command (ES or SQL): rendered text.
    RunNative {
        command: String,
        reply: oneshot::Sender<Result<String, EngineError>>,
    },
    CloseSession {
        session: SessionId,
    },
    Shutdown,
}

/// A cloneable handle to the engine thread. Cheap to clone (shares the sender).
#[derive(Clone)]
pub struct EngineHandle {
    tx: mpsc::Sender<EngineCall>,
}

impl EngineHandle {
    /// Opens a session; returns its id (or a placeholder if the engine is gone).
    pub async fn open_session(&self) -> SessionId {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(EngineCall::OpenSession { reply }).is_err() {
            return SessionId(0);
        }
        rx.await.unwrap_or(SessionId(0))
    }

    /// Runs a SQL batch for a session and returns its typed outcome.
    pub async fn run_batch(
        &self,
        session: SessionId,
        sql: String,
    ) -> Result<BatchOutcome, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(EngineCall::RunBatch {
                session,
                sql,
                reply,
            })
            .map_err(|_| EngineError::Unavailable)?;
        rx.await.map_err(|_| EngineError::Unavailable)?
    }

    /// Runs a native-protocol command (ES or SQL) and returns rendered text.
    pub async fn run_native(&self, command: String) -> Result<String, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(EngineCall::RunNative { command, reply })
            .map_err(|_| EngineError::Unavailable)?;
        rx.await.map_err(|_| EngineError::Unavailable)?
    }

    /// Closes a session (rolling back any open transaction — later milestone).
    pub fn close_session(&self, session: SessionId) {
        let _ = self.tx.send(EngineCall::CloseSession { session });
    }

    /// Asks the engine thread to stop after draining queued calls.
    pub fn shutdown(&self) {
        let _ = self.tx.send(EngineCall::Shutdown);
    }
}

/// Spawns the engine thread and returns a handle plus its join handle.
pub fn spawn_engine(engine: Engine) -> (EngineHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel();
    let join = std::thread::Builder::new()
        .name("truthdb-engine".to_string())
        .spawn(move || engine_loop(engine, rx))
        .expect("spawn engine thread");
    (EngineHandle { tx }, join)
}

fn engine_loop(mut engine: Engine, rx: mpsc::Receiver<EngineCall>) {
    let mut sessions = SessionManager::new();
    while let Ok(call) = rx.recv() {
        match call {
            EngineCall::OpenSession { reply } => {
                let _ = reply.send(sessions.open());
            }
            EngineCall::RunBatch {
                session,
                sql,
                reply,
            } => {
                // The session context (transactions/locks) is threaded here in
                // later milestones; for now every batch is autocommit.
                let _ = &session;
                let _ = reply.send(engine.sql_batch(&sql));
            }
            EngineCall::RunNative { command, reply } => {
                let _ = reply.send(engine.execute(&command));
            }
            EngineCall::CloseSession { session } => {
                sessions.close(session);
            }
            EngineCall::Shutdown => break,
        }
    }
}
