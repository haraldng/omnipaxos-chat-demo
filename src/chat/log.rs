//! The log the chat writes to. With several nodes it is the OmniPaxos
//! replicated log; with a single node it is a plain in-memory `Vec` with no
//! replication at all, the "before OmniPaxos" setup: kill that server and the
//! chat is unreachable, restart it and the conversation is gone.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{sync::watch, task::JoinHandle};

use super::now_millis;
use crate::omni_paxos::{
    AppendError, ChatEntry, DecidedStream, LeaderState, LogConfig, NodeId, ReplicatedLog,
};

#[derive(Clone)]
pub enum ChatLog {
    Replicated(ReplicatedLog),
    Single(SingleLog),
}

impl ChatLog {
    /// Replicated with OmniPaxos when there are several nodes (OmniPaxos needs
    /// at least two); a plain local log for one. The returned task, if any,
    /// keeps leader state fresh; abort it to fail-stop the node.
    pub fn start(cfg: LogConfig) -> anyhow::Result<(Self, Option<JoinHandle<()>>)> {
        if cfg.nodes.len() == 1 {
            return Ok((Self::Single(SingleLog::new(cfg.pid)), None));
        }
        let (log, events) = ReplicatedLog::start(cfg)?;
        Ok((Self::Replicated(log), Some(events)))
    }

    /// The OmniPaxos log, if this node is part of a replicated cluster (for
    /// connecting it to the network).
    pub fn replicated(&self) -> Option<&ReplicatedLog> {
        match self {
            Self::Replicated(l) => Some(l),
            Self::Single(_) => None,
        }
    }

    pub fn leader_state(&self) -> LeaderState {
        match self {
            Self::Replicated(l) => l.leader_state(),
            Self::Single(l) => *l.leader.borrow(),
        }
    }

    pub fn leader(&self) -> watch::Receiver<LeaderState> {
        match self {
            Self::Replicated(l) => l.leader(),
            Self::Single(l) => l.leader.subscribe(),
        }
    }

    pub async fn append(&self, entry: ChatEntry) -> Result<usize, AppendError<ChatEntry>> {
        match self {
            Self::Replicated(l) => l.append(entry).await,
            Self::Single(l) => Ok(l.append(entry)),
        }
    }

    pub async fn append_with_timeout(
        &self,
        entry: ChatEntry,
        timeout: Duration,
    ) -> Result<usize, AppendError<ChatEntry>> {
        match self {
            Self::Replicated(l) => l.append_with_timeout(entry, timeout).await,
            Self::Single(l) => Ok(l.append(entry)),
        }
    }

    pub async fn decided_idx(&self) -> usize {
        match self {
            Self::Replicated(l) => l.decided_idx().await,
            Self::Single(l) => *l.len.borrow(),
        }
    }

    /// Every decided entry, in log order, starting at index `from`.
    pub async fn decided(&self, from: usize) -> ChatDecided {
        match self {
            Self::Replicated(l) => ChatDecided::Replicated(l.decided(from).await),
            Self::Single(l) => ChatDecided::Single {
                log: l.clone(),
                len: l.len.subscribe(),
                next: from,
            },
        }
    }
}

/// A single server's log: entries are "decided" as soon as they are appended,
/// because there is nobody to agree with. Lives only in memory.
#[derive(Clone)]
pub struct SingleLog {
    entries: Arc<Mutex<Vec<ChatEntry>>>,
    len: Arc<watch::Sender<usize>>,
    /// Always this node, always ready.
    leader: Arc<watch::Sender<LeaderState>>,
}

impl SingleLog {
    fn new(pid: NodeId) -> Self {
        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
            len: Arc::new(watch::channel(0).0),
            leader: Arc::new(
                watch::channel(LeaderState {
                    leader: Some(pid),
                    accepted: true,
                    since_ms: now_millis(),
                })
                .0,
            ),
        }
    }

    fn append(&self, entry: ChatEntry) -> usize {
        let mut entries = self.entries.lock().unwrap();
        entries.push(entry);
        self.len.send_replace(entries.len());
        entries.len() - 1
    }
}

/// The decided log as a stream, for either kind of [`ChatLog`].
pub enum ChatDecided {
    Replicated(DecidedStream),
    Single {
        log: SingleLog,
        len: watch::Receiver<usize>,
        next: usize,
    },
}

impl ChatDecided {
    /// The next decided log slot: `Some(Some(entry))` for a chat entry,
    /// `Some(None)` for a slot that carries none, `None` once the log is gone.
    pub async fn next(&mut self) -> Option<Option<ChatEntry>> {
        match self {
            Self::Replicated(s) => s.next().await.ok(),
            Self::Single { log, len, next } => {
                let want = *next;
                len.wait_for(|l| *l > want).await.ok()?;
                let entry = log.entries.lock().unwrap()[want].clone();
                *next += 1;
                Some(Some(entry))
            }
        }
    }
}
