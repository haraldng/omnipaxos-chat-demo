//! One node's replica of the log: the whole OmniPaxos surface the chat app uses.

use std::{path::PathBuf, sync::Arc, time::Duration};

use omnipaxos::{
    messages::Message,
    util::{LogEntry, NodeId},
    ClusterConfig, OmniPaxosConfig, ServerConfig,
};
use omnipaxos_runtime::{
    spawn_actor, AppendError, OmniPaxosEvent, OmniPaxosHandle, RuntimeConfig, TokioRuntime,
};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use serde::Serialize;
use tokio::{sync::watch, task::JoinHandle};
use tracing::info;

use super::ChatEntry;
use crate::chat::now_millis;

#[derive(Clone, Debug)]
pub struct LogConfig {
    pub pid: NodeId,
    pub nodes: Vec<NodeId>,
    /// How often the actor ticks OmniPaxos.
    pub tick_period: Duration,
    /// Election timeout = `election_tick_timeout * tick_period`.
    pub election_tick_timeout: u64,
    /// Each node's RocksDB store lives in `<data_dir>/node<pid>/`; a restarted
    /// node reopens it and recovers its log and promises.
    pub data_dir: PathBuf,
}

impl LogConfig {
    pub fn new(pid: NodeId, nodes: Vec<NodeId>) -> Self {
        Self {
            pid,
            nodes,
            tick_period: Duration::from_millis(10),
            election_tick_timeout: 10,
            data_dir: PathBuf::from("target/omni-chat-data"),
        }
    }

    pub fn storage_path(&self) -> PathBuf {
        self.data_dir.join(format!("node{}", self.pid))
    }
}

/// This node's view of who leads. `accepted` is *this node's* phase: when the
/// leader is us, `accepted == false` means we are still syncing (Prepare).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct LeaderState {
    pub leader: Option<NodeId>,
    pub accepted: bool,
    /// Wall-clock millis when this node observed this state (for failover timing).
    pub since_ms: u64,
}

impl LeaderState {
    pub fn is_ready_leader(&self, me: NodeId) -> bool {
        self.leader == Some(me) && self.accepted
    }
}

/// A replica of the replicated log, backed by an OmniPaxos actor with
/// **persistent** RocksDB storage (a restarted node reopens the same path and
/// keeps its log and promises). Cheap to clone; all clones share the actor.
#[derive(Clone)]
pub struct ReplicatedLog {
    pid: NodeId,
    pub(super) handle: OmniPaxosHandle<ChatEntry>,
    /// The actor's outgoing messages (a shared queue: only the network reads it).
    pub(super) outgoing: async_channel::Receiver<Message<ChatEntry>>,
    /// Fed by the one task that drains OmniPaxos events (also a shared queue,
    /// so nobody else may consume them).
    leader: Arc<watch::Sender<LeaderState>>,
}

impl ReplicatedLog {
    /// Build and spawn the OmniPaxos actor. The returned task keeps
    /// [`ReplicatedLog::leader`] up to date; abort it to fail-stop the node.
    pub fn start(cfg: LogConfig) -> anyhow::Result<(Self, JoinHandle<()>)> {
        let path = cfg.storage_path();
        std::fs::create_dir_all(&path)?;
        let storage_cfg = PersistentStorageConfig::with_path(path.to_string_lossy().into_owned());
        let storage = PersistentStorage::<ChatEntry>::open(storage_cfg);
        let op = OmniPaxosConfig {
            cluster_config: ClusterConfig {
                configuration_id: 1,
                nodes: cfg.nodes.clone(),
                ..Default::default()
            },
            server_config: ServerConfig {
                pid: cfg.pid,
                election_tick_timeout: cfg.election_tick_timeout,
                ..Default::default()
            },
        }
        .build::<ChatEntry, PersistentStorage<ChatEntry>>(storage)?;
        let handle = spawn_actor::<_, _, TokioRuntime>(
            op,
            RuntimeConfig {
                tick_period: cfg.tick_period,
                ..Default::default()
            },
        );
        let log = Self {
            pid: cfg.pid,
            outgoing: handle.outgoing_messages(),
            handle,
            leader: Arc::new(watch::channel(LeaderState::default()).0),
        };
        let events = tokio::spawn(fan_out_events(log.clone()));
        Ok((log, events))
    }

    pub fn pid(&self) -> NodeId {
        self.pid
    }

    /// Resolves once the actor has exited, i.e. every handle is dropped and its
    /// storage is closed (so the store can be reopened). Drains outgoing
    /// messages, so only use it on a crashed node.
    pub fn stopped(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let outgoing = self.outgoing.clone();
        async move { while outgoing.recv().await.is_ok() {} }
    }

    pub fn leader_state(&self) -> LeaderState {
        *self.leader.borrow()
    }

    pub fn leader(&self) -> watch::Receiver<LeaderState> {
        self.leader.subscribe()
    }

    /// Propose `entry` from this node (routed to the leader) and wait until it
    /// is decided. Resolves with its log index.
    pub async fn append(&self, entry: ChatEntry) -> Result<usize, AppendError<ChatEntry>> {
        self.handle.append_notify(entry).await
    }

    pub async fn append_with_timeout(
        &self,
        entry: ChatEntry,
        timeout: Duration,
    ) -> Result<usize, AppendError<ChatEntry>> {
        self.handle.append_notify_with_timeout(entry, timeout).await
    }

    pub async fn decided_idx(&self) -> usize {
        self.handle.decided_idx().await
    }

    /// Every decided entry, in log order, starting at index `from`.
    pub async fn decided(&self, from: usize) -> DecidedStream {
        DecidedStream(self.handle.subscribe_decided(from).await)
    }
}

/// The decided log as a stream.
pub struct DecidedStream(async_channel::Receiver<LogEntry<ChatEntry>>);

impl DecidedStream {
    /// The next decided log slot: `Some(entry)` for a chat entry, `None` for a
    /// slot that carries none (e.g. a stop sign). `Err` when the log is gone.
    pub async fn next(&mut self) -> Result<Option<ChatEntry>, async_channel::RecvError> {
        Ok(match self.0.recv().await? {
            LogEntry::Decided(e) => Some(e),
            _ => None,
        })
    }
}

async fn fan_out_events(log: ReplicatedLog) {
    let events = log.handle.subscribe_events();
    while let Ok(ev) = events.recv().await {
        if let OmniPaxosEvent::LeaderElected { pid, accepted } = ev {
            info!(
                node = log.pid,
                "leader is now node {pid} (accepted={accepted})"
            );
            log.leader.send_replace(LeaderState {
                leader: Some(pid),
                accepted,
                since_ms: now_millis(),
            });
        }
    }
}
