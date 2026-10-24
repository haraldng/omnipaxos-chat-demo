//! One chat replica: folds the decided log into a transcript, accepts user
//! messages from clients, and (when leader) generates replies.

use std::{
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use tokio::{sync::watch, task::JoinHandle, time::Instant};
use tracing::{info, warn};

use super::{
    generate::{self, Draft},
    llm::LlmEngine,
    log::{ChatDecided, ChatLog},
    message::{new_msg_id, now_millis, ChatMessage, MsgId},
    transcript::{Applied, Transcript},
};
use crate::omni_paxos::{AppendError, ChatEntry, LeaderState, LogConfig, NodeId};

#[derive(Clone, Debug)]
pub struct NodeOptions {
    pub log: LogConfig,
    pub system_prompt: String,
    /// Prompt window: only the last N transcript messages are sent to the model.
    pub max_context_messages: usize,
}

impl NodeOptions {
    pub fn new(pid: NodeId, nodes: Vec<NodeId>) -> Self {
        Self {
            log: LogConfig::new(pid, nodes),
            system_prompt: "You are a concise, helpful assistant.".into(),
            max_context_messages: 50,
        }
    }
}

/// State shared by the HTTP layer, the materializer and the generator.
pub struct Shared {
    pub pid: NodeId,
    pub opts: NodeOptions,
    /// Replicated with OmniPaxos, or a plain local log for a single server.
    pub log: ChatLog,
    pub engine: Arc<dyn LlmEngine>,
    pub transcript: RwLock<Transcript>,
    /// Bumped to `applied_len` after every fold.
    pub version: watch::Sender<usize>,
    /// The reply this node is generating right now (leader only). Streamed to
    /// clients; never written to the log.
    pub draft: watch::Sender<Option<Draft>>,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum PostOutcome {
    Replied {
        user: ChatMessage,
        reply: ChatMessage,
    },
    /// The user message is decided; the reply is not (yet).
    Pending { msg_id: MsgId },
}

impl Shared {
    pub fn leader_state(&self) -> LeaderState {
        self.log.leader_state()
    }

    pub fn read<R>(&self, f: impl FnOnce(&Transcript) -> R) -> R {
        f(&self.transcript.read().unwrap())
    }

    /// Wait until `pred` holds on the transcript, or `deadline` passes.
    pub async fn wait_until(&self, pred: impl Fn(&Transcript) -> bool, deadline: Instant) -> bool {
        let mut rx = self.version.subscribe();
        loop {
            rx.borrow_and_update();
            if self.read(&pred) {
                return true;
            }
            tokio::select! {
                r = rx.changed() => if r.is_err() { return false },
                _ = tokio::time::sleep_until(deadline) => return self.read(&pred),
            }
        }
    }

    /// Commit a user message (from any node) and wait up to `wait` for its reply.
    ///
    /// Retries reuse the same entry (same `msg_id`), so an ambiguous failure
    /// (`Timeout`, `Superseded`) can never produce two user messages: the fold
    /// drops the second copy.
    pub async fn post(
        &self,
        content: String,
        client_msg_id: Option<String>,
        wait: Duration,
    ) -> anyhow::Result<PostOutcome> {
        let deadline = Instant::now() + wait;
        let known = client_msg_id
            .as_deref()
            .and_then(|c| self.read(|t| t.msg_id_for_client_id(c).cloned()));

        let msg_id = match known {
            Some(id) => id,
            None => {
                let msg_id = new_msg_id(self.pid);
                let entry = ChatEntry::User {
                    msg_id: msg_id.clone(),
                    client_msg_id: client_msg_id.clone(),
                    content,
                    client_node: self.pid,
                    ts: now_millis(),
                };
                self.commit(entry, &msg_id, client_msg_id.as_deref(), deadline)
                    .await?
            }
        };

        self.wait_until(|t| t.reply_to(&msg_id).is_some(), deadline)
            .await;
        Ok(self.read(|t| {
            let user = t
                .context_for(&msg_id)
                .and_then(|mut c| c.pop())
                .expect("committed user message is in transcript");
            match t.reply_to(&msg_id) {
                Some(reply) => PostOutcome::Replied {
                    user,
                    reply: reply.clone(),
                },
                None => PostOutcome::Pending {
                    msg_id: msg_id.clone(),
                },
            }
        }))
    }

    /// Append `entry` until it shows up in the local fold. Returns the effective
    /// msg id (which differs if a concurrent retry with the same
    /// `client_msg_id` won).
    async fn commit(
        &self,
        entry: ChatEntry,
        msg_id: &str,
        client_msg_id: Option<&str>,
        deadline: Instant,
    ) -> anyhow::Result<MsgId> {
        let effective = |t: &Transcript| -> Option<MsgId> {
            if t.contains(msg_id) {
                return Some(msg_id.to_string());
            }
            client_msg_id.and_then(|c| t.msg_id_for_client_id(c).cloned())
        };
        let mut backoff = Duration::from_millis(50);
        loop {
            if let Some(id) = self.read(effective) {
                return Ok(id);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                anyhow::bail!(
                    "user message {msg_id} not committed before deadline (no quorum?); \
                     retry with the same client_msg_id"
                );
            }
            match self
                .log
                .append_with_timeout(entry.clone(), remaining.min(Duration::from_secs(5)))
                .await
            {
                // Decided; the materializer may lag slightly behind.
                Ok(_) => {
                    self.wait_until(|t| effective(t).is_some(), deadline).await;
                }
                Err(AppendError::Propose(_)) => anyhow::bail!("propose rejected"),
                Err(AppendError::Shutdown) => anyhow::bail!("node shut down"),
                Err(e) => {
                    warn!(node = self.pid, "append {msg_id} failed ({e:?}), retrying");
                    tokio::time::sleep(backoff.min(remaining)).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                }
            }
        }
    }
}

/// A running replica. Dropping it does not stop it; call [`Node::crash`].
pub struct Node {
    pub shared: Arc<Shared>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    crashed: watch::Sender<bool>,
}

impl Node {
    /// Start the replicated log and the materializer and generator tasks. The
    /// caller connects `shared.log` to a network (`crate::net`).
    pub async fn start(opts: NodeOptions, engine: Arc<dyn LlmEngine>) -> anyhow::Result<Arc<Self>> {
        let pid = opts.log.pid;
        let (log, log_events) = ChatLog::start(opts.log.clone())?;
        let shared = Arc::new(Shared {
            pid,
            opts,
            log,
            engine,
            transcript: RwLock::new(Transcript::default()),
            version: watch::channel(0).0,
            draft: watch::channel(None).0,
        });

        let decided = shared.log.decided(0).await;
        let mut tasks = vec![
            tokio::spawn(materialize(shared.clone(), decided)),
            tokio::spawn(generate::run(shared.clone())),
        ];
        tasks.extend(log_events);
        Ok(Arc::new(Self {
            shared,
            tasks: Mutex::new(tasks),
            crashed: watch::channel(false).0,
        }))
    }

    pub fn pid(&self) -> NodeId {
        self.shared.pid
    }

    pub fn add_task(&self, t: JoinHandle<()>) {
        self.tasks.lock().unwrap().push(t);
    }

    /// Stop everything this node does (fail-stop). Its HTTP server shuts down
    /// via [`Node::crashed`].
    pub fn crash(&self) {
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
        self.crashed.send_replace(true);
        warn!(node = self.pid(), "node crashed");
    }

    pub fn is_crashed(&self) -> bool {
        *self.crashed.borrow()
    }

    /// Resolves once the node has crashed.
    pub fn crashed(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut rx = self.crashed.subscribe();
        async move {
            let _ = rx.wait_for(|c| *c).await;
        }
    }
}

/// Fold every decided entry into the transcript, in log order.
async fn materialize(s: Arc<Shared>, mut decided: ChatDecided) {
    while let Some(slot) = decided.next().await {
        let applied_len = {
            let mut t = s.transcript.write().unwrap();
            let idx = t.applied_len();
            match slot {
                Some(e) => match t.apply(&e) {
                    Applied::Ignored => {
                        info!(
                            node = s.pid,
                            "decided #{idx}: duplicate ignored ({})",
                            describe(&e)
                        )
                    }
                    _ => info!(node = s.pid, "decided #{idx}: {}", describe(&e)),
                },
                None => t.skip(),
            }
            t.applied_len()
        };
        s.version.send_replace(applied_len);
    }
}

fn describe(e: &ChatEntry) -> String {
    let short = |s: &str| s.chars().take(60).collect::<String>();
    match e {
        ChatEntry::User {
            msg_id, content, ..
        } => format!("user {msg_id}: {:?}", short(content)),
        ChatEntry::Assistant {
            in_reply_to,
            content,
            generated_by,
            ..
        } => format!(
            "assistant (by node {generated_by}) -> {in_reply_to}: {:?}",
            short(content)
        ),
    }
}
