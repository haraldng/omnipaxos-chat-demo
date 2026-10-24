//! Leader-only generation loop.
//!
//! Runs only while this node is leader in Accept phase. It answers the earliest
//! unanswered user message, using the transcript prefix up to that message.
//! Correctness does not rely on this loop being exactly-once: if an old
//! leader's reply is also decided, the fold keeps the first one.
//!
//! While generating, the in-progress reply is published as a [`Draft`] so
//! clients can watch it stream. Drafts are local to the leader and are never
//! logged; if the leader dies, its draft dies with it.

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use serde::Serialize;
use tokio::sync::watch;
use tracing::{info, warn};

use super::{
    message::{new_msg_id, now_millis, ChatMessage, MsgId},
    node::Shared,
};
use crate::omni_paxos::{ChatEntry, LeaderState, NodeId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DraftStatus {
    Streaming,
    /// The reply was decided in the log.
    Committed,
    /// Abandoned (lost leadership, inference error, not decided).
    Aborted,
}

#[derive(Clone, Debug, Serialize)]
pub struct Draft {
    /// Unique per generation attempt on this node.
    pub id: u64,
    pub node: NodeId,
    pub in_reply_to: MsgId,
    pub model: String,
    pub text: String,
    pub status: DraftStatus,
    pub started_ms: u64,
    pub first_token_ms: Option<u64>,
}

static DRAFT_IDS: AtomicU64 = AtomicU64::new(1);

/// Publishes one generation attempt. Marks it `Aborted` on drop unless it
/// was committed, so every early return ends the draft correctly.
struct DraftGuard<'a> {
    tx: &'a watch::Sender<Option<Draft>>,
    finished: bool,
}

impl<'a> DraftGuard<'a> {
    fn start(s: &'a Shared, in_reply_to: &str) -> Self {
        s.draft.send_replace(Some(Draft {
            id: DRAFT_IDS.fetch_add(1, Ordering::Relaxed),
            node: s.pid,
            in_reply_to: in_reply_to.to_string(),
            model: s.engine.model(),
            text: String::new(),
            status: DraftStatus::Streaming,
            started_ms: now_millis(),
            first_token_ms: None,
        }));
        Self {
            tx: &s.draft,
            finished: false,
        }
    }

    fn push(&self, token: &str) {
        if token.is_empty() {
            return;
        }
        self.tx.send_modify(|d| {
            if let Some(d) = d {
                d.first_token_ms.get_or_insert_with(now_millis);
                d.text.push_str(token);
            }
        });
    }

    fn finish(mut self, status: DraftStatus) {
        self.set(status);
        self.finished = true;
    }

    fn set(&self, status: DraftStatus) {
        self.tx.send_modify(|d| {
            if let Some(d) = d {
                d.status = status;
            }
        });
    }
}

impl Drop for DraftGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.set(DraftStatus::Aborted);
        }
    }
}

pub async fn run(s: Arc<Shared>) {
    let mut leader_rx = s.log.leader();
    loop {
        if leader_rx
            .wait_for(|st| st.is_ready_leader(s.pid))
            .await
            .is_err()
        {
            return;
        }
        info!(node = s.pid, "leading: generation loop started");
        let engine = s.engine.clone();
        tokio::spawn(async move { engine.warm_up().await });
        lead(&s, &mut leader_rx).await;
        info!(node = s.pid, "lost leadership: generation loop stopped");
    }
}

async fn lost_leadership(rx: &mut watch::Receiver<LeaderState>, me: u64) {
    let _ = rx.wait_for(|st| !st.is_ready_leader(me)).await;
}

async fn lead(s: &Shared, leader_rx: &mut watch::Receiver<LeaderState>) {
    let mut version_rx = s.version.subscribe();
    // Replies we proposed that are decided (or still pending). Only avoids
    // redundant work on this node.
    let mut in_flight: HashSet<MsgId> = HashSet::new();

    loop {
        if !leader_rx.borrow().is_ready_leader(s.pid) {
            return;
        }
        version_rx.borrow_and_update();
        let job: Option<(MsgId, Vec<ChatMessage>)> = s.read(|t| {
            let u = t.next_unanswered()?;
            if in_flight.contains(&u.msg_id) {
                return None;
            }
            Some((u.msg_id.clone(), t.context_for(&u.msg_id)?))
        });

        let Some((user_id, mut ctx)) = job else {
            tokio::select! {
                _ = lost_leadership(leader_rx, s.pid) => return,
                r = version_rx.changed() => if r.is_err() { return },
            }
            continue;
        };

        let max = s.opts.max_context_messages.max(1);
        if ctx.len() > max {
            ctx.drain(..ctx.len() - max);
        }

        let draft = DraftGuard::start(s, &user_id);
        let on_token = |t: &str| draft.push(t);
        let result = tokio::select! {
            _ = lost_leadership(leader_rx, s.pid) => return,
            r = s.engine.complete(&s.opts.system_prompt, &ctx, &on_token) => r,
        };
        let content = match result {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    node = s.pid,
                    "inference for {user_id} failed: {e:#}; retrying"
                );
                tokio::select! {
                    _ = lost_leadership(leader_rx, s.pid) => return,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                }
            }
        };
        if !leader_rx.borrow().is_ready_leader(s.pid) {
            // Deposed mid-inference; the new leader will answer.
            return;
        }

        let entry = ChatEntry::Assistant {
            msg_id: new_msg_id(s.pid),
            in_reply_to: user_id.clone(),
            content,
            model: s.engine.model(),
            generated_by: s.pid,
            ts: now_millis(),
        };
        in_flight.insert(user_id.clone());
        match s.log.append(entry).await {
            Ok(_) => draft.finish(DraftStatus::Committed),
            Err(e) => {
                // Possibly still decided later; a regenerated duplicate would
                // lose to it in the fold, so retrying is safe.
                warn!(node = s.pid, "reply to {user_id} not confirmed ({e:?})");
                in_flight.remove(&user_id);
            }
        }
    }
}
