#![allow(dead_code)]

use std::{sync::Arc, time::Duration};

use omni_chat::{
    chat::{
        llm::{LlmEngine, MockEngine},
        render, ChatMessage, PostOutcome, Role,
    },
    omni_paxos::NodeId,
    server::LocalCluster,
};
use tokio::time::Instant;

pub const T: Duration = Duration::from_secs(10);

pub async fn cluster(mock_delay: Duration) -> LocalCluster {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "warn".into()))
        .with_test_writer()
        .try_init();
    let engine: Arc<dyn LlmEngine> = Arc::new(MockEngine::with_delay(mock_delay));
    LocalCluster::start(3, |_| {}, |_| engine.clone())
        .await
        .expect("cluster starts")
}

pub async fn chat(c: &LocalCluster, via: NodeId, text: &str) -> (ChatMessage, ChatMessage) {
    match c.node(via).shared.post(text.into(), None, T).await.unwrap() {
        PostOutcome::Replied { user, reply } => (user, reply),
        PostOutcome::Pending { msg_id } => panic!("no reply to {msg_id} within {T:?}"),
    }
}

pub fn transcript(c: &LocalCluster, pid: NodeId) -> Vec<ChatMessage> {
    c.node(pid).shared.read(|t| t.messages())
}

/// Wait until every node in `pids` has folded the same transcript of `len` messages.
pub async fn converged(c: &LocalCluster, pids: &[NodeId], len: usize) -> Vec<ChatMessage> {
    let deadline = Instant::now() + T;
    loop {
        let ts: Vec<_> = pids.iter().map(|&p| transcript(c, p)).collect();
        if ts[0].len() == len && ts.iter().all(|t| *t == ts[0]) {
            return ts[0].clone();
        }
        assert!(
            Instant::now() < deadline,
            "nodes {pids:?} did not converge on {len} messages: {:#?}",
            ts.iter().map(|t| render(t)).collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Every user message has exactly one reply, and it follows it.
pub fn assert_one_reply_each(msgs: &[ChatMessage]) {
    let users = msgs.iter().filter(|m| m.role == Role::User).count();
    let replies = msgs.iter().filter(|m| m.role == Role::Assistant).count();
    assert_eq!(users, replies, "{}", render(msgs));
    for pair in msgs.chunks(2) {
        assert_eq!(pair[0].role, Role::User);
        assert_eq!(pair[1].in_reply_to.as_ref(), Some(&pair[0].msg_id));
    }
}

/// Wait until all of `pids` agree on a ready leader among them.
pub async fn leader_among(c: &LocalCluster, pids: &[NodeId]) -> NodeId {
    let deadline = Instant::now() + T;
    loop {
        let states: Vec<_> = pids
            .iter()
            .map(|&p| c.node(p).shared.leader_state())
            .collect();
        if let Some(l) = states[0].leader {
            if pids.contains(&l)
                && states.iter().all(|s| s.leader == Some(l))
                && c.node(l).shared.leader_state().is_ready_leader(l)
            {
                return l;
            }
        }
        assert!(
            Instant::now() < deadline,
            "no leader among {pids:?}: {states:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub fn others(pid: NodeId) -> Vec<NodeId> {
    [1, 2, 3].into_iter().filter(|&p| p != pid).collect()
}
