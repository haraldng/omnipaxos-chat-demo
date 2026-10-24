//! The materialized chat view (derived from the log, never replicated itself).

use serde::{Deserialize, Serialize};

use crate::omni_paxos::NodeId;

/// Server-assigned, globally unique message id (`"<node>-<uuid>"`).
pub type MsgId = String;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
}

/// One message in the materialized transcript.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub msg_id: MsgId,
    pub role: Role,
    pub content: String,
    /// Log index this message was decided at.
    pub log_idx: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<MsgId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_msg_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Node that accepted the user request, or that generated the reply.
    pub node: NodeId,
    pub ts: u64,
}

pub fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn new_msg_id(node: NodeId) -> MsgId {
    format!("{node}-{}", uuid::Uuid::new_v4().simple())
}

/// Convenience for tests and logs.
pub fn render(messages: &[ChatMessage]) -> String {
    messages
        .iter()
        .map(|m| {
            let who = match m.role {
                Role::User => "user",
                Role::Assistant => "assistant",
            };
            format!("[{}] {who}: {}", m.log_idx, m.content)
        })
        .collect::<Vec<_>>()
        .join("\n")
}
