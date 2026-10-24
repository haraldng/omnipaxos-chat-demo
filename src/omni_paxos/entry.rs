//! What is replicated: the log entry type OmniPaxos agrees on.

use omnipaxos::{macros::Entry, util::NodeId};
use serde::{Deserialize, Serialize};

use crate::chat::MsgId;

/// The only things that go into the replicated log: decided chat events.
/// Everything else (the transcript, drafts, the model) is derived or local.
///
/// `ts` is wall-clock millis on the proposing node and is **display only** —
/// ordering comes from the log index, and dedupe never looks at `ts`.
#[derive(Entry, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChatEntry {
    User {
        msg_id: MsgId,
        /// Client-supplied idempotency key; retries with the same key collapse.
        client_msg_id: Option<String>,
        content: String,
        client_node: NodeId,
        ts: u64,
    },
    Assistant {
        msg_id: MsgId,
        in_reply_to: MsgId,
        content: String,
        model: String,
        generated_by: NodeId,
        ts: u64,
    },
}
