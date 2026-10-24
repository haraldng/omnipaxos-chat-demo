//! The application replicated with OmniPaxos: one shared chat conversation.
//!
//! The log (`crate::omni_paxos`) holds only user messages and final replies. Each
//! replica folds it into the same [`Transcript`]; only the leader runs the
//! model ([`generate`]), streaming its in-progress reply as a draft.

pub mod generate;
pub mod llm;
pub mod log;
pub mod message;
pub mod node;
pub mod transcript;

pub use message::{new_msg_id, now_millis, render, ChatMessage, MsgId, Role};
pub use node::{Node, NodeOptions, PostOutcome, Shared};
pub use transcript::Transcript;
