//! OmniChat: one shared chat conversation, replicated with OmniPaxos. Any
//! replica accepts messages and serves the transcript; only the leader runs
//! the model.
//!
//! * [`omni_paxos`] — **the OmniPaxos integration** (the only code using the
//!   `omnipaxos*` crates).
//! * [`net`] — moving messages between nodes (in-process or TCP).
//! * [`chat`] — the application: transcript fold, posting, leader-only
//!   generation, LLM engines.
//! * [`server`] — the run modes, the node HTTP API, the in-process cluster,
//!   kill/restart.
//! * [`ui`] — the browser UI and the server that hosts it.

pub mod chat;
pub mod cli;
pub mod net;
pub mod omni_paxos;
pub mod server;
pub mod ui;
