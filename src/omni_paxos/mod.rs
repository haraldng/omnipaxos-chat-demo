//! # The OmniPaxos integration — start reading here
//!
//! This directory is the only code in omni-chat that uses the `omnipaxos`,
//! `omnipaxos_runtime` and `omnipaxos_storage` crates. Everything the chatbot
//! needs from consensus goes through it:
//!
//! * [`entry`] — [`ChatEntry`], exactly what is replicated.
//! * [`log`] — [`ReplicatedLog`]: builds the OmniPaxos config, spawns the async
//!   actor (persistent RocksDB storage), turns leader events into a watchable
//!   [`LeaderState`], exposes the decided log as a stream, and appends entries.
//! * [`endpoint`] — the four touchpoints between OmniPaxos and the network
//!   (outgoing, deliver, link up, wire codec). The network itself (`crate::net`)
//!   knows nothing about OmniPaxos.

pub mod endpoint;
pub mod entry;
pub mod log;

pub use entry::ChatEntry;
pub use log::{DecidedStream, LeaderState, LogConfig, ReplicatedLog};
pub use omnipaxos::util::NodeId;
pub use omnipaxos_runtime::AppendError;
