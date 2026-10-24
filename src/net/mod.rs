//! Moving messages between nodes. Knows nothing about OmniPaxos: a node is an
//! [`Endpoint`] that produces and consumes opaque messages.
//!
//! * [`local`] — in-process network with crash / partition / rejoin (tests,
//!   `cluster` mode).
//! * [`tcp`] — one OS process per node, length-prefixed frames over TCP.

use async_trait::async_trait;

pub mod local;
pub mod tcp;

pub use local::LocalNet;
pub use tcp::start_tcp;

/// Node ids on the wire (the same `u64` ids the consensus layer uses).
pub type NodeId = u64;

/// What a node must offer the network.
#[async_trait]
pub trait Endpoint: Clone + Send + Sync + 'static {
    type Msg: Send + 'static;

    fn id(&self) -> NodeId;
    fn destination(m: &Self::Msg) -> NodeId;
    fn source(m: &Self::Msg) -> NodeId;

    /// The next message this node wants sent; `None` once it has shut down.
    async fn next_outgoing(&self) -> Option<Self::Msg>;
    /// Hand a message that arrived for this node to it.
    async fn deliver(&self, m: Self::Msg);
    /// Tell the node that its link to `peer` (re)opened.
    async fn link_up(&self, peer: NodeId);

    fn encode(m: &Self::Msg) -> anyhow::Result<Vec<u8>>;
    fn decode(b: &[u8]) -> anyhow::Result<Self::Msg>;
}
