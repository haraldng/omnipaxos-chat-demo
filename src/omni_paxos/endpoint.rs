//! How OmniPaxos meets the network: [`net::Endpoint`] for [`ReplicatedLog`].
//!
//! The network only moves opaque messages. These four touchpoints are all
//! OmniPaxos needs from it.

use async_trait::async_trait;
use omnipaxos::{messages::Message, util::NodeId};

use super::{ChatEntry, ReplicatedLog};
use crate::net;

#[async_trait]
impl net::Endpoint for ReplicatedLog {
    /// Whole OmniPaxos messages, including the runtime's own `AsyncRuntime`
    /// control messages (`TaggedProposal` / `Assigned`).
    type Msg = Message<ChatEntry>;

    fn id(&self) -> NodeId {
        self.pid()
    }

    fn destination(m: &Self::Msg) -> NodeId {
        m.get_receiver()
    }

    fn source(m: &Self::Msg) -> NodeId {
        m.get_sender()
    }

    /// 1. What OmniPaxos wants sent.
    async fn next_outgoing(&self) -> Option<Self::Msg> {
        self.outgoing.recv().await.ok()
    }

    /// 2. A message arrived for this node.
    async fn deliver(&self, m: Self::Msg) {
        self.handle.handle_incoming(m).await;
    }

    /// 3. A link to `peer` (re)opened. OmniPaxos `reconnected(peer)` sends a
    /// `PrepareReq`; if `peer` leads, it answers with a `Prepare` that syncs
    /// our log. This is what catches up a node restarted with **empty**
    /// storage: with no promise it would otherwise sit idle until the leader's
    /// next write reached it.
    async fn link_up(&self, peer: NodeId) {
        self.handle.reconnected(peer).await;
    }

    /// 4. Wire format (TCP only).
    fn encode(m: &Self::Msg) -> anyhow::Result<Vec<u8>> {
        Ok(bincode::serialize(m)?)
    }

    fn decode(b: &[u8]) -> anyhow::Result<Self::Msg> {
        Ok(bincode::deserialize(b)?)
    }
}
