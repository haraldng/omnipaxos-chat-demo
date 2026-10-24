//! N chat replicas in one process, connected by an in-process network.
//! Used by the tests and by `omni_chat cluster`.

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use tokio::time::Instant;
use tracing::info;

use crate::{
    chat::{llm::LlmEngine, Node, NodeOptions},
    net::LocalNet,
    omni_paxos::{LeaderState, NodeId, ReplicatedLog},
};

pub struct LocalCluster {
    pub net: Arc<LocalNet<ReplicatedLog>>,
    /// Behind a lock so a crashed node can be replaced by a restarted one.
    nodes: RwLock<BTreeMap<NodeId, Arc<Node>>>,
}

impl LocalCluster {
    pub async fn start(
        n: u64,
        mut configure: impl FnMut(&mut NodeOptions),
        engine: impl Fn(NodeId) -> Arc<dyn LlmEngine>,
    ) -> anyhow::Result<Self> {
        let pids: Vec<NodeId> = (1..=n).collect();
        let net = LocalNet::new();
        let mut nodes = BTreeMap::new();
        for &pid in &pids {
            let mut opts = NodeOptions::new(pid, pids.clone());
            configure(&mut opts);
            let node = Node::start(opts, engine(pid)).await?;
            if let Some(log) = node.shared.log.replicated() {
                node.add_task(net.register(log.clone()));
            }
            nodes.insert(pid, node);
        }
        Ok(Self {
            net,
            nodes: RwLock::new(nodes),
        })
    }

    pub fn node(&self, pid: NodeId) -> Arc<Node> {
        self.nodes.read().unwrap()[&pid].clone()
    }

    pub fn nodes(&self) -> Vec<Arc<Node>> {
        self.nodes.read().unwrap().values().cloned().collect()
    }

    pub fn alive(&self) -> Vec<Arc<Node>> {
        self.nodes()
            .into_iter()
            .filter(|n| !n.is_crashed())
            .collect()
    }

    /// Wait until every live node agrees on one leader that is in Accept phase.
    pub async fn wait_for_leader(&self, timeout: Duration) -> anyhow::Result<NodeId> {
        let deadline = Instant::now() + timeout;
        loop {
            let states: Vec<LeaderState> = self
                .alive()
                .iter()
                .map(|n| n.shared.leader_state())
                .collect();
            if let Some(l) = states.first().and_then(|s| s.leader) {
                let agreed = states.iter().all(|s| s.leader == Some(l));
                let ready =
                    self.nodes().iter().find(|n| n.pid() == l).is_some_and(|n| {
                        !n.is_crashed() && n.shared.leader_state().is_ready_leader(l)
                    });
                if agreed && ready {
                    return Ok(l);
                }
            }
            if Instant::now() > deadline {
                anyhow::bail!("no agreed leader within {timeout:?}: {states:?}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Fail-stop `pid`: cut it off and stop all of its tasks.
    pub fn crash(&self, pid: NodeId) {
        self.net.crash(pid);
        self.node(pid).crash();
    }

    /// Restart a crashed node with **empty** in-memory storage and the same
    /// options. OmniPaxos syncs it from the current leader once it rejoins.
    ///
    /// Safe for the demo as long as at most one node is down or catching up at
    /// a time: a node that forgot its log must not be needed for a quorum
    /// before it has caught up.
    pub async fn restart(&self, pid: NodeId) -> anyhow::Result<Arc<Node>> {
        let old = self.node(pid);
        anyhow::ensure!(old.is_crashed(), "node {pid} is still running");
        let node = Node::start(old.shared.opts.clone(), old.shared.engine.clone()).await?;
        if let Some(log) = node.shared.log.replicated() {
            node.add_task(self.net.register(log.clone()));
        }
        self.nodes.write().unwrap().insert(pid, node.clone());
        self.net.rejoin(pid).await;
        info!(node = pid, "restarted with empty state");
        Ok(node)
    }
}
