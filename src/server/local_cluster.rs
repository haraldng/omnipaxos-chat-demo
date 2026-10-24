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
    /// Default store root, deleted with the cluster (`configure` may point
    /// `data_dir` elsewhere, as `omni_chat cluster` does).
    _data_root: tempfile::TempDir,
}

impl LocalCluster {
    pub async fn start(
        n: u64,
        mut configure: impl FnMut(&mut NodeOptions),
        engine: impl Fn(NodeId) -> Arc<dyn LlmEngine>,
    ) -> anyhow::Result<Self> {
        let pids: Vec<NodeId> = (1..=n).collect();
        let data_root = tempfile::TempDir::new()?;
        let net = LocalNet::new();
        let mut nodes = BTreeMap::new();
        for &pid in &pids {
            let mut opts = NodeOptions::new(pid, pids.clone());
            opts.log.data_dir = data_root.path().to_path_buf();
            configure(&mut opts);
            nodes.insert(pid, Node::start(opts, engine(pid)).await?);
        }
        // Opening each store takes milliseconds. Connecting nodes as they start
        // would elect each newcomer in turn (higher pid wins), so connect them
        // all at once, after every store is open.
        for node in nodes.values() {
            if let Some(log) = node.shared.log.replicated() {
                node.add_task(net.register(log.clone()));
            }
        }
        Ok(Self {
            net,
            nodes: RwLock::new(nodes),
            _data_root: data_root,
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

    /// Restart a crashed node with the same options. It reopens its on-disk
    /// store, so it keeps its log and promises; OmniPaxos syncs whatever was
    /// decided while it was down once it rejoins.
    pub async fn restart(&self, pid: NodeId) -> anyhow::Result<Arc<Node>> {
        let old = self.nodes.write().unwrap().remove(&pid);
        let old = old.ok_or_else(|| anyhow::anyhow!("unknown node {pid}"))?;
        if !old.is_crashed() {
            self.nodes.write().unwrap().insert(pid, old);
            anyhow::bail!("node {pid} is still running");
        }
        let (opts, engine) = (old.shared.opts.clone(), old.shared.engine.clone());
        // The store can only be reopened once the old actor has dropped it,
        // which needs every handle gone: the aborted tasks' and `old`'s own.
        let stopped = old.shared.log.replicated().map(|l| l.stopped());
        old.wait_stopped().await;
        drop(old);
        if let Some(stopped) = stopped {
            tokio::time::timeout(Duration::from_secs(5), stopped)
                .await
                .map_err(|_| anyhow::anyhow!("node {pid}'s old actor did not stop"))?;
        }

        let node = Node::start(opts, engine).await?;
        if let Some(log) = node.shared.log.replicated() {
            node.add_task(self.net.register(log.clone()));
        }
        self.nodes.write().unwrap().insert(pid, node.clone());
        self.net.rejoin(pid).await;
        info!(node = pid, "restarted from its on-disk store");
        Ok(node)
    }
}
