//! Kill and restart replicas from the UI. A dead node can't restart itself,
//! so this runs in the (non-replica) UI process.
//!
//! Restarted nodes reopen the same on-disk OmniPaxos store, so they keep their
//! log and promises. Any entries decided while they were down are synced from
//! the leader on rejoin.

use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    process::Stdio,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use tokio::process::{Child, Command};
use tracing::{info, warn};

use super::{api, LocalCluster};
use crate::{
    chat::{now_millis, Node},
    cli::Common,
    omni_paxos::NodeId,
};

#[async_trait]
pub trait Supervisor: Send + Sync {
    /// Fail-stop the node. Returns the wall-clock millis it was killed at.
    async fn kill(&self, id: NodeId) -> anyhow::Result<u64>;
    /// Start a killed node again (reopens its persistent store).
    async fn restart(&self, id: NodeId) -> anyhow::Result<()>;
}

/// One OS process per replica, like `scripts/ha_chat_demo.sh`.
pub struct ProcessSupervisor {
    exe: PathBuf,
    nodes: u64,
    node_args: Vec<String>,
    log_dir: PathBuf,
    children: Mutex<HashMap<NodeId, Child>>,
}

impl ProcessSupervisor {
    pub fn new(nodes: u64, common: &Common, log_dir: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&log_dir)?;
        Ok(Self {
            exe: std::env::current_exe()?,
            nodes,
            node_args: common.to_args(),
            log_dir,
            children: Mutex::new(HashMap::new()),
        })
    }

    pub fn log_path(&self, id: NodeId) -> PathBuf {
        self.log_dir.join(format!("node{id}.log"))
    }

    pub fn spawn(&self, id: NodeId) -> anyhow::Result<u32> {
        let mut log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path(id))?;
        writeln!(log, "===== starting node {id} =====")?;
        let child = Command::new(&self.exe)
            .args([
                "node",
                "--id",
                &id.to_string(),
                "--nodes",
                &self.nodes.to_string(),
            ])
            .args(&self.node_args)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .kill_on_drop(true)
            .spawn()?;
        let pid = child.id().unwrap_or(0);
        self.children.lock().unwrap().insert(id, child);
        Ok(pid)
    }

    fn is_running(&self, id: NodeId) -> bool {
        self.children
            .lock()
            .unwrap()
            .get_mut(&id)
            .is_some_and(|c| matches!(c.try_wait(), Ok(None)))
    }
}

#[async_trait]
impl Supervisor for ProcessSupervisor {
    async fn kill(&self, id: NodeId) -> anyhow::Result<u64> {
        let mut children = self.children.lock().unwrap();
        let child = children
            .get_mut(&id)
            .ok_or_else(|| anyhow::anyhow!("unknown node {id}"))?;
        let at = now_millis();
        child.start_kill()?; // SIGKILL: no cleanup, no goodbye to peers
        warn!(node = id, "killed (SIGKILL)");
        Ok(at)
    }

    async fn restart(&self, id: NodeId) -> anyhow::Result<()> {
        anyhow::ensure!(!self.is_running(id), "node {id} is still running");
        let pid = self.spawn(id)?;
        info!(node = id, "restarted as pid {pid} (persistent storage)");
        Ok(())
    }
}

/// All replicas in this process (`omni_chat cluster`).
pub struct InProcessSupervisor {
    pub cluster: Arc<LocalCluster>,
    pub base_port: u16,
}

impl InProcessSupervisor {
    /// Serve `node`'s HTTP API; its crash hook goes through the cluster.
    pub fn serve(self: &Arc<Self>, node: Arc<Node>) {
        let pid = node.pid();
        let addr = SocketAddr::from(([0, 0, 0, 0], self.base_port + pid as u16));
        let cluster = self.cluster.clone();
        let crash: Arc<dyn Fn() + Send + Sync> = Arc::new(move || cluster.crash(pid));
        tokio::spawn(async move {
            if let Err(e) = api::serve(addr, node, Some(crash)).await {
                warn!(node = pid, "HTTP server failed: {e:#}");
            }
        });
    }
}

#[async_trait]
impl Supervisor for Arc<InProcessSupervisor> {
    async fn kill(&self, id: NodeId) -> anyhow::Result<u64> {
        let at = now_millis();
        self.cluster.crash(id);
        Ok(at)
    }

    async fn restart(&self, id: NodeId) -> anyhow::Result<()> {
        let node = self.cluster.restart(id).await?;
        self.serve(node);
        Ok(())
    }
}
