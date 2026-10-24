//! How replicas are run and reached: the four `omni_chat` modes, the node HTTP
//! API, the in-process cluster, and the kill/restart supervisor.

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

pub mod api;
pub mod local_cluster;
pub mod supervisor;

pub use local_cluster::LocalCluster;
use supervisor::{InProcessSupervisor, ProcessSupervisor, Supervisor};

use crate::{
    chat::{Node, NodeOptions},
    cli::{Command, Common},
    net, ui,
};

/// Run the mode chosen on the command line until it's stopped.
pub async fn run(command: Command) -> anyhow::Result<()> {
    match command {
        Command::Cluster { nodes, common } => run_cluster(nodes, common).await,
        Command::Demo {
            nodes,
            log_dir,
            common,
        } => run_demo(nodes, log_dir, common).await,
        Command::Ui {
            port: ui_port,
            nodes,
            base_port,
        } => ui::serve(bind_addr(ui_port), node_urls(nodes, base_port), None).await,
        Command::Node {
            id,
            nodes,
            peer_base_port,
            common,
        } => run_node(id, nodes, peer_base_port, common).await,
    }
}

/// `cluster`: N replicas in this process, plus the UI (with kill/restart).
async fn run_cluster(nodes: u64, common: Common) -> anyhow::Result<()> {
    let engine = common.engine();
    let cluster =
        Arc::new(LocalCluster::start(nodes, |o| common.apply(o), |_| engine.clone()).await?);
    let sup = Arc::new(InProcessSupervisor {
        cluster: cluster.clone(),
        base_port: common.base_port,
    });
    for node in cluster.nodes() {
        sup.serve(node);
    }
    // base-port itself is unused by the nodes, so the UI lives there.
    let sup: Arc<dyn Supervisor> = Arc::new(sup);
    tokio::select! {
        r = ui::serve(bind_addr(common.base_port), node_urls(nodes, common.base_port), Some(sup)) => r,
        _ = shutdown_signal() => Ok(()),
    }
}

/// `demo`: N replica processes, supervised by the UI server.
async fn run_demo(nodes: u64, log_dir: std::path::PathBuf, common: Common) -> anyhow::Result<()> {
    let sup = ProcessSupervisor::new(nodes, &common, log_dir)?;
    for id in 1..=nodes {
        let pid = sup.spawn(id)?;
        println!(
            "node {id}: pid {pid}, http://127.0.0.1:{}  (log: {})",
            port(common.base_port, id),
            sup.log_path(id).display()
        );
    }
    let sup: Arc<dyn Supervisor> = Arc::new(sup);
    // Returning drops the supervisor, which kills every node (kill_on_drop).
    tokio::select! {
        r = ui::serve(bind_addr(common.base_port), node_urls(nodes, common.base_port), Some(sup)) => r,
        _ = shutdown_signal() => Ok(()),
    }
}

/// `node`: one replica as its own process, talking TCP to its peers.
async fn run_node(
    id: u64,
    nodes: u64,
    peer_base_port: Option<u16>,
    common: Common,
) -> anyhow::Result<()> {
    anyhow::ensure!((1..=nodes).contains(&id), "--id must be in 1..={nodes}");
    let pids: Vec<u64> = (1..=nodes).collect();
    let mut opts = NodeOptions::new(id, pids.clone());
    common.apply(&mut opts);
    let node = Node::start(opts, common.engine()).await?;

    let peer_base = peer_base_port.unwrap_or(common.base_port + 1000);
    let peers: HashMap<u64, SocketAddr> = pids
        .iter()
        .map(|&p| (p, addr(port(peer_base, p))))
        .collect();
    // A single server has no peers: nothing to replicate, nothing to connect.
    if let Some(log) = node.shared.log.replicated() {
        for t in net::start_tcp(peers[&id], peers, log.clone()).await? {
            node.add_task(t);
        }
    }
    // A real fail-stop, same as `kill -9`: no cleanup, no goodbye to peers.
    let crash: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
        tracing::warn!(
            node = id,
            "crash requested via /admin/crash: aborting process"
        );
        std::process::abort()
    });
    tokio::select! {
        r = api::serve(bind_addr(port(common.base_port, id)), node, Some(crash)) => r,
        _ = shutdown_signal() => Ok(()),
    }
}

fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

/// Like `addr`, but for HTTP servers the browser or `docker run -p` needs to
/// reach from outside this process's network namespace (e.g. in a container).
/// Peer-to-peer TCP and node-to-node loopback calls stay on `addr` above.
fn bind_addr(port: u16) -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], port))
}

fn port(base: u16, id: u64) -> u16 {
    base + u16::try_from(id).expect("node id fits in a port offset")
}

fn node_urls(nodes: u64, base_port: u16) -> Vec<(u64, String)> {
    (1..=nodes)
        .map(|id| (id, format!("http://127.0.0.1:{}", port(base_port, id))))
        .collect()
}

/// Resolves on Ctrl-C or SIGTERM. Returning (instead of being killed by the
/// default SIGTERM handler) runs destructors, which is what makes `demo` kill
/// its node processes.
async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
