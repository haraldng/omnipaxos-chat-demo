//! In-process network: each node's outgoing messages are forwarded straight to
//! the destination, through crash/partition filters.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use tokio::task::JoinHandle;

use super::{Endpoint, NodeId};

pub struct LocalNet<E: Endpoint> {
    peers: RwLock<HashMap<NodeId, E>>,
    /// Nodes whose links are all cut.
    isolated: RwLock<HashSet<NodeId>>,
}

impl<E: Endpoint> LocalNet<E> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            peers: RwLock::new(HashMap::new()),
            isolated: RwLock::new(HashSet::new()),
        })
    }

    /// Register a node and spawn its forwarder (outgoing → peers' incoming).
    pub fn register(self: &Arc<Self>, node: E) -> JoinHandle<()> {
        let pid = node.id();
        self.peers.write().unwrap().insert(pid, node.clone());
        let net = self.clone();
        tokio::spawn(async move {
            while let Some(msg) = node.next_outgoing().await {
                let to = E::destination(&msg);
                if !net.allows(pid, to) {
                    continue;
                }
                let peer = net.peers.read().unwrap().get(&to).cloned();
                if let Some(peer) = peer {
                    peer.deliver(msg).await;
                }
            }
        })
    }

    fn allows(&self, from: NodeId, to: NodeId) -> bool {
        let iso = self.isolated.read().unwrap();
        !iso.contains(&from) && !iso.contains(&to)
    }

    fn snapshot(&self) -> Vec<(NodeId, E)> {
        self.peers
            .read()
            .unwrap()
            .iter()
            .map(|(p, e)| (*p, e.clone()))
            .collect()
    }

    /// Cut every link to and from `pid`; the node itself keeps running.
    pub fn partition(&self, pid: NodeId) {
        self.isolated.write().unwrap().insert(pid);
    }

    /// Undo [`LocalNet::partition`] and tell both sides the links are back.
    pub async fn heal(&self, pid: NodeId) {
        self.isolated.write().unwrap().remove(&pid);
        self.links_up(pid).await;
    }

    /// Bring a restarted `pid` back onto the network. Both sides are told the
    /// links are up; the restarted node's `link_up(peer)` is what gets it
    /// synced (see `omni_paxos::endpoint`).
    pub async fn rejoin(&self, pid: NodeId) {
        self.isolated.write().unwrap().remove(&pid);
        self.links_up(pid).await;
    }

    async fn links_up(&self, pid: NodeId) {
        let peers = self.snapshot();
        let Some(me) = peers
            .iter()
            .find(|(p, _)| *p == pid)
            .map(|(_, e)| e.clone())
        else {
            return;
        };
        for (p, e) in peers.iter().filter(|(p, _)| *p != pid) {
            e.link_up(pid).await;
            me.link_up(*p).await;
        }
    }

    /// Partition `pid` permanently and drop our handle to it.
    pub fn crash(&self, pid: NodeId) {
        self.partition(pid);
        self.peers.write().unwrap().remove(&pid);
    }
}
