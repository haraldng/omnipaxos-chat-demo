//! TCP between OS processes: each message is one length-prefixed frame
//! (`Endpoint::encode`), one outbound connection per peer with reconnects.

use std::{collections::HashMap, net::SocketAddr, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};
use tracing::{debug, info, warn};

use super::{Endpoint, NodeId};

const MAX_FRAME: usize = 64 << 20;

/// Serve `listen` and dial every peer. Returns the spawned tasks.
pub async fn start_tcp<E: Endpoint>(
    listen: SocketAddr,
    peers: HashMap<NodeId, SocketAddr>,
    node: E,
) -> anyhow::Result<Vec<JoinHandle<()>>> {
    let pid = node.id();
    let listener = TcpListener::bind(listen).await?;
    info!(node = pid, "peer transport listening on {listen}");
    let mut tasks = Vec::new();

    let n = node.clone();
    tasks.push(tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((sock, addr)) => {
                    debug!(node = pid, "peer connection from {addr}");
                    tokio::spawn(read_frames(sock, n.clone()));
                }
                Err(e) => warn!(node = pid, "accept failed: {e}"),
            }
        }
    }));

    let mut queues: HashMap<NodeId, mpsc::Sender<E::Msg>> = HashMap::new();
    for (&peer, &addr) in &peers {
        if peer == pid {
            continue;
        }
        let (tx, rx) = mpsc::channel(4096);
        queues.insert(peer, tx);
        tasks.push(tokio::spawn(dial(peer, addr, rx, node.clone())));
    }

    tasks.push(tokio::spawn(async move {
        while let Some(msg) = node.next_outgoing().await {
            if let Some(q) = queues.get(&E::destination(&msg)) {
                // Full queue == slow/dead peer: drop; the protocol resends.
                let _ = q.try_send(msg);
            }
        }
    }));
    Ok(tasks)
}

async fn read_frames<E: Endpoint>(mut sock: TcpStream, node: E) {
    let mut buf = Vec::new();
    let mut announced = false;
    loop {
        let len = match sock.read_u32().await {
            Ok(l) => l as usize,
            Err(_) => return,
        };
        if len > MAX_FRAME {
            warn!("oversized frame ({len} bytes); closing connection");
            return;
        }
        buf.resize(len, 0);
        if sock.read_exact(&mut buf).await.is_err() {
            return;
        }
        match E::decode(&buf) {
            Ok(msg) => {
                let from = E::source(&msg);
                node.deliver(msg).await;
                // First message on a new inbound link: the path from `from`
                // to us is up, so replies to what we send now can arrive.
                // (This is what syncs a node restarted with empty state; see
                // `omni_paxos::endpoint`.)
                if !announced {
                    announced = true;
                    node.link_up(from).await;
                }
            }
            Err(e) => {
                warn!("bad frame: {e:#}");
                return;
            }
        }
    }
}

async fn dial<E: Endpoint>(
    peer: NodeId,
    addr: SocketAddr,
    mut rx: mpsc::Receiver<E::Msg>,
    node: E,
) {
    let me = node.id();
    let mut ever_connected = false;
    loop {
        // Discard what queued up while disconnected; it is stale.
        while rx.try_recv().is_ok() {}
        let sock = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let _ = sock.set_nodelay(true);
        info!(node = me, "connected to node {peer} at {addr}");
        if ever_connected {
            node.link_up(peer).await;
        }
        ever_connected = true;

        let mut w = BufWriter::new(sock);
        while let Some(msg) = rx.recv().await {
            let bytes = match E::encode(&msg) {
                Ok(b) => b,
                Err(e) => {
                    warn!(node = me, "encode failed: {e:#}");
                    continue;
                }
            };
            let ok = w.write_u32(bytes.len() as u32).await.is_ok()
                && w.write_all(&bytes).await.is_ok()
                && (!rx.is_empty() || w.flush().await.is_ok());
            if !ok {
                warn!(node = me, "lost connection to node {peer}");
                break;
            }
        }
    }
}
