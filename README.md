# OmniPaxos Chat Demo

This is a small demo of how to build a highly available chatbot on top of OmniPaxos: the conversation is a replicated log, the leader runs an open-weight model, and if that node dies another continues the same chat.

Related resources:
- [Blog post](https://omnipaxos.com/blog/building-a-highly-available-chatbot-with-omnipaxos/)
- [Video](https://youtu.be/RWANL2Ishe0)

## Requirements

We used the following:
- Docker (for demo)
- Rust (for development)
- Optional: [Ollama](https://ollama.com) with an open-weight model (e.g. `llama3.1:8b`)

## Demos

Build images and start the demo. This uses the **mock model only** (no Ollama, no install needed). UI on http://127.0.0.1:8080:

```bash
$ docker compose up --build
```

(Note: Running this command for the first time can take a couple of minutes. Plain `docker compose up` always uses the mock model; for a real model see [Real model via Ollama](#real-model-via-ollama).)

Or run locally without Docker:

```bash
$ scripts/ui_demo.sh                 # uses Ollama if it's running, else a mock model
```

The UI starts 3 replicas as separate processes plus a supervising page. You can chat, click a node card to send through that node, **kill** a replica (`SIGKILL`), and **restart** a dead node (it comes back empty and OmniPaxos syncs it from the leader).

### Demo 0: Single server

```bash
$ NODES=1 docker compose up --build
# or: NODES=1 scripts/ui_demo.sh
```

1. Chat a bit and tell it your name.
2. Click **kill** on the only node (or `docker kill` the container). New prompts sit at "unreachable — retrying".
3. Click **restart**. The chat comes back, but the conversation is gone — it lived only in that process's memory.

### Demo 1: Fault tolerance

```bash
$ docker compose up --build
# or: scripts/ui_demo.sh
```

1. Chat a bit (e.g. tell it your name).
2. Ask a longer question, and while the reply is still typing, click **kill** on the leader.
3. The partial reply stays on screen as a dashed "never committed" bubble. Another replica is elected leader and regenerates the answer — still knowing your name from the first message.
4. Click **restart** on the dead node. It catches up from the leader ("caught up" in the event feed) and is part of the majority again.

Wait for a restarted node to show "caught up" before killing another: until then it must not be needed for quorum.

To make mid-reply kills easier with the mock model:

```bash
$ MOCK_DELAY_MS=3000 docker compose up --build
# or: MODEL=mock MOCK_DELAY_MS=3000 scripts/ui_demo.sh
```

### Demo 2: Losing the majority

With the 3-node demo still running:

1. Kill two of the three replicas.
2. Send a new message. It waits and then fails (`503 not committed`) — a single replica cannot make progress without a majority.
3. Restart one of the dead nodes. Once a majority is back, the chat works again.

**Bonus — survive a full cluster restart.** On `main`, replicas use in-memory storage, so killing all three still loses the conversation. Switch to the `persistent-storage` branch (Docker Compose is not supported there yet) and rerun locally:

```bash
$ git checkout persistent-storage
$ scripts/ui_demo.sh
```

Chat a bit, then kill **all** three nodes and restart them all. The conversation is still there — each replica reopens its RocksDB store.

### Real model via Ollama

Locally (Ollama already running on the host):

```bash
$ ollama pull llama3.1:8b
$ MODEL=ollama MODEL_NAME=llama3.1:8b scripts/ui_demo.sh
```

Or with Docker (starts an Ollama container as well):

```bash
$ MODEL=ollama MODEL_NAME=llama3.1:8b docker compose --profile ollama up --build
```

## Development

```bash
$ cargo test
$ cargo run -- demo --nodes 3 --base-port 8080   # what scripts/ui_demo.sh runs
$ cargo run -- --help                            # model / timing flags
```

Everything that touches OmniPaxos is in [`src/omni_paxos/`](src/omni_paxos/), about 280 lines.
