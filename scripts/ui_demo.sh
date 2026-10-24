#!/usr/bin/env bash
# Browser demo: 3 replicas as separate OS processes + the chat UI, which can
# kill (SIGKILL) and restart them. Restarted nodes reopen their on-disk store;
# missed entries sync from the leader.
#
#   scripts/ui_demo.sh                  # Ollama if it's running, else the mock model
#   MODEL=mock scripts/ui_demo.sh       # force the mock model
#   NO_OPEN=1 scripts/ui_demo.sh        # don't open a browser
#   MODEL=mock MOCK_DELAY_MS=3000 ...   # slow mock: time to kill the leader mid-reply
#
# Rule of thumb for the demo: only one node down or catching up at a time.
set -euo pipefail

cd "$(dirname "$0")/.."
if [[ -z "${MODEL:-}" ]]; then
  if curl -sf -m 1 http://127.0.0.1:11434/api/tags >/dev/null; then MODEL=ollama; else MODEL=mock; fi
fi
MODEL_NAME=${MODEL_NAME:-llama3.1:8b}
BASE_PORT=${BASE_PORT:-8080}
NODES=${NODES:-3}
LOG_DIR=${LOG_DIR:-target/demo-logs}
BIN=target/debug/omni_chat

cargo build -q
DATA_DIR=${DATA_DIR:-target/omni-chat-data}
mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/node*.log
# Each run starts a fresh conversation; KEEP_DATA=1 resumes the last one.
[[ -z "${KEEP_DATA:-}" ]] && rm -rf "$DATA_DIR"

if [[ $MODEL == ollama ]]; then echo "model: ollama ($MODEL_NAME)"; else echo "model: $MODEL"; fi
RUST_LOG=${RUST_LOG:-info} "$BIN" demo --nodes "$NODES" --base-port "$BASE_PORT" \
  --model "$MODEL" --model-name "$MODEL_NAME" --mock-delay-ms "${MOCK_DELAY_MS:-0}" \
  --log-dir "$LOG_DIR" --data-dir "$DATA_DIR" 2>"$LOG_DIR/ui.log" &
DEMO=$!
trap 'kill $DEMO 2>/dev/null || true' EXIT

UI=http://127.0.0.1:$BASE_PORT
for _ in $(seq 1 50); do curl -sf -m 1 "$UI/config.json" >/dev/null && break; sleep 0.1; done
echo
echo "UI: $UI   (Ctrl-C to stop everything)"
[[ -z "${NO_OPEN:-}" ]] && command -v open >/dev/null && open "$UI"
wait $DEMO
