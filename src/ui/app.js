"use strict";

const state = {
  nodes: [],          // {id, url, alive: null|bool, status, lastOk}
  connected: null,    // node id the UI reads from / sends through
  leader: null,
  messages: [],       // transcript from the connected node
  decidedLen: -1,
  pending: [],        // {cid, text, via, note} — sent but not yet in the transcript
  events: [],
  maxDups: 0,
  stream: null,       // {node, es}: SSE subscription to the leader's in-progress reply
  draft: null,        // the leader's in-progress reply (never logged)
  seenDrafts: new Set(),
  ghosts: [],         // drafts that were interrupted before being committed
  failover: null,     // {killed, at, precise, expectResume, electMs, newLeader, resumeMs}
  supervisor: false,  // the UI server can kill/restart nodes
  outage: null,       // single server only: {at, precise, lost, backAt}
  restarting: {},     // id -> {at}: restarted with empty state, not yet caught up
};

const $ = (id) => document.getElementById(id);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const byId = (id) => state.nodes.find((n) => n.id === id);
const esc = (s) => s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const fmt = (s) => esc(s).replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>").replace(/`([^`\n]+)`/g, "<code>$1</code>");
const newId = () => (crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2) + Date.now());
const ms = (v) => (v < 1000 ? `${Math.round(v)} ms` : `${(v / 1000).toFixed(2)} s`);
const draftKey = (d) => `${d.node}:${d.id}`;
// One node means no replication: the "before OmniPaxos" setup.
const single = () => state.nodes.length === 1;
// Re-rendering on every streamed token must not replace elements needlessly:
// a click whose button is swapped between mousedown and mouseup never fires.
const setHTML = (el, html) => { if (el.__html !== html) { el.__html = html; el.innerHTML = html; } };

async function fetchJSON(url, opts = {}, timeoutMs = 1000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const r = await fetch(url, { ...opts, signal: ctrl.signal });
    const body = await r.json().catch(() => ({}));
    return { status: r.status, body };
  } finally {
    clearTimeout(t);
  }
}

function logEvent(text, kind = "") {
  state.events.unshift({ at: new Date(), text, kind });
  state.events.length = Math.min(state.events.length, 40);
  renderEvents();
}

// ---------- polling ----------

async function poll() {
  await Promise.all(state.nodes.map(async (n) => {
    let status = null;
    try {
      const r = await fetchJSON(n.url + "/admin/status", {}, 800);
      if (r.status === 200) status = r.body;
    } catch { /* unreachable */ }
    const alive = status !== null;
    if (n.alive !== null && n.alive !== alive) {
      // (single server: endOutage logs a more telling "back" line)
      if (!(single() && alive)) logEvent(alive ? `node ${n.id} is back` : `node ${n.id} is down`, alive ? "ok" : "bad");
      if (single()) {
        if (!alive && !state.outage) startOutage(n.lastOk, false);
        if (alive) endOutage(status);
      }
      // Killed from outside the UI (e.g. `kill -9` in a terminal): time it from
      // the last successful poll, so the number is an upper bound.
      if (!single() && !alive && n.id === state.leader && state.failover?.killed !== n.id) {
        startFailover(n.id, n.lastOk, false);
      }
    }
    n.alive = alive;
    n.status = status;
    if (alive) n.lastOk = Date.now();
  }));

  const leader = state.nodes.find((n) => n.alive && n.status.is_leader)?.id ?? null;
  const target = leaderLen();
  for (const [id, r] of Object.entries(state.restarting)) {
    const n = byId(Number(id));
    if (n.alive && n.status.decided_len >= target && target >= 0) {
      logEvent(`node ${id} caught up: ${n.status.decided_len} entries synced in ${ms(Date.now() - r.at)}`, "ok");
      delete state.restarting[id];
    }
  }
  if (leader !== state.leader) {
    if (single()) {
      if (leader !== null) logEvent(`node ${leader} is serving (single server, no replication)`, "ok");
    } else if (leader !== null) logEvent(`node ${leader} elected leader`, "ok");
    else logEvent("no leader — election in progress", "warn");
    state.leader = leader;
  }
  noteElection(leader);
  ensureStream(leader);

  const dups = Math.max(0, ...state.nodes.filter((n) => n.alive).map((n) => n.status.duplicates_ignored));
  if (dups > state.maxDups) {
    logEvent(`fold ignored ${dups - state.maxDups} duplicate entr${dups - state.maxDups === 1 ? "y" : "ies"}`, "warn");
    state.maxDups = dups;
  }

  if (!byId(state.connected)?.alive) {
    const next = state.nodes.find((n) => n.alive);
    if (next && next.id !== state.connected) {
      logEvent(`node ${state.connected} unreachable — UI switched to node ${next.id}`, "warn");
      connect(next.id, false);
    }
  }

  await refreshTranscript();
  render();
}

async function refreshTranscript() {
  const c = byId(state.connected);
  if (!c?.alive || c.status.decided_len === state.decidedLen) return;
  try {
    const r = await fetchJSON(c.url + "/v1/chat", {}, 2000);
    if (r.status === 200) {
      state.messages = r.body.messages;
      state.decidedLen = r.body.decided_len;
    }
  } catch { /* next tick */ }
}

async function pollLoop() {
  for (;;) {
    await poll();
    await sleep(400);
  }
}

// ---------- streaming (the leader's in-progress reply) ----------

function ensureStream(leader) {
  if (state.stream?.node === leader) return;
  closeStream(`node ${state.stream?.node} is no longer leader`);
  if (leader === null) return;
  const es = new EventSource(byId(leader).url + "/v1/stream");
  state.stream = { node: leader, es };
  es.onmessage = (e) => onDraft(JSON.parse(e.data));
  es.onerror = () => {
    if (state.stream?.es === es) closeStream(`node ${leader} died mid-reply`);
  };
}

function closeStream(reason) {
  if (!state.stream) return;
  state.stream.es.close();
  const d = state.draft;
  if (d && d.node === state.stream.node && d.status === "streaming") interrupt(d, reason);
  state.stream = null;
}

function onDraft(d) {
  if (!d) return;
  const key = draftKey(d);
  if (d.status === "streaming") {
    state.seenDrafts.add(key);
    state.draft = d;
    noteResume(d);
  } else if (d.status === "committed") {
    if (state.draft && draftKey(state.draft) === key) state.draft = d; // shown until the log has it
    state.decidedLen = -1;
    refreshTranscript().then(() => render());
  } else if (d.status === "aborted") {
    // Only drafts we watched stream; replayed old ones on (re)connect are ignored.
    if (state.seenDrafts.has(key)) interrupt(d, `node ${d.node} abandoned it (lost leadership)`);
  }
  render();
}

function interrupt(d, reason) {
  const key = draftKey(d);
  if (d.text && !state.ghosts.some((g) => g.key === key)) {
    state.ghosts.push({ key, node: d.node, in_reply_to: d.in_reply_to, text: d.text, reason });
    logEvent(`reply from node ${d.node} interrupted after ${d.text.length} chars — never committed`, "bad");
  }
  if (state.draft && draftKey(state.draft) === key) state.draft = null;
}

// ---------- single-server downtime ----------

function startOutage(at, precise) {
  state.outage = { at, precise, lost: state.messages.length, backAt: null };
  renderFailover();
}

function endOutage(status) {
  const o = state.outage;
  if (!o || o.backAt !== null) return;
  o.backAt = Date.now();
  o.empty = status.decided_len === 0;
  logEvent(o.empty
    ? `node 1 is back after ${ms(o.backAt - o.at)} — with an empty conversation (${o.lost} messages lost)`
    : `node 1 is back after ${ms(o.backAt - o.at)}`, o.empty ? "bad" : "ok");
}

// ---------- failover timing ----------

function startFailover(killed, at, precise) {
  const last = state.messages[state.messages.length - 1];
  const expectResume = (state.draft?.node === killed && state.draft.status === "streaming") || last?.role === "user";
  state.failover = { killed, at, precise, expectResume, electMs: null, newLeader: null, resumeMs: null };
  renderFailover();
}

function noteElection(leader) {
  const f = state.failover;
  if (!f || f.electMs !== null || leader === null) return;
  // Classic failover: someone else took over while the killed node is still down.
  // If the killed node is already back (or is leading again), this was an outage
  // you recovered by restarting — clear the running timer instead of hanging on
  // "electing…" forever (the old guard `leader === f.killed` caused that).
  if (leader === f.killed || byId(f.killed)?.alive) {
    state.failover = null;
    logEvent(`cluster serving again on node ${leader} — downtime timer cleared`, "ok");
    return;
  }
  f.newLeader = leader;
  f.electMs = Math.max(0, byId(leader).status.leader_since_ms - f.at);
  logEvent(`failover: node ${leader} leading ${ms(f.electMs)} after node ${f.killed} died`, "ok");
}

function noteResume(d) {
  const f = state.failover;
  if (!f || !f.expectResume || f.resumeMs !== null || d.node === f.killed || !d.first_token_ms || d.started_ms < f.at) return;
  f.resumeMs = Math.max(0, d.first_token_ms - f.at);
  logEvent(`reply resumed on node ${d.node} ${ms(f.resumeMs)} after the crash`, "ok");
}

// ---------- actions ----------

function connect(id, announce = true) {
  if (state.connected === id) return;
  state.connected = id;
  state.decidedLen = -1;
  if (announce) logEvent(`UI now talking to node ${id}`);
  render();
}

function otherNode(id) {
  const alive = state.nodes.filter((n) => n.alive && n.id !== id);
  if (alive.length) return (alive.find((n) => n.id === state.connected) ?? alive[0]).id;
  const i = state.nodes.findIndex((n) => n.id === id);
  return state.nodes[(i + 1) % state.nodes.length].id;
}

async function send(text) {
  const p = { cid: newId(), text, via: state.connected, note: `sending via node ${state.connected}` };
  state.pending.push(p);
  render(true);
  for (let attempt = 1; ; attempt++) {
    const n = byId(p.via);
    try {
      const r = await fetchJSON(n.url + "/v1/chat", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ content: text, client_msg_id: p.cid, wait_ms: 120000 }),
      }, 125000);
      if (r.status === 200 || r.status === 202) break;
      p.note = `node ${p.via}: ${r.body.error ?? "HTTP " + r.status} — retrying`;
    } catch {
      if (single()) {
        // Nobody else to try: keep retrying the one server, but say so once.
        if (!p.unreachable) logEvent(`can't reach node ${p.via} — the chatbot is unavailable`, "bad");
        p.unreachable = true;
        p.note = `node ${p.via} unreachable — retrying`;
        render();
        await sleep(Math.min(2000, 250 * attempt));
        continue;
      }
      const next = otherNode(p.via);
      logEvent(`request via node ${p.via} failed — retrying via node ${next} (same client_msg_id)`, "warn");
      p.via = next;
      p.note = `retrying via node ${next}`;
    }
    render();
    await sleep(Math.min(2000, 250 * attempt));
  }
  state.decidedLen = -1; // force a transcript refresh
  await poll();
}

/** Log length of the node(s) furthest ahead — what a restarted node must reach. */
function leaderLen() {
  const lens = state.nodes.filter((n) => n.alive && n.status).map((n) => n.status.decided_len);
  return lens.length ? Math.max(...lens) : -1;
}

async function kill(id) {
  const wasLeader = id === state.leader;
  logEvent(`killing node ${id}${wasLeader && !single() ? " (the leader)" : ""}`, "bad");
  let at = Date.now() + 50, precise = false;
  // Through the supervisor it's a real SIGKILL; otherwise the node aborts itself.
  const url = state.supervisor ? `/api/nodes/${id}/kill` : byId(id).url + "/admin/crash";
  try {
    const r = await fetchJSON(url, { method: "POST" }, 1000);
    if (r.body.aborting_at_ms) { at = r.body.aborting_at_ms; precise = true; }
  } catch { /* it died */ }
  delete state.restarting[id];
  if (single()) { state.outage = null; startOutage(at, precise); return; }
  if (wasLeader) startFailover(id, at, precise);
  else logEvent(`node ${id} was a follower — no failover needed`);
}

async function restart(id) {
  const down = state.nodes.filter((n) => n.alive === false && n.id !== id).length;
  if (down) logEvent(`note: ${down} other node(s) still down — restart one at a time`, "warn");
  logEvent(`restarting node ${id} with empty state`, "warn");
  try {
    const r = await fetchJSON(`/api/nodes/${id}/restart`, { method: "POST" }, 3000);
    if (r.status !== 200) { logEvent(`restart failed: ${r.body.error ?? r.status}`, "bad"); return; }
    // A single server has nobody to catch up from: it just starts empty.
    if (!single()) state.restarting[id] = { at: Date.now() };
  } catch { logEvent(`restart of node ${id} failed`, "bad"); }
}

// ---------- rendering ----------

let lastChatKey = "";

function render(forceScroll = false) {
  renderHeader();
  renderFailover();
  renderNodes();
  renderLog();
  renderChat(forceScroll);
}

function renderHeader() {
  let l = state.leader !== null ? `leader: node ${state.leader}` : "leader: electing…";
  if (single()) l = state.leader !== null ? "single server · no replication" : "single server · down";
  const text = `via node ${state.connected} · ${l}`;
  if ($("conn").textContent !== text) $("conn").textContent = text;
}

function renderFailover() {
  if (single()) return renderDowntime();
  const f = state.failover;
  if (!f) {
    setHTML($("failover"), `<div class="big idle">Kill the leader to measure how fast the cluster recovers.</div>`);
    return;
  }
  const bound = f.precise ? "" : "≤ ";
  if (f.electMs === null) {
    setHTML($("failover"), `<div class="big">${ms(Date.now() - f.at)}</div>
      <div class="sub">node ${f.killed} died · electing a new leader…</div>`);
    return;
  }
  let sub = `node ${f.killed} died → <b>node ${f.newLeader}</b> leading`;
  if (f.expectResume) {
    sub += f.resumeMs !== null
      ? ` · reply resumed after <b>${bound}${ms(f.resumeMs)}</b>`
      : ` · waiting for the reply to resume…`;
  }
  setHTML($("failover"), `<div class="big">${bound}${ms(f.electMs)}</div><div class="sub">${sub}</div>`);
}

function renderDowntime() {
  const o = state.outage;
  if (!o) {
    setHTML($("failover"), `<div class="big idle">Single server: if it dies, nothing can take over.</div>`);
    return;
  }
  const bound = o.precise ? "" : "≤ ";
  if (o.backAt === null) {
    setHTML($("failover"), `<div class="big bad">down for ${ms(Date.now() - o.at)}</div>
      <div class="sub">node 1 died · no other server can take over · the chat is unreachable</div>`);
    return;
  }
  const lost = o.empty ? ` · <b>conversation lost</b> (${o.lost} messages)` : "";
  setHTML($("failover"), `<div class="big bad">${bound}${ms(o.backAt - o.at)}</div>
    <div class="sub">of downtime until node 1 was back${lost}</div>`);
}

function renderNodes() {
  const box = $("nodes");
  if (box.children.length !== state.nodes.length) {
    box.innerHTML = state.nodes.map((n) => `<div class="node" data-connect="${n.id}">
      <div class="name">Node ${n.id} <span class="badge"></span></div>
      <div class="stat"></div>
      <button data-kill="${n.id}">kill</button>
    </div>`).join("");
    box.__html = null;
  }
  state.nodes.forEach((n, i) => {
    const card = box.children[i];
    const down = n.alive === false;
    const target = leaderLen();
    const syncing = !down && n.status && state.restarting[n.id] && n.status.decided_len < target;
    let role = down ? "down" : syncing ? "syncing" : n.status?.is_leader ? "leader" : "follower";
    if (single() && role === "leader") role = "serving";
    const stat = down ? "unreachable"
      : !n.status ? "…"
      : syncing ? `log ${n.status.decided_len} / ${target}` : `log ${n.status.decided_len}`;
    card.classList.toggle("down", down);
    card.classList.toggle("connected", n.id === state.connected);
    const badge = card.querySelector(".badge");
    if (badge.textContent !== role) { badge.textContent = role; badge.className = `badge ${role}`; }
    const st = card.querySelector(".stat");
    if (st.textContent !== stat) st.textContent = stat;
    const btn = card.querySelector("button");
    const canRestart = down && state.supervisor;
    const label = canRestart ? "restart" : "kill";
    if (btn.textContent !== label) {
      btn.textContent = label;
      btn.classList.toggle("restart", canRestart);
      if (canRestart) { btn.dataset.restart = n.id; delete btn.dataset.kill; }
      else { btn.dataset.kill = n.id; delete btn.dataset.restart; }
    }
    const disabled = down && !canRestart;
    if (btn.disabled !== disabled) btn.disabled = disabled;
  });
}

function renderLog() {
  const entries = [...state.messages].sort((a, b) => a.log_idx - b.log_idx).slice(-14);
  const head = `<tr class="log-head"><td>#</td><td></td><td></td><td class="ticks">${state.nodes.map((n) => n.id).join("")}</td></tr>`;
  const rows = entries.map((m) => {
    const ticks = state.nodes.map((n) => {
      if (n.alive === false) return `<span class="t-down">✗</span>`;
      return n.status && n.status.decided_len > m.log_idx ? `<span class="t-ok">✓</span>` : `<span class="t-no">·</span>`;
    }).join("");
    return `<tr><td class="idx">${m.log_idx}</td><td class="role">${m.role === "user" ? "user" : "bot"}</td>
      <td class="text" title="${esc(m.content)}">${esc(m.content)}</td><td class="ticks">${ticks}</td></tr>`;
  }).join("");
  setHTML($("log"), entries.length ? head + rows : `<tr><td class="hint">empty — nothing decided yet</td></tr>`);
}

function renderChat(forceScroll) {
  const inLog = new Set(state.messages.map((m) => m.client_msg_id).filter(Boolean));
  state.pending = state.pending.filter((p) => !inLog.has(p.cid));
  const replies = new Map(state.messages.filter((m) => m.in_reply_to).map((m) => [m.in_reply_to, m]));
  if (state.draft && replies.has(state.draft.in_reply_to)) state.draft = null; // it's in the log now
  // A "ghost" whose text is exactly what got committed wasn't lost after all.
  const ghosts = state.ghosts.filter((g) => {
    const r = replies.get(g.in_reply_to);
    return !(r && r.node === g.node && r.content === g.text);
  });

  const d = state.draft;
  const key = JSON.stringify([
    state.messages.map((m) => m.msg_id), state.pending.map((p) => [p.cid, p.note]),
    ghosts.map((g) => g.key), d && [draftKey(d), d.status, d.text === ""], state.leader,
  ]);
  const box = $("messages");
  const nearBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 80;

  if (key === lastChatKey && !forceScroll) {
    // Only the streaming text changed: update it in place (cheap, keeps selection).
    const el = document.getElementById("draft-text");
    if (el && d) el.innerHTML = fmt(d.text);
    if (nearBottom) box.scrollTop = box.scrollHeight;
    return;
  }
  lastChatKey = key;

  let html = "";
  if (!state.messages.length && !state.pending.length) {
    html = `<div class="empty"><p>One conversation, replicated on every node.<br>Chat, kill the leader, keep chatting.</p></div>`;
  }
  for (const m of state.messages) {
    const meta = m.role === "user"
      ? `via node ${m.node} · #${m.log_idx}`
      : `node ${m.node} · ${esc(m.model ?? "")} · #${m.log_idx}`;
    if (m.role === "assistant") html += ghostsFor(ghosts, m.in_reply_to);
    html += `<div class="msg ${m.role}"><div class="bubble">${fmt(m.content)}</div><div class="meta">${meta}</div></div>`;
    if (m.role === "user" && !replies.has(m.msg_id)) {
      html += ghostsFor(ghosts, m.msg_id);
      if (d && d.in_reply_to === m.msg_id && d.text) {
        const what = d.status === "committed" ? "committing…" : "streaming";
        html += `<div class="msg assistant ${d.status === "streaming" ? "streaming" : ""}">
          <div class="bubble" id="draft-text">${fmt(d.text)}</div>
          <div class="meta">node ${d.node} · ${esc(d.model)} · ${what}</div></div>`;
      } else {
        const who = state.leader !== null ? `node ${state.leader} is generating` : "waiting for a leader";
        html += `<div class="msg assistant thinking"><div class="bubble"><span class="dots">${who}</span></div></div>`;
      }
    }
  }
  for (const p of state.pending) {
    html += `<div class="msg user pending"><div class="bubble">${fmt(p.text)}</div><div class="meta dots">${esc(p.note)}</div></div>`;
  }
  box.innerHTML = html;
  if (nearBottom || forceScroll) box.scrollTop = box.scrollHeight;
}

function ghostsFor(ghosts, userMsgId) {
  return ghosts.filter((g) => g.in_reply_to === userMsgId).map((g) =>
    `<div class="msg assistant ghost"><div class="bubble">${fmt(g.text)}</div>
      <div class="meta">✗ ${esc(g.reason)} · never committed to the log</div></div>`).join("");
}

function renderEvents() {
  setHTML($("events"), state.events.map((e) =>
    `<li class="${e.kind}"><time>${e.at.toLocaleTimeString()}</time>${esc(e.text)}</li>`).join(""));
}

// ---------- wiring ----------

function submit() {
  const text = $("input").value.trim();
  if (!text) return;
  $("input").value = "";
  autosize();
  send(text);
}

function autosize() {
  const t = $("input");
  t.style.height = "auto";
  t.style.height = Math.min(t.scrollHeight, 160) + "px";
}

$("composer").addEventListener("submit", (e) => { e.preventDefault(); submit(); });
$("input").addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey && !e.isComposing) { e.preventDefault(); submit(); }
});
$("input").addEventListener("input", autosize);
document.addEventListener("click", (e) => {
  const k = e.target.closest("[data-kill]");
  if (k) { e.stopPropagation(); kill(Number(k.dataset.kill)); return; }
  const r = e.target.closest("[data-restart]");
  if (r) { e.stopPropagation(); restart(Number(r.dataset.restart)); return; }
  const c = e.target.closest("[data-connect]");
  if (c && byId(Number(c.dataset.connect))?.alive) connect(Number(c.dataset.connect));
});
// Keep the "electing…" counter ticking between polls.
setInterval(() => {
  if ((state.failover && state.failover.electMs === null) || (state.outage && state.outage.backAt === null)) renderFailover();
}, 50);

(async () => {
  const cfg = await (await fetch("/config.json")).json();
  state.nodes = cfg.nodes.map((n) => ({ ...n, alive: null, status: null, lastOk: Date.now() }));
  state.supervisor = !!cfg.supervisor;
  if (single()) {
    $("failover").previousElementSibling.textContent = "Downtime";
    $("log").previousElementSibling.textContent = "Log (one copy, not replicated)";
  }
  if (state.supervisor && single()) {
    $("cluster-hint").innerHTML = "One server, no replication. <b>Kill</b> sends it SIGKILL; " +
      "<b>restart</b> brings it back with empty memory.";
  } else if (state.supervisor) {
    $("cluster-hint").innerHTML = "Click a node to send through it. <b>Kill</b> sends it SIGKILL; " +
      "<b>restart</b> brings it back with empty state, and OmniPaxos catches it up. One node down at a time.";
  }
  state.connected = state.nodes[0].id;
  render();
  $("input").focus();
  pollLoop();
})();
