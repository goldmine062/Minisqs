package com.minisqs.server;

/** Single-page dashboard served at "/". Polls /api/queues once a second. */
final class Dashboard {
    private Dashboard() {}

    static final String HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MiniSQS</title>
<style>
  :root {
    --bg: #f6f7f9; --surface: #ffffff; --text: #1d2330; --muted: #5c6577; --line: #dde1e8;
    --visible: #2f6fdb; --inflight: #d98a1c; --delayed: #9aa3b2; --bad: #c43d3d; --ok: #2c8a57;
    color-scheme: light;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #12151b; --surface: #1a1f28; --text: #e6e9ef; --muted: #98a2b3; --line: #2b3240;
      --visible: #5b93f0; --inflight: #f0a53a; --delayed: #6c7586; --bad: #ef6b6b; --ok: #4cc283;
      color-scheme: dark;
    }
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--text);
         font: 15px/1.5 "Segoe UI", system-ui, -apple-system, Roboto, sans-serif; }
  main { max-width: 1100px; margin: 0 auto; padding: 24px 20px 60px; }
  h1 { font-size: 24px; margin: 0 0 4px; }
  h2 { font-size: 17px; margin: 32px 0 10px; }
  .sub { color: var(--muted); margin: 0; }
  .info { color: var(--muted); font-size: 13px; margin-top: 8px; }
  .table-wrap { overflow-x: auto; background: var(--surface); border: 1px solid var(--line); border-radius: 8px; }
  table { border-collapse: collapse; width: 100%; min-width: 760px; }
  th, td { padding: 10px 12px; text-align: right; border-bottom: 1px solid var(--line); white-space: nowrap; }
  th { color: var(--muted); font-weight: 600; font-size: 13px; }
  th:first-child, td:first-child { text-align: left; }
  tr:last-child td { border-bottom: none; }
  .bar { display: flex; height: 10px; width: 160px; background: var(--line); border-radius: 5px; overflow: hidden; margin-top: 6px; }
  .bar span { display: block; height: 100%; transition: width .3s; }
  .legend { display: flex; gap: 16px; font-size: 13px; color: var(--muted); margin: 10px 0 0; flex-wrap: wrap; }
  .dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; vertical-align: -1px; }
  .num-visible { color: var(--visible); font-weight: 600; }
  .num-inflight { color: var(--inflight); font-weight: 600; }
  .panels { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px; }
  .panel { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 16px; }
  .panel h3 { margin: 0 0 12px; font-size: 15px; }
  label { display: block; font-size: 13px; color: var(--muted); margin: 10px 0 4px; }
  input, select, textarea { width: 100%; padding: 8px 10px; font: inherit; color: var(--text);
    background: var(--bg); border: 1px solid var(--line); border-radius: 6px; }
  textarea { min-height: 64px; resize: vertical; }
  .row { display: flex; gap: 10px; }
  .row > div { flex: 1; }
  button { margin-top: 12px; padding: 8px 14px; font: inherit; font-weight: 600; cursor: pointer;
    color: #fff; background: var(--visible); border: none; border-radius: 6px; }
  button.secondary { background: transparent; color: var(--text); border: 1px solid var(--line); }
  button.small { margin: 0 6px 0 0; padding: 4px 10px; font-size: 13px; }
  button:focus-visible, input:focus-visible, select:focus-visible, textarea:focus-visible {
    outline: 2px solid var(--visible); outline-offset: 2px; }
  .status { font-size: 13px; margin-top: 10px; min-height: 20px; }
  .status.err { color: var(--bad); }
  .status.ok { color: var(--ok); }
  .msg { border-top: 1px solid var(--line); padding: 10px 0; font-size: 14px; }
  .msg code { font-size: 12px; color: var(--muted); word-break: break-all; }
  .empty { color: var(--muted); padding: 16px 12px; }
  @media (prefers-reduced-motion: reduce) { .bar span { transition: none; } }
</style>
</head>
<body>
<main>
  <h1>MiniSQS</h1>
  <p class="sub">Live view of every queue, refreshed each second.</p>
  <p class="info" id="info"></p>

  <h2>Queues</h2>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Queue</th><th>Visible</th><th>In flight</th><th>Delayed</th><th>Oldest</th>
        <th>Sent/s</th><th>Deleted/s</th><th>Sent</th><th>Received</th><th>Deleted</th><th>To DLQ</th>
      </tr></thead>
      <tbody id="queues"><tr><td class="empty" colspan="11">Loading…</td></tr></tbody>
    </table>
  </div>
  <div class="legend">
    <span><span class="dot" style="background:var(--visible)"></span>Visible: ready to receive</span>
    <span><span class="dot" style="background:var(--inflight)"></span>In flight: received, waiting for delete or timeout</span>
    <span><span class="dot" style="background:var(--delayed)"></span>Delayed: not visible yet</span>
  </div>

  <h2>Try it</h2>
  <div class="panels">
    <section class="panel">
      <h3>Create a queue</h3>
      <label for="cq-name">Name</label>
      <input id="cq-name" value="orders">
      <div class="row">
        <div><label for="cq-vis">Visibility timeout (s)</label><input id="cq-vis" type="number" min="0" value="10"></div>
        <div><label for="cq-max">Max receives</label><input id="cq-max" type="number" min="0" value="0"></div>
      </div>
      <label for="cq-dlq">Dead-letter queue (needed when max receives &gt; 0)</label>
      <input id="cq-dlq" placeholder="orders-dlq">
      <button id="cq-btn">Create queue</button>
      <div class="status" id="cq-status"></div>
    </section>

    <section class="panel">
      <h3>Send a message</h3>
      <label for="sm-queue">Queue</label>
      <select id="sm-queue"></select>
      <label for="sm-body">Body</label>
      <textarea id="sm-body">{"orderId": 42, "item": "keyboard"}</textarea>
      <label for="sm-delay">Delay (s)</label>
      <input id="sm-delay" type="number" min="0" value="0">
      <button id="sm-btn">Send message</button>
      <div class="status" id="sm-status"></div>
    </section>

    <section class="panel">
      <h3>Receive messages</h3>
      <label for="rm-queue">Queue</label>
      <select id="rm-queue"></select>
      <div class="row">
        <div><label for="rm-max">Max</label><input id="rm-max" type="number" min="1" max="10" value="1"></div>
        <div><label for="rm-wait">Wait (s)</label><input id="rm-wait" type="number" min="0" max="20" value="5"></div>
      </div>
      <button id="rm-btn">Receive</button>
      <div class="status" id="rm-status"></div>
      <div id="rm-list"></div>
    </section>
  </div>
</main>
<script>
const $ = (id) => document.getElementById(id);
let previous = null;
let previousAt = 0;

async function api(method, url, body) {
  const res = await fetch(url, { method, body });
  const data = await res.json();
  if (!res.ok) throw new Error(data.message || res.statusText);
  return data;
}

function setStatus(id, text, ok) {
  const el = $(id);
  el.textContent = text;
  el.className = "status " + (ok ? "ok" : "err");
}

function cell(text, cls) {
  const td = document.createElement("td");
  td.textContent = text;
  if (cls) td.className = cls;
  return td;
}

function fillSelect(id, names) {
  const sel = $(id);
  const current = sel.value;
  const key = names.join("|");
  if (sel.dataset.key === key) return;
  sel.dataset.key = key;
  sel.innerHTML = "";
  for (const n of names) {
    const o = document.createElement("option");
    o.value = o.textContent = n;
    sel.appendChild(o);
  }
  if (names.includes(current)) sel.value = current;
}

async function refresh() {
  try {
    const [{ queues }, info] = await Promise.all([api("GET", "/api/queues"), api("GET", "/api/info")]);
    $("info").textContent =
      `Log: ${(info.walBytes / 1024).toFixed(1)} KB, ${info.walAppends.toLocaleString()} records written, ` +
      `${info.walFsyncs.toLocaleString()} fsyncs (fsync ${info.fsyncEnabled ? "on" : "off"}). ` +
      `Startup recovery: ${info.recoveredMessages} messages from ${info.recoveredRecords} records in ${info.recoveryMillis} ms.`;

    const now = performance.now();
    const seconds = previous ? (now - previousAt) / 1000 : 0;
    const body = $("queues");
    body.innerHTML = "";
    if (queues.length === 0) {
      body.innerHTML = '<tr><td class="empty" colspan="11">No queues yet. Create one below or start a producer.</td></tr>';
    }
    for (const q of queues) {
      const prev = previous && previous[q.name];
      const rate = (k) => prev && seconds > 0 ? Math.max(0, (q[k] - prev[k]) / seconds).toFixed(0) : "–";
      const tr = document.createElement("tr");
      const name = cell(q.name + (q.dlq ? `  (DLQ: ${q.dlq}, max ${q.maxReceiveCount})` : ""));
      const total = q.visible + q.inFlight + q.delayed;
      const bar = document.createElement("div");
      bar.className = "bar";
      for (const [k, color] of [["visible", "--visible"], ["inFlight", "--inflight"], ["delayed", "--delayed"]]) {
        const s = document.createElement("span");
        s.style.width = total ? (100 * q[k] / total) + "%" : "0";
        s.style.background = `var(${color})`;
        bar.appendChild(s);
      }
      name.appendChild(bar);
      tr.append(
        name,
        cell(q.visible.toLocaleString(), "num-visible"),
        cell(q.inFlight.toLocaleString(), "num-inflight"),
        cell(q.delayed.toLocaleString()),
        cell(q.oldestMessageAgeMs ? (q.oldestMessageAgeMs / 1000).toFixed(1) + " s" : "–"),
        cell(rate("sent")),
        cell(rate("deleted")),
        cell(q.sent.toLocaleString()),
        cell(q.received.toLocaleString()),
        cell(q.deleted.toLocaleString()),
        cell(q.movedToDlq.toLocaleString()),
      );
      body.appendChild(tr);
    }
    previous = Object.fromEntries(queues.map((q) => [q.name, q]));
    previousAt = now;
    const names = queues.map((q) => q.name);
    fillSelect("sm-queue", names);
    fillSelect("rm-queue", names);
  } catch (e) {
    $("info").textContent = "Can't reach the broker: " + e.message;
  }
}

$("cq-btn").onclick = async () => {
  const p = new URLSearchParams({
    name: $("cq-name").value.trim(),
    visibilityTimeout: $("cq-vis").value,
    maxReceiveCount: $("cq-max").value,
    dlq: $("cq-dlq").value.trim(),
  });
  try {
    await api("POST", "/api/queues?" + p);
    setStatus("cq-status", "Created " + p.get("name"), true);
    refresh();
  } catch (e) { setStatus("cq-status", e.message, false); }
};

$("sm-btn").onclick = async () => {
  const q = $("sm-queue").value;
  if (!q) return setStatus("sm-status", "Create a queue first.", false);
  try {
    const r = await api("POST", `/api/queues/${encodeURIComponent(q)}/messages?delay=${$("sm-delay").value || 0}`, $("sm-body").value);
    setStatus("sm-status", "Sent " + r.messageId, true);
    refresh();
  } catch (e) { setStatus("sm-status", e.message, false); }
};

$("rm-btn").onclick = async () => {
  const q = $("rm-queue").value;
  if (!q) return setStatus("rm-status", "Create a queue first.", false);
  setStatus("rm-status", "Waiting for messages…", true);
  try {
    const r = await api("POST", `/api/queues/${encodeURIComponent(q)}/receive?max=${$("rm-max").value}&wait=${$("rm-wait").value}`);
    setStatus("rm-status", r.messages.length ? `Received ${r.messages.length}. Delete each one or it comes back after the visibility timeout.` : "No messages arrived.", r.messages.length > 0);
    const list = $("rm-list");
    list.innerHTML = "";
    for (const m of r.messages) {
      const div = document.createElement("div");
      div.className = "msg";
      const text = document.createElement("div");
      text.textContent = m.body;
      const meta = document.createElement("code");
      meta.textContent = `id ${m.messageId}, receive #${m.receiveCount}`;
      const del = document.createElement("button");
      del.className = "small";
      del.textContent = "Delete";
      del.onclick = async () => {
        try {
          await api("DELETE", `/api/queues/${encodeURIComponent(q)}/messages?receiptHandle=${m.receiptHandle}`);
          div.remove();
        } catch (e) { meta.textContent = e.message; }
      };
      const release = document.createElement("button");
      release.className = "small secondary";
      release.textContent = "Return to queue";
      release.onclick = async () => {
        try {
          await api("POST", `/api/queues/${encodeURIComponent(q)}/visibility?receiptHandle=${m.receiptHandle}&timeout=0`);
          div.remove();
        } catch (e) { meta.textContent = e.message; }
      };
      const actions = document.createElement("div");
      actions.style.marginTop = "6px";
      actions.append(del, release);
      div.append(text, meta, actions);
      list.appendChild(div);
    }
    refresh();
  } catch (e) { setStatus("rm-status", e.message, false); }
};

refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>
""";
}
