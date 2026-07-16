"""Basic Web UI for Elastic-Agent Manager.

T-029: Self-contained single-page dashboard served as inline HTML.
Shows node list, status cards, and supports manual operations
(scale out, scale in, drain, remove).
"""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["ui"])

_DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Elastic-Agent Dashboard</title>
<style>
  :root {
    --bg: #0f172a; --surface: #1e293b; --border: #334155;
    --text: #e2e8f0; --text-muted: #94a3b8; --accent: #3b82f6;
    --green: #22c55e; --yellow: #eab308; --red: #ef4444; --orange: #f97316;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: var(--bg); color: var(--text); min-height: 100vh; }
  .container { max-width: 1200px; margin: 0 auto; padding: 20px; }

  header { display: flex; justify-content: space-between; align-items: center;
           padding: 16px 0; border-bottom: 1px solid var(--border); margin-bottom: 24px; }
  header h1 { font-size: 1.5rem; font-weight: 600; }
  header .refresh-info { color: var(--text-muted); font-size: 0.85rem; }

  .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
           gap: 16px; margin-bottom: 24px; }
  .stat-card { background: var(--surface); border: 1px solid var(--border);
               border-radius: 8px; padding: 16px; }
  .stat-card .label { color: var(--text-muted); font-size: 0.8rem; text-transform: uppercase;
                      letter-spacing: 0.05em; margin-bottom: 4px; }
  .stat-card .value { font-size: 1.8rem; font-weight: 700; }

  .actions { display: flex; gap: 8px; margin-bottom: 24px; flex-wrap: wrap; }
  .btn { padding: 8px 16px; border-radius: 6px; border: 1px solid var(--border);
         background: var(--surface); color: var(--text); cursor: pointer;
         font-size: 0.875rem; transition: all 0.15s; }
  .btn:hover { border-color: var(--accent); background: #1e3a5f; }
  .btn-primary { background: var(--accent); border-color: var(--accent); }
  .btn-primary:hover { background: #2563eb; }
  .btn-danger { border-color: var(--red); color: var(--red); }
  .btn-danger:hover { background: #3b1111; }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; }

  .node-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
               gap: 16px; }
  .node-card { background: var(--surface); border: 1px solid var(--border);
               border-radius: 8px; padding: 16px; position: relative; }
  .node-card .node-header { display: flex; justify-content: space-between;
                            align-items: center; margin-bottom: 12px; }
  .node-card .node-id { font-weight: 600; font-size: 0.95rem; }
  .node-card .instance-id { color: var(--text-muted); font-size: 0.75rem;
                            word-break: break-all; }

  .status-badge { display: inline-block; padding: 2px 8px; border-radius: 9999px;
                  font-size: 0.75rem; font-weight: 600; text-transform: uppercase; }
  .status-running { background: #14532d; color: var(--green); }
  .status-ready { background: #14532d; color: var(--green); }
  .status-pending, .status-starting, .status-bootstrapping {
    background: #422006; color: var(--yellow); }
  .status-draining { background: #431407; color: var(--orange); }
  .status-terminated, .status-error, .status-failed {
    background: #450a0a; color: var(--red); }
  .status-stopped { background: #1e293b; color: var(--text-muted); }

  .ws-indicator { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
                  margin-right: 4px; vertical-align: middle; }
  .ws-connected { background: var(--green); box-shadow: 0 0 4px var(--green); }
  .ws-disconnected { background: var(--red); }

  .node-details { display: grid; grid-template-columns: auto 1fr; gap: 4px 12px;
                  font-size: 0.85rem; margin-bottom: 12px; }
  .node-details dt { color: var(--text-muted); }
  .node-details dd { word-break: break-all; }

  .node-actions { display: flex; gap: 6px; }
  .node-actions .btn { padding: 4px 10px; font-size: 0.8rem; }

  .modal-backdrop { position: fixed; inset: 0; background: rgba(0,0,0,0.6);
                    display: none; z-index: 100; justify-content: center;
                    align-items: center; }
  .modal-backdrop.active { display: flex; }
  .modal { background: var(--surface); border: 1px solid var(--border);
           border-radius: 12px; padding: 24px; min-width: 360px; max-width: 480px; }
  .modal h2 { margin-bottom: 16px; font-size: 1.1rem; }
  .modal label { display: block; margin-bottom: 4px; color: var(--text-muted);
                 font-size: 0.85rem; }
  .modal input, .modal select { width: 100%; padding: 8px 12px; margin-bottom: 12px;
                                 border: 1px solid var(--border); border-radius: 6px;
                                 background: var(--bg); color: var(--text); font-size: 0.9rem; }
  .modal .modal-actions { display: flex; gap: 8px; justify-content: flex-end;
                          margin-top: 8px; }

  .toast { position: fixed; bottom: 24px; right: 24px; padding: 12px 20px;
           border-radius: 8px; font-size: 0.875rem; z-index: 200; display: none;
           max-width: 400px; animation: slideUp 0.3s ease; }
  .toast.show { display: block; }
  .toast.success { background: #14532d; border: 1px solid var(--green); }
  .toast.error { background: #450a0a; border: 1px solid var(--red); }
  @keyframes slideUp { from { transform: translateY(20px); opacity: 0; }
                        to { transform: translateY(0); opacity: 1; } }

  .empty-state { text-align: center; padding: 60px 20px; color: var(--text-muted); }
  .empty-state p { margin-top: 8px; }
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>Elastic-Agent Dashboard</h1>
    <div class="refresh-info">
      <a href="/batch" id="navBatch" style="color:var(--accent);text-decoration:none;margin-right:12px">Batch Console →</a>
      Auto-refresh: <span id="refreshInterval">5s</span>
      &middot; Last: <span id="lastRefresh">--</span>
    </div>
  </header>

  <div class="stats" id="statsCards">
    <div class="stat-card"><div class="label">Total Nodes</div><div class="value" id="statTotal">--</div></div>
    <div class="stat-card"><div class="label">Running</div><div class="value" id="statRunning" style="color:var(--green)">--</div></div>
    <div class="stat-card"><div class="label">Bootstrapping</div><div class="value" id="statBootstrapping" style="color:var(--yellow)">--</div></div>
    <div class="stat-card"><div class="label">WS Connected</div><div class="value" id="statConnected" style="color:var(--accent)">--</div></div>
    <div class="stat-card"><div class="label">Draining</div><div class="value" id="statDraining" style="color:var(--orange)">--</div></div>
  </div>

  <div class="actions">
    <button class="btn btn-primary" onclick="showScaleOutModal()">Scale Out</button>
    <button class="btn" onclick="refreshNodes()">Refresh</button>
  </div>

  <div class="node-grid" id="nodeGrid"></div>
</div>

<!-- Scale Out Modal -->
<div class="modal-backdrop" id="scaleOutModal">
  <div class="modal">
    <h2>Scale Out</h2>
    <label for="scaleCount">Number of instances</label>
    <input type="number" id="scaleCount" value="1" min="1" max="50">
    <label for="instanceType">Instance type (optional)</label>
    <input type="text" id="instanceType" placeholder="e.g. ecs.c6.large">
    <div class="modal-actions">
      <button class="btn" onclick="hideModal('scaleOutModal')">Cancel</button>
      <button class="btn btn-primary" onclick="doScaleOut()">Create</button>
    </div>
  </div>
</div>

<!-- Confirm Modal -->
<div class="modal-backdrop" id="confirmModal">
  <div class="modal">
    <h2 id="confirmTitle">Confirm</h2>
    <p id="confirmMsg" style="margin-bottom:16px;color:var(--text-muted)"></p>
    <div class="modal-actions">
      <button class="btn" onclick="hideModal('confirmModal')">Cancel</button>
      <button class="btn btn-danger" id="confirmBtn" onclick="">Confirm</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
const _urlKey = new URLSearchParams(window.location.search).get('api_key');
if (_urlKey) localStorage.setItem('ea_api_key', _urlKey);
const API_KEY = _urlKey || localStorage.getItem('ea_api_key') || '';
const headers = API_KEY ? {'Authorization': `Bearer ${API_KEY}`, 'Content-Type': 'application/json'}
                        : {'Content-Type': 'application/json'};

// Batch Console link (key persists via localStorage; keep query too).
{const nb = document.getElementById('navBatch'); if (nb) nb.href = '/' + window.location.search;}

let refreshTimer = null;

async function api(method, path, body) {
  const opts = {method, headers: {...headers}};
  if (body) opts.body = JSON.stringify(body);
  const resp = await fetch('/api' + path, opts);
  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`${resp.status}: ${err}`);
  }
  return resp.json();
}

function toast(msg, type = 'success') {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = 'toast show ' + type;
  setTimeout(() => el.className = 'toast', 3000);
}

function showModal(id) { document.getElementById(id).classList.add('active'); }
function hideModal(id) { document.getElementById(id).classList.remove('active'); }
function showScaleOutModal() { showModal('scaleOutModal'); }

function confirm(title, msg, action) {
  document.getElementById('confirmTitle').textContent = title;
  document.getElementById('confirmMsg').textContent = msg;
  document.getElementById('confirmBtn').onclick = () => { hideModal('confirmModal'); action(); };
  showModal('confirmModal');
}

async function doScaleOut() {
  hideModal('scaleOutModal');
  const count = parseInt(document.getElementById('scaleCount').value) || 1;
  const instanceType = document.getElementById('instanceType').value || null;
  try {
    const body = {count};
    if (instanceType) body.instance_type = instanceType;
    const resp = await api('POST', '/scale-out', body);
    toast(`Created ${resp.nodes.length} node(s)`);
    refreshNodes();
  } catch(e) { toast(e.message, 'error'); }
}

async function drainNode(nodeId) {
  confirm('Drain Node', `Start draining node ${nodeId}? It will finish current tasks then shut down.`,
    async () => {
      try {
        await api('POST', `/nodes/${nodeId}/drain`);
        toast(`Node ${nodeId} set to draining`);
        refreshNodes();
      } catch(e) { toast(e.message, 'error'); }
    });
}

async function removeNode(nodeId) {
  confirm('Remove Node', `Remove node ${nodeId}? This will terminate the instance.`,
    async () => {
      try {
        await api('DELETE', `/nodes/${nodeId}`);
        toast(`Node ${nodeId} removed`);
        refreshNodes();
      } catch(e) { toast(e.message, 'error'); }
    });
}

async function scaleInNode(nodeId) {
  confirm('Terminate Node', `Terminate node ${nodeId}?`,
    async () => {
      try {
        await api('POST', '/scale-in', {node_ids: [nodeId], force: false});
        toast(`Node ${nodeId} terminating`);
        refreshNodes();
      } catch(e) { toast(e.message, 'error'); }
    });
}

function statusClass(status) {
  const s = status.toLowerCase();
  if (['running', 'ready'].includes(s)) return 'status-running';
  if (['pending', 'starting', 'bootstrapping'].includes(s)) return 'status-pending';
  if (s === 'draining') return 'status-draining';
  if (['terminated', 'error', 'failed'].includes(s)) return 'status-terminated';
  return 'status-stopped';
}

function timeAgo(isoStr) {
  if (!isoStr) return '--';
  const diff = (Date.now() - new Date(isoStr).getTime()) / 1000;
  if (diff < 60) return Math.floor(diff) + 's ago';
  if (diff < 3600) return Math.floor(diff/60) + 'm ago';
  if (diff < 86400) return Math.floor(diff/3600) + 'h ago';
  return Math.floor(diff/86400) + 'd ago';
}

function renderNode(n) {
  const isActive = ['running', 'ready', 'bootstrapping', 'draining'].includes(n.status.toLowerCase());
  return `
    <div class="node-card">
      <div class="node-header">
        <div>
          <span class="ws-indicator ${n.ws_connected ? 'ws-connected' : 'ws-disconnected'}"></span>
          <span class="node-id">${n.node_id.substring(0, 12)}...</span>
        </div>
        <span class="status-badge ${statusClass(n.status)}">${n.status}</span>
      </div>
      <div class="instance-id">${n.instance_id}</div>
      <dl class="node-details">
        <dt>Platform</dt><dd>${n.platform || '--'}</dd>
        <dt>Public IP</dt><dd>${n.public_ip || '--'}</dd>
        <dt>Private IP</dt><dd>${n.private_ip || '--'}</dd>
        <dt>Created</dt><dd>${timeAgo(n.created_at)}</dd>
        <dt>Last HB</dt><dd>${timeAgo(n.last_heartbeat)}</dd>
      </dl>
      <div class="node-actions">
        ${isActive ? `<button class="btn" onclick="drainNode('${n.node_id}')">Drain</button>` : ''}
        ${isActive ? `<button class="btn btn-danger" onclick="scaleInNode('${n.node_id}')">Terminate</button>` : ''}
        <button class="btn btn-danger" onclick="removeNode('${n.node_id}')">Remove</button>
      </div>
    </div>`;
}

async function refreshNodes() {
  try {
    const data = await api('GET', '/nodes?limit=200');
    const nodes = data.nodes || [];

    document.getElementById('statTotal').textContent = data.total;
    document.getElementById('statRunning').textContent =
      nodes.filter(n => ['running','ready'].includes(n.status.toLowerCase())).length;
    document.getElementById('statBootstrapping').textContent =
      nodes.filter(n => ['pending','starting','bootstrapping'].includes(n.status.toLowerCase())).length;
    document.getElementById('statConnected').textContent =
      nodes.filter(n => n.ws_connected).length;
    document.getElementById('statDraining').textContent =
      nodes.filter(n => n.status.toLowerCase() === 'draining').length;

    const grid = document.getElementById('nodeGrid');
    if (nodes.length === 0) {
      grid.innerHTML = '<div class="empty-state"><h3>No nodes</h3><p>Click "Scale Out" to create worker instances.</p></div>';
    } else {
      grid.innerHTML = nodes.map(renderNode).join('');
    }
    document.getElementById('lastRefresh').textContent = new Date().toLocaleTimeString();
  } catch(e) {
    toast('Failed to load nodes: ' + e.message, 'error');
  }
}

refreshNodes();
refreshTimer = setInterval(refreshNodes, 5000);
</script>
</body>
</html>
"""


_BATCH_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Elastic-Agent Batch Console</title>
<style>
  :root {
    --bg:#0f172a; --surface:#1e293b; --border:#334155; --text:#e2e8f0;
    --muted:#94a3b8; --accent:#3b82f6; --green:#22c55e; --yellow:#eab308;
    --red:#ef4444; --orange:#f97316;
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
         background:var(--bg); color:var(--text); min-height:100vh; }
  .container { max-width:1200px; margin:0 auto; padding:20px; }
  header { display:flex; justify-content:space-between; align-items:center;
           padding:16px 0; border-bottom:1px solid var(--border); margin-bottom:20px; }
  header h1 { font-size:1.4rem; } header a { color:var(--accent); text-decoration:none; font-size:.9rem; }
  .card { background:var(--surface); border:1px solid var(--border); border-radius:10px;
          padding:18px; margin-bottom:20px; }
  .card h2 { font-size:1.05rem; margin-bottom:12px; }
  label { display:block; font-size:.8rem; color:var(--muted); margin:8px 0 3px; }
  input, select, textarea { width:100%; background:var(--bg); color:var(--text);
    border:1px solid var(--border); border-radius:6px; padding:7px 9px; font-size:.85rem;
    font-family:inherit; }
  textarea { resize:vertical; min-height:52px; font-family:ui-monospace,Menlo,monospace; }
  .grid2 { display:grid; grid-template-columns:1fr 1fr; gap:10px; }
  .grid3 { display:grid; grid-template-columns:1fr 1fr 1fr; gap:10px; }
  .btn { background:var(--accent); color:#fff; border:none; border-radius:6px;
    padding:8px 14px; font-size:.85rem; cursor:pointer; margin-top:10px; }
  .btn-danger { background:var(--red); }
  .btn-ghost { background:transparent; border:1px solid var(--border); color:var(--text); }
  table { width:100%; border-collapse:collapse; font-size:.83rem; }
  th, td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--border); }
  th { color:var(--muted); font-weight:500; }
  .badge { padding:2px 8px; border-radius:10px; font-size:.72rem; }
  .b-running { background:rgba(59,130,246,.2); color:var(--accent); }
  .b-done { background:rgba(34,197,94,.2); color:var(--green); }
  .b-failed { background:rgba(239,68,68,.2); color:var(--red); }
  .b-rotating { background:rgba(249,115,22,.2); color:var(--orange); }
  .b-pending, .b-bootstrapping, .b-logging_in { background:rgba(148,163,184,.2); color:var(--muted); }
  .muted { color:var(--muted); font-size:.8rem; }
  .toast { position:fixed; bottom:20px; right:20px; background:var(--surface);
    border:1px solid var(--border); border-radius:8px; padding:12px 18px; opacity:0;
    transition:opacity .3s; pointer-events:none; }
  .toast.show { opacity:1; } .toast.error { border-color:var(--red); }
  details { margin-top:6px; } summary { cursor:pointer; }
  .hint { font-size:.72rem; color:var(--muted); margin-top:2px; }
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>Batch Console</h1>
    <a href="/" id="navFleet">← Fleet Dashboard</a>
  </header>

  <!-- Accounts -->
  <div class="card">
    <h2>Accounts</h2>
    <p class="hint">Only account identities (email + 接码 token). Credentials are minted on the worker at login — never stored here.</p>
    <table><thead><tr><th>ID</th><th>Email</th><th>Group</th><th>Enabled</th><th></th></tr></thead>
      <tbody id="acctRows"></tbody></table>
    <div class="grid3" style="margin-top:12px">
      <div><label>ID</label><input id="acctId" placeholder="acc-1"></div>
      <div><label>Email</label><input id="acctEmail" placeholder="a@x.com"></div>
      <div><label>接码 Token</label><input id="acctToken" placeholder="optional"></div>
    </div>
    <div class="grid2">
      <div><label>Group</label><input id="acctGroup" value="standard"></div>
    </div>
    <button class="btn" onclick="addAccount()">Add Account</button>
  </div>

  <!-- Job submission -->
  <div class="card">
    <h2>Submit Job</h2>
    <div class="grid2">
      <div><label>Job name</label><input id="jName" placeholder="ai4sci-opus48-seed128"></div>
      <div><label>Workers (fan-out)</label><input id="jWorkers" type="number" value="1" min="1"></div>
    </div>
    <label>Setup — repo URL</label>
    <input id="jRepo" placeholder="https://github.com/ApexIntelligence-AI/Agent-AI4Sci-Bench.git">
    <label>Setup — commands (one per line, run after clone)</label>
    <textarea id="jSetup" placeholder="uv sync"></textarea>
    <label>Run command (shell; {{shard_index}} / $(hostname -s) supported)</label>
    <textarea id="jRun" placeholder='uv run ai4sci-bench run --output-dir "results/opus48_$(hostname -s)_seed128"'></textarea>
    <div class="grid2">
      <div><label>Working dir (cwd)</label><input id="jCwd" value="."></div>
      <div><label>Shard by</label>
        <select id="jShard"><option value="hostname">hostname</option>
          <option value="shard_index">shard_index</option><option value="none">none</option></select></div>
    </div>
    <label>Env (KEY=VALUE per line)</label>
    <textarea id="jEnv" placeholder="AI4SCI_SANDBOX_CPU=1&#10;AI4SCI_SANDBOX_MEM=4g"></textarea>
    <div class="grid3">
      <div><label>Account mode</label>
        <select id="jAcctMode"><option value="worker_local_login">worker_local_login</option>
          <option value="manager_distribute">manager_distribute</option><option value="none">none</option></select></div>
      <div><label>Account group</label><input id="jAcctGroup" value="standard"></div>
      <div><label>config_dir (blank = ~/.claude)</label><input id="jConfigDir" placeholder=""></div>
    </div>
    <div class="grid2">
      <div><label>Rotation strategy</label>
        <select id="jRot"><option value="none">none</option>
          <option value="on_exhaust_restart_resume">on_exhaust_restart_resume (a)</option></select></div>
      <div><label>Resume args (appended on rotation restart)</label>
        <input id="jResume" placeholder='--resume "results/opus48_$(hostname -s)_seed128"'></div>
    </div>

    <details>
      <summary class="muted">Advanced: upload Harness code (escape hatch)</summary>
      <div class="grid2" style="margin-top:8px">
        <div><label>Filename (&lt;name&gt;.py)</label><input id="hFile" placeholder="my_harness.py"></div>
        <div><label>Class name</label><input id="hClass" placeholder="MyHarness"></div>
      </div>
      <label>Harness code (a Harness subclass)</label>
      <textarea id="hCode" style="min-height:120px"></textarea>
      <button class="btn btn-ghost" onclick="uploadHarness()">Upload → set harness_ref</button>
      <div><label>harness_ref (set = uploaded code drives the job; blank = declarative)</label>
        <input id="jHarnessRef" placeholder=""></div>
    </details>

    <button class="btn" onclick="submitJob()">Launch Job</button>
  </div>

  <!-- Jobs monitor -->
  <div class="card">
    <h2>Jobs <span class="muted" id="jobsRefresh"></span></h2>
    <div id="jobsList"><p class="muted">No jobs yet.</p></div>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
const _urlKey = new URLSearchParams(window.location.search).get('api_key');
if (_urlKey) localStorage.setItem('ea_api_key', _urlKey);
const API_KEY = _urlKey || localStorage.getItem('ea_api_key') || '';
const headers = API_KEY ? {'Authorization':`Bearer ${API_KEY}`,'Content-Type':'application/json'}
                        : {'Content-Type':'application/json'};
// Fleet Dashboard link (key persists via localStorage; keep query too).
{const nav = document.getElementById('navFleet'); if (nav) nav.href = '/fleet' + window.location.search;}
async function api(method, path, body) {
  const opts = {method, headers:{...headers}};
  if (body) opts.body = JSON.stringify(body);
  const resp = await fetch('/api' + path, opts);
  if (!resp.ok) throw new Error(`${resp.status}: ${await resp.text()}`);
  return resp.status === 204 ? null : resp.json();
}
function toast(msg, type='success') {
  const el = document.getElementById('toast');
  el.textContent = msg; el.className = 'toast show ' + type;
  setTimeout(() => el.className = 'toast', 3500);
}
function lines(id) {
  return document.getElementById(id).value.split('\\n').map(s => s.trim()).filter(Boolean);
}

// ---- Accounts ----
async function refreshAccounts() {
  try {
    const d = await api('GET', '/accounts');
    document.getElementById('acctRows').innerHTML = (d.accounts || []).map(a => `
      <tr><td>${a.id}</td><td>${a.email}</td><td>${a.group}</td><td>${a.enabled}</td>
      <td><button class="btn btn-danger" style="margin:0;padding:3px 9px"
          onclick="removeAccount('${a.id}')">✕</button></td></tr>`).join('')
      || '<tr><td colspan="5" class="muted">No accounts.</td></tr>';
  } catch(e) { toast(e.message, 'error'); }
}
async function addAccount() {
  const id = document.getElementById('acctId').value.trim();
  const email = document.getElementById('acctEmail').value.trim();
  if (!id || !email) return toast('id + email required', 'error');
  try {
    await api('POST', '/accounts', {id, email,
      email_token: document.getElementById('acctToken').value.trim(),
      group: document.getElementById('acctGroup').value.trim() || 'standard'});
    document.getElementById('acctId').value = ''; document.getElementById('acctEmail').value = '';
    document.getElementById('acctToken').value = '';
    toast('Account added'); refreshAccounts();
  } catch(e) { toast(e.message, 'error'); }
}
async function removeAccount(id) {
  try { await api('DELETE', '/accounts/' + id); toast('Removed'); refreshAccounts(); }
  catch(e) { toast(e.message, 'error'); }
}

// ---- Harness upload ----
async function uploadHarness() {
  try {
    const r = await api('POST', '/jobs/harness', {
      filename: document.getElementById('hFile').value.trim(),
      class_name: document.getElementById('hClass').value.trim(),
      content: document.getElementById('hCode').value});
    document.getElementById('jHarnessRef').value = r.harness_ref;
    toast('Harness uploaded');
  } catch(e) { toast(e.message, 'error'); }
}

// ---- Job submit ----
function buildEnv() {
  const env = {};
  for (const l of lines('jEnv')) { const i = l.indexOf('='); if (i > 0) env[l.slice(0,i)] = l.slice(i+1); }
  return env;
}
async function submitJob() {
  const ref = document.getElementById('jHarnessRef').value.trim();
  const spec = {
    name: document.getElementById('jName').value.trim() || 'job',
    setup: {repo: document.getElementById('jRepo').value.trim() || null, commands: lines('jSetup')},
    run: {command: document.getElementById('jRun').value.trim(),
          cwd: document.getElementById('jCwd').value.trim() || '.', env: buildEnv()},
    account: {mode: document.getElementById('jAcctMode').value,
              group: document.getElementById('jAcctGroup').value.trim() || 'standard',
              config_dir: document.getElementById('jConfigDir').value.trim()},
    rotation: {strategy: document.getElementById('jRot').value,
               resume_args: document.getElementById('jResume').value.trim()},
    fanout: {workers: parseInt(document.getElementById('jWorkers').value) || 1,
             shard_by: document.getElementById('jShard').value},
  };
  if (ref) spec.harness_ref = ref;
  try { const j = await api('POST', '/jobs', spec);
    toast('Launched ' + j.job_id); refreshJobs(); }
  catch(e) { toast(e.message, 'error'); }
}

// ---- Jobs monitor ----
function badge(p) { return `<span class="badge b-${p}">${p}</span>`; }
async function refreshJobs() {
  try {
    const d = await api('GET', '/jobs');
    const jobs = d.jobs || [];
    if (!jobs.length) { document.getElementById('jobsList').innerHTML = '<p class="muted">No jobs yet.</p>'; }
    else {
      const details = await Promise.all(jobs.map(j => api('GET', '/jobs/' + j.job_id)));
      document.getElementById('jobsList').innerHTML = details.map(j => `
        <div style="border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:10px">
          <b>${j.name}</b> <span class="muted">${j.job_id}</span> ·
          ${Object.entries(j.phases).map(([p,n]) => badge(p)+' '+n).join(' ')}
          <details><summary class="muted">${j.workers_detail.length} workers</summary>
          <table style="margin-top:6px"><thead><tr><th>shard</th><th>worker</th><th>phase</th>
            <th>account</th><th>rot</th><th>error</th></tr></thead><tbody>
          ${j.workers_detail.map(w => `<tr><td>${w.shard_index}</td>
            <td>${(w.worker_id||'').substring(0,14)}</td><td>${badge(w.phase)}</td>
            <td>${w.account_email||'--'}</td><td>${w.rotations}</td>
            <td class="muted">${w.error||''}</td></tr>`).join('')}
          </tbody></table></details>
        </div>`).join('');
    }
    document.getElementById('jobsRefresh').textContent = '· ' + new Date().toLocaleTimeString();
  } catch(e) { /* silent */ }
}

refreshAccounts(); refreshJobs();
setInterval(refreshJobs, 5000);
</script>
</body>
</html>
"""


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root_batch():
    """Root serves the Batch Console — the primary surface (accounts, jobs)."""
    return HTMLResponse(content=_BATCH_HTML)


@router.get("/batch", response_class=HTMLResponse, include_in_schema=False)
async def batch_console():
    """Alias for the Batch Console."""
    return HTMLResponse(content=_BATCH_HTML)


@router.get("/fleet", response_class=HTMLResponse, include_in_schema=False)
async def fleet_dashboard():
    """Serve the Fleet Dashboard (nodes, scaling)."""
    return HTMLResponse(content=_DASHBOARD_HTML)


@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_alt():
    """Alias for the Fleet Dashboard."""
    return HTMLResponse(content=_DASHBOARD_HTML)
