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
<html lang="zh-CN" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Elastic-Agent Dashboard</title>
<script>
  document.documentElement.dataset.theme =
    sessionStorage.getItem('ea_theme') === 'dark' ? 'dark' : 'light';
</script>
<style>
  :root {
    color-scheme:light;
    --bg:#f3f6fb; --surface:#ffffff; --surface-soft:#f8fafc; --border:#d7dee9;
    --text:#172033; --text-muted:#5b6678; --accent:#2563eb;
    --green:#16803c; --yellow:#a16207; --red:#c62828; --orange:#c4510c;
    --hover:#e8f0ff; --shadow:0 8px 26px rgba(36,49,73,.07);
  }
  :root[data-theme="dark"] {
    color-scheme:dark;
    --bg:#121925; --surface:#1c2635; --surface-soft:#151e2b; --border:#344256;
    --text:#e7edf6; --text-muted:#a6b2c3; --accent:#6ea8fe;
    --green:#58cf7b; --yellow:#f3c969; --red:#ff7474; --orange:#ff9a62;
    --hover:#203659; --shadow:0 10px 30px rgba(0,0,0,.18);
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: linear-gradient(180deg,var(--hover),var(--bg) 190px);
         color: var(--text); min-height: 100vh; }
  .container { max-width: 1200px; margin: 0 auto; padding: 20px; }

  header { display: flex; justify-content: space-between; align-items: center;
           padding: 16px 0; border-bottom: 1px solid var(--border); margin-bottom: 24px; }
  header h1 { font-size: 1.5rem; font-weight: 600; }
  header .refresh-info { color: var(--text-muted); font-size: 0.85rem; }

  .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
           gap: 16px; margin-bottom: 24px; }
  .stat-card { background: var(--surface); border: 1px solid var(--border);
               border-radius: 10px; padding: 16px; box-shadow:var(--shadow); }
  .stat-card .label { color: var(--text-muted); font-size: 0.8rem; text-transform: uppercase;
                      letter-spacing: 0.05em; margin-bottom: 4px; }
  .stat-card .value { font-size: 1.8rem; font-weight: 700; }

  .actions { display: flex; gap: 8px; margin-bottom: 24px; flex-wrap: wrap; }
  .btn { padding: 8px 16px; border-radius: 6px; border: 1px solid var(--border);
         background: var(--surface); color: var(--text); cursor: pointer;
         font-size: 0.875rem; transition: all 0.15s; }
  .btn:hover { border-color: var(--accent); background: var(--hover); }
  .btn-primary { background: var(--accent); border-color: var(--accent); }
  .btn-primary:hover { background: #2563eb; }
  .btn-danger { border-color: var(--red); color: var(--red); }
  .btn-danger:hover { background: color-mix(in srgb,var(--red) 10%,var(--surface)); }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; }

  .node-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
               gap: 16px; }
  .node-card { background: var(--surface); border: 1px solid var(--border);
               border-radius: 10px; padding: 16px; position: relative; box-shadow:var(--shadow); }
  .node-card .node-header { display: flex; justify-content: space-between;
                            align-items: center; margin-bottom: 12px; }
  .node-card .node-id { font-weight: 600; font-size: 0.95rem; }
  .node-card .instance-id { color: var(--text-muted); font-size: 0.75rem;
                            word-break: break-all; }

  .status-badge { display: inline-block; padding: 2px 8px; border-radius: 9999px;
                  font-size: 0.75rem; font-weight: 600; text-transform: uppercase; }
  .status-running { background: color-mix(in srgb,var(--green) 14%,var(--surface)); color: var(--green); }
  .status-ready { background: color-mix(in srgb,var(--green) 14%,var(--surface)); color: var(--green); }
  .status-pending, .status-starting, .status-bootstrapping {
    background: color-mix(in srgb,var(--yellow) 14%,var(--surface)); color: var(--yellow); }
  .status-draining { background: color-mix(in srgb,var(--orange) 14%,var(--surface)); color: var(--orange); }
  .status-terminated, .status-error, .status-failed {
    background: color-mix(in srgb,var(--red) 14%,var(--surface)); color: var(--red); }
  .status-stopped { background: var(--surface-soft); color: var(--text-muted); }

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
                                 background: var(--surface-soft); color: var(--text); font-size: 0.9rem; }
  .modal .modal-actions { display: flex; gap: 8px; justify-content: flex-end;
                          margin-top: 8px; }

  .toast { position: fixed; bottom: 24px; right: 24px; padding: 12px 20px;
           border-radius: 8px; font-size: 0.875rem; z-index: 200; display: none;
           max-width: 400px; animation: slideUp 0.3s ease; }
  .toast.show { display: block; }
  .toast.success { background: var(--surface); border: 1px solid var(--green); }
  .toast.error { background: var(--surface); border: 1px solid var(--red); }
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
      <a href="#" id="fleetThemeToggle" onclick="toggleFleetTheme();return false"
        style="color:var(--accent);text-decoration:none;margin-right:12px">切换深色</a>
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
// Never accept bearer credentials in URLs (browser history/referrer/server logs)
// and keep them only for this tab/session rather than durable localStorage.
const _params = new URLSearchParams(window.location.search);
if (_params.has('api_key')) {
  _params.delete('api_key');
  history.replaceState(null, '', window.location.pathname + (_params.size ? '?' + _params : ''));
}
let API_KEY = sessionStorage.getItem('ea_api_key') || '';
if (!API_KEY) {
  const k = (window.prompt('请输入 API Key：') || '').trim();
  if (k) { sessionStorage.setItem('ea_api_key', k); API_KEY = k; }
}
const headers = API_KEY ? {'Authorization': `Bearer ${API_KEY}`, 'Content-Type': 'application/json'}
                        : {'Content-Type': 'application/json'};

{const nb = document.getElementById('navBatch'); if (nb) nb.href = '/';}

let refreshTimer = null;
let nodesRefreshRunning = false;

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
function updateFleetThemeLabel() {
  document.getElementById('fleetThemeToggle').textContent =
    document.documentElement.dataset.theme === 'dark' ? '切换亮色' : '切换深色';
}
function toggleFleetTheme() {
  const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
  document.documentElement.dataset.theme = next;
  sessionStorage.setItem('ea_theme', next);
  updateFleetThemeLabel();
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
        await api('POST', '/scale-in', {node_ids: [nodeId], force: true});
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

function esc(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  })[ch]);
}
function jsArg(value) { return esc(JSON.stringify(String(value ?? ''))); }

function renderNode(n) {
  const isActive = ['running', 'ready', 'bootstrapping', 'draining'].includes(n.status.toLowerCase());
  return `
    <div class="node-card" data-node-id="${esc(n.node_id)}">
      <div class="node-header">
        <div>
          <span class="ws-indicator ${n.ws_connected ? 'ws-connected' : 'ws-disconnected'}"></span>
          <span class="node-id">${esc(n.node_id.substring(0, 12))}...</span>
        </div>
        <span class="status-badge ${statusClass(n.status)}">${esc(n.status)}</span>
      </div>
      <div class="instance-id">${esc(n.instance_id)}</div>
      <dl class="node-details">
        <dt>Platform</dt><dd>${esc(n.platform || '--')}</dd>
        <dt>Public IP</dt><dd>${esc(n.public_ip || '--')}</dd>
        <dt>Private IP</dt><dd>${esc(n.private_ip || '--')}</dd>
        <dt>Created</dt><dd>${timeAgo(n.created_at)}</dd>
        <dt>Last HB</dt><dd>${timeAgo(n.last_heartbeat)}</dd>
      </dl>
      <div class="node-actions">
        ${isActive ? `<button class="btn" onclick="drainNode(${jsArg(n.node_id)})">Drain</button>` : ''}
        ${isActive ? `<button class="btn btn-danger" onclick="scaleInNode(${jsArg(n.node_id)})">Terminate</button>` : ''}
        <button class="btn btn-danger" onclick="removeNode(${jsArg(n.node_id)})">Remove</button>
      </div>
    </div>`;
}

function reconcileNodeCards(nodes) {
  const grid = document.getElementById('nodeGrid');
  if (!nodes.length) {
    if (grid.dataset.empty !== 'true') {
      grid.innerHTML = '<div class="empty-state"><h3>No nodes</h3><p>Job 启动后，临时 Worker 会显示在这里。</p></div>';
      grid.dataset.empty = 'true';
    }
    return;
  }
  delete grid.dataset.empty;
  grid.querySelector('.empty-state')?.remove();
  const wanted = new Set(nodes.map(node => String(node.node_id)));
  Array.from(grid.querySelectorAll('.node-card')).forEach(card => {
    if (!wanted.has(card.dataset.nodeId)) card.remove();
  });
  nodes.forEach((node, index) => {
    const signature = JSON.stringify(node);
    let card = Array.from(grid.querySelectorAll('.node-card'))
      .find(candidate => candidate.dataset.nodeId === String(node.node_id));
    if (!card || card._renderSignature !== signature) {
      const template = document.createElement('template');
      template.innerHTML = renderNode(node).trim();
      const replacement = template.content.firstElementChild;
      replacement._renderSignature = signature;
      if (card) card.replaceWith(replacement);
      card = replacement;
    }
    const current = grid.children[index] || null;
    if (card !== current) grid.insertBefore(card, current);
  });
}
async function refreshNodes() {
  if (nodesRefreshRunning) return;
  nodesRefreshRunning = true;
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

    reconcileNodeCards(nodes);
    document.getElementById('lastRefresh').textContent = new Date().toLocaleTimeString();
  } catch(e) {
    toast('Failed to load nodes: ' + e.message, 'error');
  } finally {
    nodesRefreshRunning = false;
  }
}

async function pollNodes() {
  if (!document.hidden) await refreshNodes();
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(pollNodes, 5000);
}
document.addEventListener('visibilitychange', () => {
  if (!document.hidden) {
    clearTimeout(refreshTimer);
    refreshTimer = setTimeout(pollNodes, 0);
  }
});
updateFleetThemeLabel();
pollNodes();
</script>
</body>
</html>
"""


_BATCH_HTML = """\
<!DOCTYPE html>
<html lang="zh-CN" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Elastic-Agent Batch Console</title>
<script>
  document.documentElement.dataset.theme =
    sessionStorage.getItem('ea_theme') === 'dark' ? 'dark' : 'light';
</script>
<style>
  :root {
    color-scheme:light;
    --bg:#f3f6fb; --surface:#ffffff; --surface-soft:#f8fafc;
    --border:#d7dee9; --text:#172033; --muted:#5b6678; --accent:#2563eb;
    --accent-soft:#e8f0ff; --green:#16803c; --yellow:#a16207;
    --red:#c62828; --orange:#c4510c; --shadow:0 8px 26px rgba(36,49,73,.07);
    --terminal:#111827; --terminal-text:#e5e7eb; --overlay:rgba(15,23,42,.46);
  }
  :root[data-theme="dark"] {
    color-scheme:dark;
    --bg:#121925; --surface:#1c2635; --surface-soft:#151e2b;
    --border:#344256; --text:#e7edf6; --muted:#a6b2c3; --accent:#6ea8fe;
    --accent-soft:#203659; --green:#58cf7b; --yellow:#f3c969;
    --red:#ff7474; --orange:#ff9a62; --shadow:0 10px 30px rgba(0,0,0,.18);
    --terminal:#090f19; --terminal-text:#e5e7eb; --overlay:rgba(0,0,0,.66);
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
         background:linear-gradient(180deg,var(--accent-soft) 0, var(--bg) 190px);
         color:var(--text); min-height:100vh; }
  .container { max-width:1240px; margin:0 auto; padding:20px; }
  header { display:flex; justify-content:space-between; align-items:center;
           padding:16px 0; border-bottom:1px solid var(--border); margin-bottom:20px; }
  header h1 { font-size:1.5rem; letter-spacing:-.02em; }
  header a { color:var(--accent); text-decoration:none; font-size:.9rem; }
  .card { background:var(--surface); border:1px solid var(--border); border-radius:12px;
          padding:18px; margin-bottom:20px; box-shadow:var(--shadow); }
  .card h2 { font-size:1.05rem; margin-bottom:12px; }
  label { display:block; font-size:.8rem; color:var(--muted); margin:8px 0 3px; }
  input, select, textarea { width:100%; background:var(--surface-soft); color:var(--text);
    border:1px solid var(--border); border-radius:6px; padding:7px 9px; font-size:.85rem;
    font-family:inherit; }
  input:focus, select:focus, textarea:focus { outline:2px solid color-mix(in srgb,var(--accent) 28%,transparent);
    border-color:var(--accent); }
  textarea { resize:vertical; min-height:52px; font-family:ui-monospace,Menlo,monospace; }
  .grid2 { display:grid; grid-template-columns:1fr 1fr; gap:10px; }
  .grid3 { display:grid; grid-template-columns:1fr 1fr 1fr; gap:10px; }
  .btn { background:var(--accent); color:#fff; border:none; border-radius:6px;
    padding:8px 14px; font-size:.85rem; cursor:pointer; margin-top:10px; }
  .btn:hover { filter:brightness(.96); }
  .btn:focus-visible, a:focus-visible, summary:focus-visible { outline:3px solid color-mix(in srgb,var(--accent) 35%,transparent);
    outline-offset:2px; }
  .btn:disabled { opacity:.55; cursor:not-allowed; }
  .btn-danger { background:var(--red); }
  .btn-ghost { background:var(--surface-soft); border:1px solid var(--border); color:var(--text); }
  table { width:100%; border-collapse:collapse; font-size:.83rem; }
  th, td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--border); }
  th { color:var(--muted); font-weight:500; }
  .badge { padding:2px 8px; border-radius:10px; font-size:.72rem; }
  .b-running { background:rgba(59,130,246,.2); color:var(--accent); }
  .b-done { background:rgba(34,197,94,.2); color:var(--green); }
  .b-failed { background:rgba(239,68,68,.2); color:var(--red); }
  .b-rotating { background:rgba(249,115,22,.2); color:var(--orange); }
  .b-pending, .b-preparing, .b-provisioning, .b-bootstrapping, .b-logging_in,
  .b-recovered, .b-interrupted { background:rgba(100,116,139,.14); color:var(--muted); }
  .b-succeeded { background:rgba(34,197,94,.16); color:var(--green); }
  .b-cancelled { background:rgba(100,116,139,.14); color:var(--muted); }
  .muted { color:var(--muted); font-size:.8rem; }
  .toast { position:fixed; bottom:20px; right:20px; background:var(--surface);
    border:1px solid var(--border); border-radius:8px; padding:12px 18px; opacity:0;
    transition:opacity .3s; pointer-events:none; z-index:1200; box-shadow:var(--shadow); }
  .toast.show { opacity:1; } .toast.error { border-color:var(--red); }
  details { margin-top:6px; } summary { cursor:pointer; }
  .hint { font-size:.72rem; color:var(--muted); margin-top:2px; }
  code { background:var(--surface-soft); border:1px solid var(--border); border-radius:4px;
    padding:1px 4px; }
  .workflow { display:grid; grid-template-columns:repeat(6,1fr); gap:8px; margin-top:12px; }
  .workflow-step { background:var(--surface-soft); border:1px solid var(--border);
    border-radius:9px; padding:10px; font-size:.78rem; text-align:center; }
  .workflow-step b { display:block; color:var(--accent); margin-bottom:3px; }
  .action-card { border-color:var(--orange); background:color-mix(in srgb,var(--surface) 92%,#fff7ed); }
  .otp-action-card { position:fixed; right:18px; bottom:18px; z-index:900;
    width:min(460px,calc(100vw - 36px)); max-height:calc(100vh - 36px);
    overflow:auto; margin:0; box-shadow:0 20px 55px rgba(15,23,42,.24); }
  .otp-action-card.otp-minimized { width:min(360px,calc(100vw - 36px));
    max-height:none; overflow:hidden; padding:10px 12px; }
  .otp-action-card.otp-minimized .otp-action-head { align-items:center; }
  .otp-action-card.otp-minimized .otp-action-head p,
  .otp-action-card.otp-minimized .otp-jump-list,
  .otp-action-card.otp-minimized > .job-otp-list { display:none; }
  .otp-action-card.otp-minimized h2 { margin:0; font-size:.92rem; }
  .otp-action-head { display:flex; justify-content:space-between; align-items:flex-start;
    gap:12px; }
  .otp-action-head .btn { flex:none; margin:0; }
  .otp-jump-list { display:flex; flex-wrap:wrap; gap:6px; margin-top:9px; }
  .otp-jump-list .btn { margin:0; padding:5px 9px; text-align:left; }
  .job-otp-summary-badge { display:inline-block; margin-left:6px; padding:2px 8px;
    border-radius:999px; color:var(--orange);
    background:color-mix(in srgb,var(--orange) 13%,var(--surface)); font-size:.72rem; }
  .job-otp-summary-badge[hidden],.job-otp-region[hidden] { display:none; }
  .job-otp-region { margin:10px 0; border:1px solid color-mix(in srgb,var(--orange) 45%,var(--border));
    border-radius:9px; padding:10px;
    background:color-mix(in srgb,var(--orange) 7%,var(--surface)); }
  .job-otp-region-head { display:flex; justify-content:space-between; gap:8px;
    align-items:center; margin-bottom:8px; color:var(--orange); font-size:.82rem; }
  .job-otp-list { display:grid; grid-template-columns:repeat(auto-fit,minmax(285px,1fr));
    gap:9px; }
  .otp-challenge-card { min-width:0; border:1px solid var(--border); border-radius:8px;
    padding:10px; background:var(--surface); box-shadow:var(--shadow); }
  .otp-title { display:block; overflow-wrap:anywhere; }
  .otp-context { display:grid; gap:2px; margin-top:5px; }
  .otp-account-email,.otp-account-id,.otp-worker,.otp-job {
    display:block; overflow-wrap:anywhere; }
  .otp-explanation { color:var(--orange); margin-top:7px; }
  .otp-expiry { margin-top:3px; }
  .otp-controls { display:grid; grid-template-columns:minmax(0,1fr) auto;
    gap:8px; margin-top:8px; align-items:center; }
  .otp-controls .btn { margin:0; white-space:nowrap; }
  .otp-code { letter-spacing:.16em; font-variant-numeric:tabular-nums; }
  .job-row { border:1px solid var(--border); border-radius:10px; padding:0;
    margin:0 0 10px; background:var(--surface); overflow:hidden; }
  .job-row.job-failed { border-left:4px solid var(--red); }
  .job-row.job-running, .job-row.job-preparing { border-left:4px solid var(--accent); }
  .job-summary { display:flex; justify-content:space-between; align-items:center; gap:12px;
    padding:12px; list-style:none; }
  .job-summary::-webkit-details-marker { display:none; }
  .job-summary:hover { background:var(--surface-soft); }
  .job-summary:focus-visible { outline-offset:-3px; }
  .job-summary-main { display:block; min-width:0; overflow-wrap:anywhere; }
  .job-summary-title,.job-summary-meta { display:block; }
  .job-summary-toggle { color:var(--accent); font-size:.78rem; white-space:nowrap; }
  .job-summary-open { display:none; }
  .job-row[open] > .job-summary { border-bottom:1px solid var(--border);
    background:var(--surface-soft); }
  .job-row[open] > .job-summary .job-summary-closed { display:none; }
  .job-row[open] > .job-summary .job-summary-open { display:inline; }
  .job-detail { padding:12px; }
  .job-head { display:flex; justify-content:space-between; align-items:flex-start; gap:10px; }
  .job-actions { display:flex; flex-wrap:wrap; justify-content:flex-end; gap:6px; }
  .job-actions .btn { margin:0; padding:5px 10px; }
  .job-actions [data-result-action] { min-width:132px; }
  .worker-records-title { margin-top:10px; margin-bottom:4px; }
  .job-alert { background:color-mix(in srgb,var(--red) 8%,var(--surface));
    border:1px solid color-mix(in srgb,var(--red) 35%,var(--border));
    color:var(--red); border-radius:7px; padding:7px 9px; margin-top:8px; font-size:.8rem;
    white-space:pre-wrap; overflow-wrap:anywhere; }
  .cleanup-alert { color:var(--orange); border-color:color-mix(in srgb,var(--orange) 35%,var(--border));
    background:color-mix(in srgb,var(--orange) 8%,var(--surface)); }
  .table-scroll { overflow-x:auto; }
  .log-dialog { background:var(--surface); border:1px solid var(--border); border-radius:12px;
    width:90%; max-width:1040px; height:min(82vh,760px); display:flex; flex-direction:column;
    padding:14px; box-shadow:0 24px 70px rgba(0,0,0,.25); }
  .log-toolbar { display:flex; justify-content:space-between; align-items:center; gap:8px;
    margin-bottom:8px; flex-wrap:wrap; }
  .log-toolbar .btn { margin:0; padding:4px 9px; }
  #logContent { overflow:auto; background:var(--terminal); color:var(--terminal-text);
    padding:12px; border-radius:7px; font-size:.76rem; line-height:1.45; flex:1;
    white-space:pre-wrap; margin:0; tab-size:2; }
  #logMeta { color:var(--muted); font-size:.75rem; margin-bottom:7px; min-height:1.2em; }
  @media (max-width:800px) {
    .grid2,.grid3 { grid-template-columns:1fr; }
    .workflow { grid-template-columns:repeat(2,1fr); }
    .container { padding:12px; }
    header,.job-head,.job-summary { align-items:flex-start; flex-direction:column; }
    .job-actions { justify-content:flex-start; }
    .log-dialog { width:96%; height:90vh; }
    .otp-action-card { right:10px; bottom:10px; width:calc(100vw - 20px);
      max-height:52vh; }
    .otp-action-head,.job-otp-region-head { align-items:flex-start;
      flex-direction:column; }
    .otp-action-card.otp-minimized .otp-action-head { align-items:center;
      flex-direction:row; }
    .otp-controls { grid-template-columns:1fr; }
  }
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>Batch Console</h1>
    <div>
      <a href="/fleet" id="navFleet">Fleet Dashboard</a>
      &nbsp;·&nbsp;
      <a href="#" onclick="toggleTheme();return false" id="themeToggle">切换深色</a>
      &nbsp;·&nbsp;
      <a href="#" onclick="forgetKey();return false" style="color:var(--muted)">换 Key</a>
    </div>
  </header>

  <div class="card">
    <h2>Job 怎么运行</h2>
    <p class="muted">填写代码与命令后先点「Validate / Preview」，确认计划再启动。日志用于排错，结果目录才会收集并上传 S3；Job 结束后临时 Worker 会自动销毁。</p>
    <div class="workflow">
      <div class="workflow-step"><b>1</b>申请机器</div>
      <div class="workflow-step"><b>2</b>初始化环境</div>
      <div class="workflow-step"><b>3</b>登录账号</div>
      <div class="workflow-step"><b>4</b>运行命令</div>
      <div class="workflow-step"><b>5</b>收集结果</div>
      <div class="workflow-step"><b>6</b>销毁 Worker</div>
    </div>
  </div>

  <div class="card action-card otp-action-card" id="loginActionCard"
       role="alert" style="display:none">
    <div class="otp-action-head">
      <div>
        <h2 id="loginActionTitle">⚠️ Worker 等待登录验证码</h2>
        <p class="muted">
          只有上报需要人工验证的 Worker 才会显示。每条提示都绑定到一个账号、一个 Worker
          和一次登录请求，请在过期前填写 OpenAI 邮件中的 6 位验证码。
        </p>
      </div>
      <button class="btn" id="loginActionButton"
              onclick="toggleOtpActionCard()">查看并填写</button>
    </div>
    <div id="loginAttemptLinks" class="otp-jump-list"></div>
    <div id="loginAttempts" class="job-otp-list"></div>
  </div>

  <!-- Accounts -->
  <div class="card">
    <h2>Accounts</h2>
    <p class="hint">
      Manager 会把登录密码与接码查询 token 保存到权限 0600 的账号文件，提交后均不回显。
      Claude/Codex OAuth 凭证只在 worker 生成且不回传。Codex 至少配置 OpenAI 密码或接码查询 Token
      之一，也可同时配置。查询 Token 不是 OpenAI 登录凭据，只用于从接码平台读取 OpenAI 发出的邮箱验证码；
      仅有 Token 时会切换到邮箱验证码并自动取码。没有可用查询 Token、自动查询失败，或自动验证码被拒绝时，
      只有对应 Worker 才会弹出人工验证码卡。
    </p>
    <table><thead><tr><th>ID</th><th>Agent</th><th>Email</th><th>Secrets</th>
      <th>Group</th><th>Enabled</th><th>EIP / 当前 Worker</th><th></th></tr></thead>
      <tbody id="acctRows"></tbody></table>
    <div class="grid3" style="margin-top:12px">
      <div><label>ID</label><input id="acctId" placeholder="acc-1"></div>
      <div><label>Email</label><input id="acctEmail" placeholder="a@x.com"></div>
      <div><label>Agent</label><select id="acctAgent">
        <option value="claude">Claude</option><option value="codex">Codex</option>
      </select></div>
    </div>
    <div class="grid3">
      <div><label>登录密码（Codex 至少填写一项，可同时填写；写入后不回显）</label>
        <input id="acctPassword" type="password" placeholder="OpenAI password">
        <label style="margin-top:5px"><input id="acctClearPassword" type="checkbox" style="width:auto">
          清除该账号已有登录密码</label></div>
      <div><label>接码查询 Token（只用于读取邮箱验证码；写入后不回显）</label>
        <input id="acctToken" type="password" placeholder="171mail / MailCatcher query token">
        <label style="margin-top:5px"><input id="acctClearToken" type="checkbox" style="width:auto">
          清除该账号已有查询 token</label></div>
      <div><label>Group</label><input id="acctGroup" value="standard"></div>
    </div>
    <button class="btn" onclick="addAccount()">Add Account</button>
  </div>

  <!-- Job submission -->
  <div class="card">
    <h2>Submit Job</h2>
    <div class="grid3">
      <div><label>Job name</label><input id="jName" placeholder="ai4sci-opus48-seed128"></div>
      <div><label>Workers (fan-out)</label><input id="jWorkers" type="number" value="1" min="1"></div>
      <div><label>Environment profile（固定通用环境）</label>
        <select id="jProfile"><option value="ubuntu-agent-v1">ubuntu-agent-v1</option>
          <option value="ubuntu-agent-docker-v1">ubuntu-agent-docker-v1</option></select></div>
    </div>
    <div class="grid3">
      <div><label>机器命名前缀（EC2 Name=前缀-i；空=用 Job name）</label>
        <input id="jNamePrefix" placeholder="my-fleet"></div>
      <div><label>机型 instance_type（空=Manager 默认）</label>
        <select id="jInstanceType">
          <option value="">（默认）</option>
          <optgroup label="通用/便宜">
            <option>t3.large</option><option>t3.xlarge</option><option>t3.2xlarge</option>
            <option>m5.xlarge</option><option>m5.2xlarge</option><option>m5.4xlarge</option>
          </optgroup>
          <optgroup label="内存型 (ai4sci 建议)">
            <option>r5.large</option><option>r5.xlarge</option><option>r5.2xlarge</option>
            <option>r5.4xlarge</option><option>r5.8xlarge</option>
          </optgroup>
          <optgroup label="计算型">
            <option>c5.xlarge</option><option>c5.2xlarge</option><option>c5.4xlarge</option><option>c5.9xlarge</option>
          </optgroup>
        </select></div>
      <div><label>Region（空=当前 Manager 区域；目前不支持跨区）</label>
        <input id="jRegion" placeholder="留空；仅可填当前 Manager 配置的区域"></div>
    </div>
    <div class="grid2">
      <div><label>根盘 disk_gb（0=Manager 默认；吃盘任务如 ai4sci 建议 ≥60）</label>
        <input id="jDiskGb" type="number" value="0" min="0"></div>
    </div>
    <label>Setup — repo URL（clone 到「代码目录」= target_dir）</label>
    <input id="jRepo" placeholder="https://github.com/ApexIntelligence-AI/Agent-AI4Sci-Bench.git">
    <div class="grid3">
      <div><label>Repo branch/tag ref</label><input id="jRepoRef" value="main"></div>
      <div><label>Resolved commit（推荐，完整 40 位 SHA）</label><input id="jResolvedCommit" placeholder="精确复现时填写"></div>
      <div><label>代码目录 target_dir（绝对路径）</label>
        <input id="jTargetDir" value="/opt/elastic-agent/harness"></div>
    </div>
    <label>代码分发方式</label>
    <select id="jDeliver">
      <option value="manager_rsync">manager_rsync（私有 repo 推荐：token 只在 Manager，clone 后 rsync 到 worker，token 不上机）</option>
      <option value="worker_clone">worker_clone（公开 repo：worker 自己 git clone）</option>
    </select>
    <label>Setup — commands（每行一条,在代码目录里跑；默认已装 uv 并 pin Python 3.13）</label>
    <textarea id="jSetup" placeholder="uv sync">curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH" && uv python pin 3.13 && uv sync --python 3.13</textarea>
    <details><summary class="muted">Structured setup steps（JSON，可单独设置 env/cwd/timeout/retries）</summary>
      <textarea id="jSetupSteps" style="min-height:90px" placeholder='[{"name":"install","command":"uv sync","env":{"UV_LINK_MODE":"copy"},"cwd":".","timeout":1200,"retries":1}]'></textarea>
      <div class="hint">每一步固定以 Job 用户运行；不允许指定 root 或其他用户名。旧的逐行 commands 仍兼容。</div>
    </details>
    <div class="hint">💡 契约：repo 会 clone 到「代码目录」,setup 和 run 命令<b>都从这个目录跑</b>——你照「本地 <code>git clone && cd repo && …</code>」那样写命令即可。</div>
    <div class="grid2">
      <div><label>需要 Docker（run 用 Docker,如 ai4sci <code>--sandbox os</code>）</label>
        <select id="jNeedsDocker"><option value="false">否</option><option value="true">是</option></select></div>
      <div><label>S3 数据集（每行 <code>s3://桶/前缀/ 目标目录</code>；worker 用实例角色直拉，不经 Manager）</label>
        <textarea id="jS3" placeholder="s3://my-bucket/datasets/ /home/ubuntu/data"></textarea></div>
    </div>
    <label>Run command（shell;从代码目录运行;支持 {{shard_index}} 和 $(hostname -s)）</label>
    <textarea id="jRun" placeholder='uv run ai4sci-bench run --output-dir "results/opus48_$(hostname -s)_seed128"'></textarea>
    <div class="grid3">
      <div><label>Working dir（空/. = 代码目录;相对路径=其子目录）</label><input id="jCwd" value="."></div>
      <div><label>Shard by</label>
        <select id="jShard"><option value="hostname">hostname</option>
          <option value="shard_index">shard_index</option><option value="none">none</option></select></div>
      <div><label>Shell mode</label><select id="jShell"><option value="true">bash -lc</option>
        <option value="false">direct argv</option></select></div>
    </div>
    <div class="grid2">
      <div><label>Run timeout 秒（默认 24h，最长 30 天）</label>
        <input id="jRunTimeout" type="number" value="86400" min="60" max="2592000"></div>
      <div><label>Job TTL 秒（含启动/登录/收集；默认 48h）</label>
        <input id="jTtl" type="number" value="172800" min="300" max="2592000"></div>
    </div>
    <label>Env (KEY=VALUE per line)</label>
    <textarea id="jEnv" placeholder="AI4SCI_SANDBOX_CPU=1&#10;AI4SCI_SANDBOX_MEM=4g"></textarea>
    <label>Secret env references（KEY=aws-secretsmanager://... 或 KEY=aws-ssm://...）</label>
    <textarea id="jSecretEnv" placeholder="OPENAI_API_KEY=aws-secretsmanager://prod/openai#api_key&#10;DB_PASSWORD=aws-ssm:///prod/db/password"></textarea>
    <div class="hint">只提交 AWS 引用；明文仅在命令下发前解析，不写回 JobSpec，也不在 API 中回显。</div>
    <div class="grid2">
      <div><label>结果收集目录 collect.paths（每行一个,相对代码目录,如 results）</label>
        <textarea id="jCollect" placeholder="results">results</textarea></div>
      <div><label>增量收集间隔秒（0=只在完成时收集；&gt;0=边跑边收→持续上 S3，长跑推荐 120）</label>
        <input id="jCollectInterval" type="number" value="0" min="0"></div>
    </div>
    <div class="grid3">
      <div><label>Account mode</label>
        <select id="jAcctMode" onchange="updateAccountModeUI()">
          <option value="worker_local_login">worker_local_login</option>
          <option value="manager_distribute">manager_distribute</option><option value="none">none</option></select></div>
      <div><label>Agent</label><select id="jAgentType" onchange="updateAgentUI()">
        <option value="claude">Claude</option><option value="codex">Codex</option>
      </select></div>
      <div><label>Account group</label><input id="jAcctGroup" value="standard"></div>
    </div>
    <div class="grid3">
      <div><label>config_dir（空 = Agent 默认目录）</label>
        <input id="jConfigDir" placeholder="留空，或填写 worker 上的绝对路径"></div>
      <div><label>Accounts per worker</label>
        <input id="jPerWorker" type="number" value="1" min="1" max="32"></div>
      <div><label>自动登录页面超时秒（60–1200）</label>
        <input id="jLoginTimeout" type="number" value="900" min="60" max="1200"></div>
    </div>
    <div class="grid2">
      <div><label>账号固定 EIP</label>
        <select id="jAcctBinding" onchange="markEipBindingTouched()">
          <option value="none">关闭（普通临时 EC2）</option>
          <option value="eip">启用（一号一 IP）</option>
        </select></div>
      <div><label>指定账号（Ctrl/Cmd 多选；留空则按 Group 自动选择）</label>
        <select id="jAcctIds" multiple size="4" disabled></select></div>
    </div>
    <div class="hint" id="jEipHint">
      启用后，一个账号固定绑定一个 IPv4 EIP，每台临时 EC2 只使用一个账号；如指定账号，
      选中账号数必须等于 Workers。新 EC2 仍会重新登录 Claude，任务结束先收集结果再销毁 EC2，
      但保留 EIP。Claude 与 Codex 账号都按 account_id 绑定各自 EIP。
    </div>
    <div class="grid2">
      <div><label>Rotation strategy</label>
        <select id="jRot"><option value="none">none</option>
          <option value="on_exhaust_restart_resume">on_exhaust_restart_resume (a)</option></select></div>
      <div><label>Resume args (appended on rotation restart)</label>
        <input id="jResume" placeholder='--resume "results/opus48_$(hostname -s)_seed128"'></div>
    </div>
    <div class="grid2">
      <div><label>Max rotations</label><input id="jMaxRotations" type="number" value="20" min="0" max="100"></div>
      <div><label>Spot instance</label><select id="jSpot"><option value="false">否</option><option value="true">是</option></select></div>
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

    <div>
      <button class="btn btn-ghost" id="jPlanBtn" onclick="previewJob()">Validate / Preview</button>
      <button class="btn" id="jSubmitBtn" onclick="submitJob()">Launch Job</button>
    </div>
    <pre id="jPlanOutput" style="display:none;white-space:pre-wrap;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px;margin-top:10px;font-size:.72rem"></pre>
  </div>

  <!-- Jobs monitor -->
  <div class="card">
    <div style="display:flex;justify-content:space-between;align-items:center;gap:8px">
      <h2>Jobs <span class="muted" id="jobsRefresh"></span></h2>
      <button class="btn btn-ghost" id="historyToggle" style="display:none;margin:0"
        onclick="toggleJobHistory()">显示旧历史</button>
    </div>
    <p class="hint" style="margin-bottom:10px">
      Job 默认收起，点击摘要查看详情。失败时先看「任务输出」中的 stderr；
      命令 stdout/stderr 在 Worker 销毁后仍可查看，默认保留 30 天；
      登录、SSH、systemd 问题可在 Worker 存活时看「系统日志」。
    </p>
    <div id="jobsList"><p class="muted">No jobs yet.</p></div>
  </div>

  <!-- Job run output remains queryable after the temporary Worker is gone. -->
  <div id="logModal" style="display:none;position:fixed;inset:0;background:var(--overlay);z-index:1000;align-items:center;justify-content:center" onclick="if(event.target===this)closeLogs()">
    <div class="log-dialog" role="dialog" aria-modal="true" aria-labelledby="logTitle">
      <div class="log-toolbar">
        <b id="logTitle">任务输出</b>
        <span>
          <button class="btn btn-ghost" onclick="refreshOpenLogs(true)">↻ 刷新</button>
          <button class="btn btn-ghost" id="logPauseBtn" onclick="toggleLogPause()">暂停刷新</button>
          <button class="btn btn-ghost" id="logFollowBtn" onclick="toggleLogFollow()">✓ 跟随最新</button>
          <button class="btn btn-ghost" onclick="copyLogs()">复制日志</button>
          <button class="btn btn-ghost" onclick="downloadLogText()">下载 .txt</button>
          <button class="btn btn-ghost" id="logCloseBtn" onclick="closeLogs()">✕ 关闭</button>
        </span>
      </div>
      <div id="logMeta" role="status" aria-live="polite"></div>
      <pre id="logContent" tabindex="0"></pre>
    </div>
  </div>

  <!-- Collected results (browsable / downloadable) -->
  <div class="card">
    <div style="display:flex;justify-content:space-between;align-items:center">
      <h2>已收集结果 <span class="muted">· 点击下载全部</span></h2>
      <button class="btn btn-ghost" style="margin:0" onclick="refreshVisibleResults(true)">刷新结果</button>
    </div>
    <div id="resultsList"><p class="muted">No results yet.</p></div>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
const _params = new URLSearchParams(window.location.search);
if (_params.has('api_key')) {
  _params.delete('api_key');
  history.replaceState(null, '', window.location.pathname + (_params.size ? '?' + _params : ''));
}
let API_KEY = sessionStorage.getItem('ea_api_key') || '';
if (!API_KEY) {
  const k = (window.prompt('请输入 API Key：') || '').trim();
  if (k) { sessionStorage.setItem('ea_api_key', k); API_KEY = k; }
}
const headers = API_KEY ? {'Authorization':`Bearer ${API_KEY}`,'Content-Type':'application/json'}
                        : {'Content-Type':'application/json'};
{const nav = document.getElementById('navFleet'); if (nav) nav.href = '/fleet';}
let eipBindingTouched = false;
let providerType = '';
let providerDefaultsReady;
let latestJobs = [];
let showLegacyHistory = false;
let dashboardPollRunning = false;
let dashboardPollTimer = null;
const jobResultsCache = new Map();
const jobResultsRequestVersions = new Map();
const resultDownloadsInFlight = new Map();
let latestLoginAttempts = [];
const otpCardsByKey = new Map();
const openedOtpChallenges = new Set();
const otpSubmitting = new Set();
function forgetKey() { sessionStorage.removeItem('ea_api_key'); location.href = '/'; }
function updateThemeLabel() {
  const button = document.getElementById('themeToggle');
  if (button) button.textContent =
    document.documentElement.dataset.theme === 'dark' ? '切换亮色' : '切换深色';
}
function toggleTheme() {
  const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
  document.documentElement.dataset.theme = next;
  sessionStorage.setItem('ea_theme', next);
  updateThemeLabel();
}
async function api(method, path, body, extraHeaders={}) {
  const opts = {method, headers:{...headers, ...extraHeaders}};
  if (body) opts.body = JSON.stringify(body);
  const resp = await fetch('/api' + path, opts);
  if (!resp.ok) {
    const error = new Error(`${resp.status}: ${await resp.text()}`);
    error.status = resp.status;
    throw error;
  }
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
function esc(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  })[ch]);
}
function jsArg(value) { return esc(JSON.stringify(String(value ?? ''))); }

// ---- Accounts ----
async function refreshAccounts() {
  try {
    const d = await api('GET', '/accounts');
    const accounts = d.accounts || [];
    let alloc = {};
    let eipBindings = {};
    try { alloc = (await api('GET', '/accounts/allocations')).allocations || {}; } catch(e) {}
    try {
      const response = await api('GET', '/accounts/bindings');
      (response.bindings || []).forEach(binding => {
        eipBindings[binding.account_id] = binding;
      });
    } catch(e) {}
    document.getElementById('acctRows').innerHTML = accounts.map(a => {
      const b = alloc[a.id] || [];
      const active = b.length
        ? b.map(x => `${esc((x.worker_id||'').replace('aws:',''))} `
          + `<span class="muted">(${esc(x.job_name||x.job_id)}·`
          + `${esc(x.phase)}${x.active?'·当前':''})</span>`).join('<br>')
        : '<span class="muted">空闲</span>';
      const durable = eipBindings[a.id];
      const eipValue = durable
        ? durable.eip_ip || durable.eip_allocation_id || '分配中'
        : '';
      const eip = durable
        ? `${esc(eipValue)} <span class="muted">(${esc(durable.state)})</span>`
        : '<span class="muted">无 EIP</span>';
      const secrets = `${a.has_password ? 'password' : ''}`
        + `${a.has_password && a.has_email_token ? ' + ' : ''}`
        + `${a.has_email_token ? 'mail token' : ''}` || '—';
      return `<tr><td>${esc(a.id)}</td><td>${esc(a.agent_type)}</td><td>${esc(a.email)}</td>
        <td>${esc(secrets)}</td><td>${esc(a.group)}</td><td>${esc(a.enabled)}</td>
        <td style="font-size:.72rem">${eip}<br>${active}</td>
        <td><button class="btn btn-danger" style="margin:0;padding:3px 9px"
            onclick="removeAccount(${jsArg(a.id)})">✕</button></td></tr>`;
    }).join('') || '<tr><td colspan="8" class="muted">No accounts.</td></tr>';

    const picker = document.getElementById('jAcctIds');
    const selectedAgent = document.getElementById('jAgentType').value;
    const selected = new Set(Array.from(picker.selectedOptions).map(o => o.value));
    picker.replaceChildren();
    accounts.forEach(a => {
      const option = document.createElement('option');
      option.value = a.id;
      option.dataset.agentType = a.agent_type;
      option.dataset.enabled = String(Boolean(a.enabled));
      const durable = eipBindings[a.id];
      const eipLabel = durable
        ? ` · EIP ${durable.eip_ip || durable.eip_allocation_id || durable.state}`
        : '';
      option.textContent = `${a.agent_type} · ${a.email || a.id} · ${a.group || 'standard'} (${a.id})${eipLabel}`;
      option.disabled = !a.enabled || a.agent_type !== selectedAgent;
      option.selected = selected.has(a.id) && !option.disabled;
      picker.appendChild(option);
    });
    updateEipBindingUI();
  } catch(e) { toast(e.message, 'error'); }
}
async function addAccount() {
  const id = document.getElementById('acctId').value.trim();
  const email = document.getElementById('acctEmail').value.trim();
  const agentType = document.getElementById('acctAgent').value;
  const password = document.getElementById('acctPassword').value;
  const emailToken = document.getElementById('acctToken').value.trim();
  if (!id || !email) return toast('id + email required', 'error');
  try {
    await api('POST', '/accounts', {id, email,
      agent_type: agentType,
      password: password,
      clear_password: document.getElementById('acctClearPassword').checked,
      email_token: emailToken,
      clear_email_token: document.getElementById('acctClearToken').checked,
      group: document.getElementById('acctGroup').value.trim() || 'standard'});
    document.getElementById('acctId').value = ''; document.getElementById('acctEmail').value = '';
    document.getElementById('acctPassword').value = '';
    document.getElementById('acctClearPassword').checked = false;
    document.getElementById('acctToken').value = '';
    document.getElementById('acctClearToken').checked = false;
    toast('Account added'); refreshAccounts();
  } catch(e) { toast(e.message, 'error'); }
}

function otpKey(attempt) {
  return String(attempt.login_request_id || '') + ':'
    + String(attempt.challenge_id || '');
}
function createOtpCard() {
  const card = document.createElement('article');
  card.className = 'otp-challenge-card';
  const title = document.createElement('b');
  title.className = 'otp-title';
  const context = document.createElement('div');
  context.className = 'otp-context muted';
  for (const className of [
    'otp-account-email', 'otp-account-id', 'otp-worker', 'otp-job',
  ]) {
    const line = document.createElement('span');
    line.className = className;
    context.appendChild(line);
  }
  const explanation = document.createElement('div');
  explanation.className = 'hint otp-explanation';
  explanation.textContent =
    '邮箱自动取码不可用或未成功，需要人工输入 OpenAI 邮件中的验证码。';
  const expiry = document.createElement('div');
  expiry.className = 'hint otp-expiry';
  const controls = document.createElement('div');
  controls.className = 'otp-controls';
  const input = document.createElement('input');
  input.className = 'otp-code';
  input.type = 'text';
  input.inputMode = 'numeric';
  input.autocomplete = 'one-time-code';
  input.maxLength = 6;
  input.placeholder = '6 位验证码';
  input.setAttribute('aria-label', 'OpenAI 6 位验证码');
  const button = document.createElement('button');
  button.className = 'btn otp-submit';
  button.textContent = '提交给这个 Worker';
  button.addEventListener('click', event =>
    submitLoginOtp(event.currentTarget));
  input.addEventListener('keydown', event => {
    if (event.key === 'Enter') button.click();
  });
  controls.append(input, button);
  card.append(title, context, explanation, expiry, controls);
  return card;
}
function updateOtpCard(card, attempt) {
  const key = otpKey(attempt);
  const email = String(attempt.account_email || '邮箱未知');
  const accountId = String(attempt.account_id || '账号未知');
  const workerId = String(attempt.worker_id || 'Worker 未知');
  const jobId = String(attempt.job_id || '');
  const jobName = String(attempt.job_name || jobId || 'Job 尚未关联');
  const hasShard = attempt.shard_index !== null
    && attempt.shard_index !== undefined && attempt.shard_index !== '';
  const shard = hasShard ? `shard-${Number(attempt.shard_index)}` : 'shard 未知';
  card.dataset.otpKey = key;
  card.dataset.loginRequestId = String(attempt.login_request_id || '');
  card.dataset.challengeId = String(attempt.challenge_id || '');
  card.dataset.workerId = workerId;
  card.dataset.accountId = accountId;
  card.dataset.jobId = jobId;
  card.querySelector('.otp-title').textContent = `Codex OTP · ${email}`;
  card.querySelector('.otp-account-email').textContent = `账号邮箱：${email}`;
  card.querySelector('.otp-account-id').textContent = `账号 ID：${accountId}`;
  card.querySelector('.otp-worker').textContent = `Worker：${workerId}`;
  card.querySelector('.otp-job').textContent =
    `Job：${jobName}${jobId && jobName !== jobId ? ` (${jobId})` : ''} · ${shard}`;
  const seconds = Math.max(
    0, Number(attempt.expires_at || 0) - Date.now() / 1000,
  );
  card.querySelector('.otp-expiry').textContent = seconds > 0
    ? `约 ${Math.ceil(seconds / 60)} 分钟后过期`
    : '验证码已过期，等待 Job 收敛';
  const button = card.querySelector('.otp-submit');
  button.dataset.loginRequestId = String(attempt.login_request_id || '');
  button.dataset.challengeId = String(attempt.challenge_id || '');
  const submitting = otpSubmitting.has(key)
    || String(attempt.status || '') === 'submitting_otp';
  button.disabled = submitting;
  button.textContent = submitting ? '正在提交…' : '提交给这个 Worker';
}
function insertOtpCard(container, card, index) {
  const current = container.children[index] || null;
  if (card !== current) container.insertBefore(card, current);
}
function focusLoginAttempt(key) {
  const card = otpCardsByKey.get(String(key || ''));
  if (!card) return;
  const jobNode = card.closest('.job-row');
  if (jobNode) {
    jobNode.open = true;
    setOtpActionMinimized(true);
  }
  requestAnimationFrame(() => {
    const compactViewport = window.matchMedia('(max-width: 800px)').matches;
    card.scrollIntoView({
      behavior:compactViewport ? 'auto' : 'smooth',
      block:'center',
    });
    card.querySelector('.otp-code')?.focus({preventScroll:true});
  });
}
function focusFirstLoginAttempt() {
  const first = otpCardsByKey.values().next().value;
  if (first) focusLoginAttempt(first.dataset.otpKey);
}
function setOtpActionMinimized(minimized) {
  const actionCard = document.getElementById('loginActionCard');
  actionCard.classList.toggle('otp-minimized', Boolean(minimized));
  document.getElementById('loginActionButton').textContent =
    minimized ? '展开提醒' : '查看并填写';
}
function toggleOtpActionCard() {
  const actionCard = document.getElementById('loginActionCard');
  if (actionCard.classList.contains('otp-minimized')) {
    setOtpActionMinimized(false);
    return;
  }
  focusFirstLoginAttempt();
}
function reconcileOtpJumpLinks(attempts) {
  const container = document.getElementById('loginAttemptLinks');
  const wanted = new Set(attempts.map(otpKey));
  Array.from(container.querySelectorAll('.otp-jump')).forEach(button => {
    if (!wanted.has(button.dataset.otpKey)) button.remove();
  });
  attempts.forEach((attempt, index) => {
    const key = otpKey(attempt);
    let button = Array.from(container.querySelectorAll('.otp-jump'))
      .find(candidate => candidate.dataset.otpKey === key);
    if (!button) {
      button = document.createElement('button');
      button.className = 'btn btn-ghost otp-jump';
      button.addEventListener('click', event =>
        focusLoginAttempt(event.currentTarget.dataset.otpKey));
    }
    button.dataset.otpKey = key;
    button.textContent = `${attempt.account_email || attempt.account_id}`
      + ` · Worker ${attempt.worker_id}`;
    const current = container.children[index] || null;
    if (button !== current) container.insertBefore(button, current);
  });
}
function reconcileLoginAttempts(attempts) {
  latestLoginAttempts = Array.isArray(attempts) ? attempts : [];
  const actionCard = document.getElementById('loginActionCard');
  const fallback = document.getElementById('loginAttempts');
  const wanted = new Set(latestLoginAttempts.map(otpKey));
  let hasNewChallenge = false;
  for (const [key, card] of otpCardsByKey.entries()) {
    if (wanted.has(key)) continue;
    card.remove();
    otpCardsByKey.delete(key);
    openedOtpChallenges.delete(key);
    otpSubmitting.delete(key);
  }

  const jobContexts = new Map();
  const destinationIndexes = new Map();
  latestLoginAttempts.forEach(attempt => {
    const key = otpKey(attempt);
    let card = otpCardsByKey.get(key);
    if (!card) {
      card = createOtpCard();
      otpCardsByKey.set(key, card);
      hasNewChallenge = true;
    }
    updateOtpCard(card, attempt);

    const jobId = String(attempt.job_id || '');
    const jobNode = jobId ? document.getElementById('jobrow-' + jobId) : null;
    const region = jobNode?.querySelector('.job-otp-region');
    const destination = region?.querySelector('.job-otp-list') || fallback;
    const destinationIndex = destinationIndexes.get(destination) || 0;
    insertOtpCard(destination, card, destinationIndex);
    destinationIndexes.set(destination, destinationIndex + 1);

    if (jobNode && region) {
      const context = jobContexts.get(jobId) || {
        jobNode,
        region,
        badge: jobNode.querySelector('.job-otp-summary-badge'),
        count: 0,
      };
      context.count += 1;
      jobContexts.set(jobId, context);
      if (!openedOtpChallenges.has(key)) {
        jobNode.open = true;
        openedOtpChallenges.add(key);
      }
    }
  });

  for (const context of jobContexts.values()) {
    context.region.hidden = false;
    context.region.querySelector('.job-otp-count').textContent =
      `${context.count} 个 Worker 等待验证码`;
    if (context.badge) {
      context.badge.hidden = false;
      context.badge.textContent = `⚠ ${context.count} 个 Worker 等待验证码`;
    }
  }
  document.querySelectorAll('.job-row').forEach(jobNode => {
    if (jobContexts.has(String(jobNode.dataset.jobId || ''))) return;
    const badge = jobNode.querySelector('.job-otp-summary-badge');
    const region = jobNode.querySelector('.job-otp-region');
    if (badge) badge.hidden = true;
    if (region) region.hidden = true;
  });
  reconcileOtpJumpLinks(latestLoginAttempts);
  document.getElementById('loginActionTitle').textContent =
    `⚠️ ${latestLoginAttempts.length} 个 Worker 等待登录验证码`;
  if (hasNewChallenge || !latestLoginAttempts.length) {
    setOtpActionMinimized(false);
  }
  actionCard.style.display = latestLoginAttempts.length ? 'block' : 'none';
}
async function refreshLoginAttempts() {
  try {
    const data = await api('GET', '/accounts/login-attempts');
    const attempts = data.attempts || [];
    reconcileLoginAttempts(attempts);
  } catch(e) { /* coordinator may not be initialized until the first Job */ }
}
async function submitLoginOtp(source) {
  const card = source.closest('.otp-challenge-card');
  if (!card) return;
  const requestId = String(source.dataset.loginRequestId || '');
  const challengeId = String(source.dataset.challengeId || '');
  const key = card.dataset.otpKey;
  const input = card.querySelector('.otp-code');
  const code = (input?.value || '').trim();
  if (!/^\\d{6}$/.test(code)) return toast('验证码必须是 6 位数字', 'error');
  if (otpSubmitting.has(key)) return;
  otpSubmitting.add(key);
  source.disabled = true;
  source.textContent = '正在提交…';
  try {
    await api('POST', '/accounts/login-attempts/' + encodeURIComponent(requestId) + '/otp',
      {challenge_id: challengeId, code: code});
    if (input) input.value = '';
    toast(`验证码已提交给账号 ${card.dataset.accountId}`
      + ` · Worker ${card.dataset.workerId}`);
    await refreshLoginAttempts();
  } catch(e) {
    toast(e.message, 'error');
  } finally {
    otpSubmitting.delete(key);
    if (source.isConnected) {
      source.disabled = false;
      source.textContent = '提交给这个 Worker';
    }
  }
}
async function terminateWorker(wid) {
  if (!window.confirm('终止 worker ' + wid + ' ？该 EC2 实例会被销毁（失败/空转的 worker 用它清理，省钱）。')) return;
  try {
    await api('POST', '/scale-in', {node_ids: [wid], force: true});
    toast('已提交终止 ' + wid); refreshJobs();
  }
  catch(e) { toast(e.message, 'error'); }
}
async function removeAccount(id) {
  try { await api('DELETE', '/accounts/' + encodeURIComponent(id)); toast('Removed'); refreshAccounts(); }
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
function buildKeyValueLines(id) {
  const env = {};
  for (const l of lines(id)) { const i = l.indexOf('='); if (i > 0) env[l.slice(0,i)] = l.slice(i+1); }
  return env;
}
function buildEnv() { return buildKeyValueLines('jEnv'); }
function buildSecretEnv() { return buildKeyValueLines('jSecretEnv'); }
function markEipBindingTouched() {
  eipBindingTouched = true;
  updateEipBindingUI();
}
async function initializeProviderDefaults() {
  try {
    const health = await api('GET', '/health');
    providerType = health.provider || '';
    updateAccountModeUI();
  } catch(e) {}
}
function updateAccountModeUI() {
  const workerLocal = document.getElementById('jAcctMode').value === 'worker_local_login';
  const binding = document.getElementById('jAcctBinding');
  binding.disabled = !workerLocal;
  if (!workerLocal) binding.value = 'none';
  else if (providerType === 'aws' && !eipBindingTouched) binding.value = 'eip';
  updateEipBindingUI();
}
function updateEipBindingUI() {
  const enabled = document.getElementById('jAcctMode').value === 'worker_local_login'
    && document.getElementById('jAcctBinding').value === 'eip';
  const picker = document.getElementById('jAcctIds');
  const rotation = document.getElementById('jRot');
  const perWorker = document.getElementById('jPerWorker');
  picker.disabled = !enabled;
  if (enabled) perWorker.value = '1';
  perWorker.disabled = enabled;
  const restartOption = Array.from(rotation.options)
    .find(o => o.value === 'on_exhaust_restart_resume');
  if (restartOption) restartOption.disabled = enabled;
  if (enabled && rotation.value === 'on_exhaust_restart_resume') rotation.value = 'none';
}
function updateAgentUI() {
  const agentType = document.getElementById('jAgentType').value;
  const picker = document.getElementById('jAcctIds');
  const accountMode = document.getElementById('jAcctMode');
  const distribute = Array.from(accountMode.options)
    .find(option => option.value === 'manager_distribute');
  if (distribute) distribute.disabled = agentType === 'codex';
  if (agentType === 'codex' && accountMode.value === 'manager_distribute') {
    accountMode.value = 'worker_local_login';
  }
  updateAccountModeUI();
  Array.from(picker.options).forEach(option => {
    option.disabled = option.dataset.enabled !== 'true' || option.dataset.agentType !== agentType;
    if (option.disabled) option.selected = false;
  });
  document.getElementById('jConfigDir').placeholder =
    agentType === 'codex'
      ? '/home/ubuntu/.codex（示例；必须是绝对路径）'
      : '/home/ubuntu/.claude（示例；必须是绝对路径）';
}
function parseSetupSteps() {
  const raw = document.getElementById('jSetupSteps').value.trim();
  if (!raw) return [];
  const value = JSON.parse(raw);
  if (!Array.isArray(value)) throw new Error('Structured setup steps 必须是 JSON array');
  return value;
}
function buildJobSpec() {
  const ref = document.getElementById('jHarnessRef').value.trim();
  const workers = parseInt(document.getElementById('jWorkers').value) || 1;
  const accountBinding = document.getElementById('jAcctBinding').value;
  const accountIds = accountBinding === 'eip'
    ? Array.from(document.getElementById('jAcctIds').selectedOptions).map(o => o.value)
    : [];
  if (accountBinding === 'eip' && accountIds.length && accountIds.length !== workers) {
    throw new Error(`EIP 绑定模式下，选中账号数必须等于 Workers（当前 ${accountIds.length}/${workers}）`);
  }
  const repo = document.getElementById('jRepo').value.trim() || null;
  const setup = {
    repo: repo,
    target_dir: document.getElementById('jTargetDir').value.trim(),
    commands: lines('jSetup'), steps: parseSetupSteps(),
    deliver: document.getElementById('jDeliver').value,
    needs_docker: document.getElementById('jNeedsDocker').value === 'true',
    s3_datasets: lines('jS3').map(function(l){var p=l.trim().split(/ +/); return {uri:p[0], dest:p[1]||''};})
                  .filter(function(d){return d.uri && d.dest;})
  };
  if (repo) {
    setup.ref = document.getElementById('jRepoRef').value.trim();
    setup.resolved_commit = document.getElementById('jResolvedCommit').value.trim();
  }
  const spec = {
    name: document.getElementById('jName').value.trim() || 'job',
    environment: {profile: document.getElementById('jProfile').value},
    setup: setup,
    run: {command: document.getElementById('jRun').value.trim(),
          cwd: document.getElementById('jCwd').value.trim() || '.', env: buildEnv(),
          secret_env: buildSecretEnv(),
          timeout: parseInt(document.getElementById('jRunTimeout').value) || 86400,
          shell: document.getElementById('jShell').value === 'true'},
    ttl_seconds: parseInt(document.getElementById('jTtl').value) || 172800,
    account: {mode: document.getElementById('jAcctMode').value,
              agent_type: document.getElementById('jAgentType').value,
              group: document.getElementById('jAcctGroup').value.trim() || 'standard',
              per_worker: parseInt(document.getElementById('jPerWorker').value) || 1,
              config_dir: document.getElementById('jConfigDir').value.trim(),
              login_timeout_seconds: parseInt(document.getElementById('jLoginTimeout').value) || 900,
              binding: accountBinding,
              ids: accountIds},
    rotation: {strategy: document.getElementById('jRot').value,
               resume_args: document.getElementById('jResume').value.trim(),
               max_rotations: parseInt(document.getElementById('jMaxRotations').value) || 0},
    fanout: {workers: workers,
             shard_by: document.getElementById('jShard').value,
             name_prefix: document.getElementById('jNamePrefix').value.trim(),
             instance_type: document.getElementById('jInstanceType').value.trim(),
             region: document.getElementById('jRegion').value.trim(),
             disk_gb: parseInt(document.getElementById('jDiskGb').value) || 0,
             spot: document.getElementById('jSpot').value === 'true'},
    collect: {paths: lines('jCollect'),
              interval_seconds: parseInt(document.getElementById('jCollectInterval').value) || 0},
  };
  if (ref) spec.harness_ref = ref;
  return spec;
}
function showJobPlan(plan) {
  const output = document.getElementById('jPlanOutput');
  output.style.display = 'block';
  output.textContent = JSON.stringify(plan, null, 2);
}
async function previewJob() {
  const button = document.getElementById('jPlanBtn');
  const label = button.textContent;
  button.disabled = true; button.textContent = 'Validating…';
  try {
    await providerDefaultsReady;
    const plan = await api('POST', '/jobs/plan', buildJobSpec());
    showJobPlan(plan); toast('Job plan valid'); return plan;
  } catch(e) { toast(e.message, 'error'); throw e; }
  finally { button.disabled = false; button.textContent = label; }
}
async function submitJob() {
  const btn = document.getElementById('jSubmitBtn');
  const label = btn ? btn.textContent : '';
  if (btn) { btn.disabled = true; btn.textContent = 'Launching…'; }
  try {
    await providerDefaultsReady;
    const spec = buildJobSpec();
    // Pure preflight first: no spec journal, account claim or EC2 is created
    // until this succeeds. The backend repeats the same check at submit time.
    const plan = await api('POST', '/jobs/plan', spec);
    showJobPlan(plan);
    const serialized = JSON.stringify(spec);
    if (!window._pendingJobSubmission || window._pendingJobSubmission.spec !== serialized) {
      window._pendingJobSubmission = {
        spec: serialized,
        key: (crypto.randomUUID ? crypto.randomUUID() : String(Date.now()) + '-' + Math.random())
      };
    }
    const j = await api('POST', '/jobs', spec, {
      'Idempotency-Key': window._pendingJobSubmission.key
    });
    window._pendingJobSubmission = null;
    toast('Launched ' + j.job_id); refreshJobs(); }
  catch(e) { toast(e.message, 'error'); }
  finally { if (btn) { btn.disabled = false; btn.textContent = label; } }
}

// ---- Jobs monitor ----
function badge(p) {
  const label = String(p || 'unknown');
  const cls = label.toLowerCase().replace(/[^a-z0-9_-]/g, '');
  return `<span class="badge b-${cls}">${esc(label)}</span>`;
}
function workerReleased(worker) {
  // `cleaned_up` keeps this UI compatible with a Manager rolling upgrade;
  // `worker_released` also covers ordinary (non-EIP) Job teardown.
  return worker.worker_released === true || worker.cleaned_up === true;
}
function workerExecutionTerminal(worker) {
  return ['done', 'failed', 'cancelled'].includes(String(worker.phase || ''));
}
function workerResourceHtml(worker) {
  if (workerReleased(worker)) {
    return '<span class="badge b-done">Worker 已销毁</span>';
  }
  if (!workerExecutionTerminal(worker)) {
    return '<span class="muted">活动或准备中</span>';
  }
  if (worker.worker_release_expected === false) {
    return '<span class="muted">按策略保留</span>';
  }
  if (worker.cleanup_error) {
    return '<span class="badge b-failed">清理失败，正在重试</span>';
  }
  if (typeof worker.worker_released !== 'boolean') {
    return '<span class="badge b-pending">资源状态未知</span>';
  }
  return '<span class="badge b-pending">等待清理</span>';
}
function jobStateLabel(state) {
  return ({
    preparing:'准备中', prepared:'已准备', launching:'启动中', running:'运行中',
    succeeded:'成功', failed:'失败', cancelled:'已取消', interrupted:'中断',
    recovered:'旧记录', unknown:'状态未知'
  })[String(state || '').toLowerCase()] || String(state || '未知');
}
function workerActionsHtml(worker, jobId) {
  const focusId = esc(worker.worker_id || `shard-${Number(worker.shard_index)||0}`);
  const outputLabel = String(worker.phase || '') === 'failed'
    ? '查看失败日志' : '任务输出';
  const taskLog = `<button class="btn btn-ghost" data-job-focus="worker-output-${focusId}"
    style="padding:2px 8px;font-size:.72rem"
    onclick="showJobLogs(${jsArg(jobId)},${jsArg(worker.worker_id || '')})">${outputLabel}</button>`;
  if (!worker.worker_id || workerReleased(worker)) {
    return taskLog;
  }
  const systemLog = `<button class="btn btn-ghost" data-job-focus="worker-system-${focusId}"
    style="padding:2px 8px;font-size:.72rem"
    onclick="showWorkerLogs(${jsArg(worker.worker_id)})">系统日志</button>`;
  const terminate = workerExecutionTerminal(worker) ? '' : `
    <button class="btn btn-danger" data-job-focus="worker-terminate-${focusId}"
      style="padding:2px 8px;font-size:.72rem"
      onclick="terminateWorker(${jsArg(worker.worker_id)})">终止</button>`;
  return `${taskLog}${systemLog}${terminate}`;
}
function formatDownloadBytes(value) {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
  const units = ['B','KiB','MiB','GiB'];
  let amount = bytes;
  let unit = 0;
  while (amount >= 1024 && unit < units.length - 1) {
    amount /= 1024; unit += 1;
  }
  const digits = unit === 0 || amount >= 100 ? 0 : amount >= 10 ? 1 : 2;
  return `${amount.toFixed(digits)} ${units[unit]}`;
}
function formatDownloadElapsed(startedAt) {
  const seconds = Math.max(0, Math.floor((Date.now() - startedAt) / 1000));
  if (seconds < 60) return `${seconds}秒`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}分${String(seconds % 60).padStart(2,'0')}秒`;
}
function formatResultDownloadLabel(state) {
  const elapsed = formatDownloadElapsed(state.startedAt);
  if (state.phase === 'choosing') return '选择保存位置…';
  if (state.phase === 'preparing') {
    return `服务器准备中 · ${elapsed}（点此取消）`;
  }
  if (state.phase === 'cancelling') return '正在取消…';
  if (state.phase === 'saving') return '正在保存…';
  if (state.total > 0) {
    const percent = Math.min(100, Math.floor(state.received * 100 / state.total));
    return `下载 ${percent}% · ${formatDownloadBytes(state.received)}（点此取消）`;
  }
  return `已接收 ${formatDownloadBytes(state.received)} · ${elapsed}（点此取消）`;
}
function resultDownloadCancellable(state) {
  return !['choosing','saving','cancelling'].includes(state.phase);
}
function repaintResultDownload(jobId, state, force=false) {
  if (resultDownloadsInFlight.get(jobId) !== state) return;
  const now = Date.now();
  if (!force && now - (state.lastPaint || 0) < 500) return;
  state.lastPaint = now;
  const label = formatResultDownloadLabel(state);
  const cancellable = resultDownloadCancellable(state);
  document.querySelectorAll('[data-result-download-job]').forEach(button => {
    if (button.dataset.resultDownloadJob !== jobId) return;
    button.textContent = label;
    button.title = cancellable
      ? '正在生成并下载当前结果快照；点击可取消'
      : '正在完成浏览器操作';
    button.dataset.resultAction = 'downloading';
    button.classList.add('btn-danger');
    button.disabled = !cancellable;
    if (cancellable) {
      button.removeAttribute('aria-disabled');
      button.onclick = () => cancelResultDownload(jobId);
    } else {
      button.setAttribute('aria-disabled', 'true');
      button.onclick = null;
    }
  });
}
function cancelResultDownload(rawJobId) {
  const jobId = String(rawJobId);
  const state = resultDownloadsInFlight.get(jobId);
  if (!state || !resultDownloadCancellable(state)) return;
  state.phase = 'cancelling';
  state.cancelled = true;
  state.controller.abort();
  repaintResultDownload(jobId, state, true);
}
async function downloadResults(rawJobId) {
  const jobId = String(rawJobId);
  if (resultDownloadsInFlight.has(jobId)) return;
  const state = {
    phase:'choosing', startedAt:Date.now(), received:0, total:0,
    sourceBytes:0, objectCount:0, lastPaint:0, cancelled:false,
    controller:new AbortController(), reader:null, writable:null, timer:null,
  };
  resultDownloadsInFlight.set(jobId, state);
  state.timer = setInterval(
    () => repaintResultDownload(jobId, state, true), 1_000,
  );
  repaintResultDownload(jobId, state, true);
  let fileHandle = null;
  let chunks = null;
  let streamComplete = false;
  try {
    if (window.isSecureContext && typeof window.showSaveFilePicker === 'function') {
      try {
        fileHandle = await window.showSaveFilePicker({
          suggestedName: jobId + '-results.tar.gz',
        });
      } catch (error) {
        if (error?.name === 'AbortError') throw error;
        // Older Chromium variants may expose the API but reject its options.
        // Fall back to the ordinary in-memory browser download below.
        fileHandle = null;
      }
    }
    state.phase = 'preparing';
    repaintResultDownload(jobId, state, true);
    toast('正在准备结果压缩包；按钮会显示传输量，点击可取消。');
    const response = await fetch(
      '/api/jobs/' + encodeURIComponent(jobId) + '/results/download/stream',
      {headers: {...headers}, signal: state.controller.signal}
    );
    if (!response.ok) {
      throw new Error(`${response.status}: ${await response.text()}`);
    }
    state.total = Number(response.headers.get('content-length')) || 0;
    state.sourceBytes = Number(
      response.headers.get('x-elastic-agent-source-bytes')
    ) || 0;
    state.objectCount = Number(
      response.headers.get('x-elastic-agent-object-count')
    ) || 0;
    state.phase = 'transferring';
    repaintResultDownload(jobId, state, true);

    if (!response.body) throw new Error('浏览器没有提供可读取的响应流');
    state.reader = response.body.getReader();
    if (fileHandle) {
      state.writable = await fileHandle.createWritable();
    } else {
      const fallbackBytes = Math.max(state.sourceBytes, state.total);
      if (fallbackBytes >= 256 * 1024 * 1024) {
        const sizeKind = state.sourceBytes > 0 ? '源文件' : '压缩包';
        throw new Error(
          `结果${sizeKind}约 ${formatDownloadBytes(fallbackBytes)}，当前浏览器`
          + '不能安全地直接写入磁盘；请使用 HTTPS 下的桌面版 Chrome 重试。'
        );
      }
      chunks = [];
    }
    while (true) {
      const {done, value} = await state.reader.read();
      if (done) break;
      if (!(value instanceof Uint8Array)) {
        throw new Error('下载响应包含无效数据');
      }
      state.received += value.byteLength;
      if (state.writable) await state.writable.write(value);
      else chunks.push(value);
      repaintResultDownload(jobId, state);
    }
    streamComplete = true;

    state.phase = 'saving';
    repaintResultDownload(jobId, state, true);
    if (state.writable) {
      await state.writable.close();
      state.writable = null;
    } else {
      const blob = new Blob(chunks, {type:'application/gzip'});
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url; link.download = jobId + '-results.tar.gz';
      document.body.appendChild(link); link.click(); link.remove();
      setTimeout(() => URL.revokeObjectURL(url), 60_000);
    }
    toast(`下载完成：${formatDownloadBytes(state.received)}`);
  } catch(e) {
    if (state.writable) {
      try { await state.writable.abort(); } catch (_) { /* best effort */ }
      state.writable = null;
    }
    if (state.cancelled || e?.name === 'AbortError') toast('下载已取消');
    else toast('下载失败：' + e.message, 'error');
  }
  finally {
    if (!streamComplete) {
      state.controller.abort();
      if (state.reader) {
        try { await state.reader.cancel(); } catch (_) { /* best effort */ }
      }
    }
    state.reader = null;
    clearInterval(state.timer);
    resultDownloadsInFlight.delete(jobId);
    reconcileJobCards(visibleJobs(latestJobs));
    refreshResults();
  }
}
async function cancelJob(jobId) {
  if (!window.confirm('取消 Job ' + jobId + '？将收集已有结果并销毁全部 Worker。')) return;
  try {
    await api('POST', '/jobs/' + encodeURIComponent(jobId) + '/cancel');
    toast('Job 已取消并进入清理：' + jobId); refreshJobs();
  } catch(e) { toast(e.message, 'error'); }
}
// Run async `fn` over items with at most `limit` in flight; never throws.
async function mapLimit(items, limit, fn) {
  const it = items[Symbol.iterator]();
  const runners = [];
  for (let k = 0; k < Math.min(limit, items.length); k++) {
    runners.push((async () => {
      for (let n = it.next(); !n.done; n = it.next()) {
        try { await fn(n.value); } catch (e) { /* ignore per-item */ }
      }
    })());
  }
  await Promise.all(runners);
}
function jobTime(job) {
  return Date.parse(job.created_at || job.started_at || job.completed_at || 0) || 0;
}
function sortedJobs(jobs) {
  return [...jobs].sort((a,b) => jobTime(b) - jobTime(a)
    || String(b.job_id).localeCompare(String(a.job_id)));
}
function isLegacyJob(job) {
  return job.in_memory === false
    && ['recovered','unknown'].includes(String(job.state || '').toLowerCase());
}
function visibleJobs(jobs) {
  const sorted = sortedJobs(jobs);
  const legacy = sorted.filter(isLegacyJob);
  const current = sorted.filter(job => !isLegacyJob(job));
  const active = current.filter(job => !job.done);
  const terminal = current.filter(job => job.done);
  const recentTerminal = terminal.slice(0, 24);
  const olderTerminal = terminal.slice(24);
  const hiddenHistory = [...olderTerminal, ...legacy];
  const visible = sortedJobs([
    ...active, ...recentTerminal, ...(showLegacyHistory ? hiddenHistory : []),
  ]);
  const toggle = document.getElementById('historyToggle');
  toggle.style.display = hiddenHistory.length ? 'inline-block' : 'none';
  toggle.textContent = showLegacyHistory
    ? `隐藏 ${hiddenHistory.length} 条历史`
    : `显示 ${hiddenHistory.length} 条历史`;
  return visible;
}
function toggleJobHistory() {
  showLegacyHistory = !showLegacyHistory;
  reconcileJobCards(visibleJobs(latestJobs));
}
function formatWhen(value) {
  if (!value) return '';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '' : date.toLocaleString();
}
function resultFor(jobId) {
  return jobResultsCache.get(jobId)?.value || null;
}
function resultFileCount(result) {
  const count = Number(result?.file_count);
  return Number.isFinite(count) && count > 0 ? Math.floor(count) : 0;
}
function jobResultActionHtml(job, result) {
  const jobId = String(job.job_id);
  const cached = jobResultsCache.get(jobId);
  const fileCount = resultFileCount(result);
  const downloadState = resultDownloadsInFlight.get(jobId);
  let state = 'empty';
  let label = '暂无结果';
  let enabled = false;
  let title = '当前尚未发现已收集文件';
  if (downloadState) {
    state = 'downloading'; label = formatResultDownloadLabel(downloadState);
    title = resultDownloadCancellable(downloadState)
      ? '正在生成并下载当前结果快照；点击可取消'
      : '正在完成浏览器操作';
    enabled = true;
  } else if (fileCount > 0) {
    state = 'available'; label = `⬇ 下载结果 (${fileCount})`;
    title = job.done ? '下载最终收集结果' : '下载当前已上传的中间结果快照';
    enabled = true;
  } else if (cached?.error) {
    const missing = Number(cached.errorStatus) === 404;
    const waiting = missing && !job.done;
    state = waiting ? 'checking' : (missing ? 'empty' : 'unavailable');
    label = waiting ? '⏳ 等待结果…' : (missing ? '暂无结果' : '结果暂不可用');
    title = waiting
      ? 'Job 仍在运行，等待首次结果收集'
      : missing
      ? 'Job 已结束，但没有可下载文件'
      : '结果查询暂时失败，页面会自动重试';
  } else if (!cached || cached.loading) {
    state = 'checking'; label = '⏳ 检查结果…';
    title = '正在检查结果是否已经可用';
  }
  const action = downloadState
    ? resultDownloadCancellable(downloadState)
      ? `onclick="cancelResultDownload(${jsArg(jobId)})"`
      : 'disabled aria-disabled="true"'
    : enabled
    ? `onclick="downloadResults(${jsArg(jobId)})"`
    : 'disabled aria-disabled="true"';
  return `<button class="${downloadState ? 'btn btn-danger' : 'btn btn-ghost'}"
    data-job-focus="job-results"
    data-result-download-job="${esc(jobId)}"
    data-result-action="${state}" title="${esc(title)}"
    ${action}>${label}</button>`;
}
function jobRowHtml(j, r) {
  const wd = j.workers_detail || [];
  const recordedWorkers = Number.isFinite(Number(j.workers))
    ? Number(j.workers) : wd.length;
  const state = String(j.state || (j.done ? 'succeeded' : 'preparing')).toLowerCase();
  const scoreStr = (r && r.scores && r.scores.length)
    ? r.scores.map(s => `${esc(s.task_id)} ${esc(s.prompt_level)}: <b>${Number(s.final_score||0).toFixed(1)}</b>`).join(' · ')
    : '';
  const dlBtn = jobResultActionHtml(j, r);
  const cancelBtn = !j.done && j.in_memory !== false
    ? `<button class="btn btn-danger" data-job-focus="job-cancel"
        onclick="cancelJob(${jsArg(j.job_id)})">取消 Job</button>`
    : '';
  const errors = [...new Set([
    j.error, j.note, j.cancel_reason,
    ...wd.map(worker => worker.error),
    ...wd.map(worker => worker.collection_error),
    ...wd.map(worker => worker.cleanup_error),
  ].filter(Boolean).map(String))];
  const cleanupPending = Number(j.cleanup_pending || 0);
  const outputLabel = state === 'failed' ? '📄 查看失败日志' : '📄 任务输出';
  const outputClass = state === 'failed' ? 'btn' : 'btn btn-ghost';
  const phases = Object.entries(j.phases || {})
    .map(([phase,count]) => badge(phase)+' '+(Number(count)||0)).join(' ');
  const created = formatWhen(j.created_at);
  return `
  <details id="jobrow-${esc(j.job_id)}" class="job-row job-${esc(state)}" data-job-id="${esc(j.job_id)}">
    <summary class="job-summary" data-job-focus="job-summary">
      <span class="job-summary-main">
        <span class="job-summary-title"><b>${esc(j.name||'')}</b> ${badge(state)}
          <span class="muted">${esc(j.job_id)}</span>
          <span class="job-otp-summary-badge" hidden></span></span>
        <span class="job-summary-meta muted" style="margin-top:5px">${phases || jobStateLabel(state)}
          ${created ? ` · 提交 ${esc(created)}` : ''}
          · ${recordedWorkers} 条 Worker 执行记录</span>
      </span>
      <span class="job-summary-toggle" aria-hidden="true">
        <span class="job-summary-closed">点击查看详情 ▾</span>
        <span class="job-summary-open">收起详情 ▴</span>
      </span>
    </summary>
    <div class="job-detail">
      <div class="job-head">
        <span class="muted">操作与运行详情</span>
        <div class="job-actions">
          <button class="${outputClass}" data-job-focus="job-output"
            onclick="showJobLogs(${jsArg(j.job_id)},'')">${outputLabel}</button>
          ${dlBtn}${cancelBtn}
        </div>
      </div>
      ${errors.length ? `<div class="job-alert">${errors.map(esc).join('\\n')}</div>` : ''}
      ${cleanupPending ? `<div class="job-alert cleanup-alert">正在清理 ${cleanupPending} 个 Worker / 租约，请勿重复提交同一账号。</div>` : ''}
      <section class="job-otp-region" hidden>
        <div class="job-otp-region-head">
          <b>⚠️ 此 Job 有 Worker 等待登录验证码</b>
          <span class="job-otp-count"></span>
        </div>
        <div class="job-otp-list"></div>
      </section>
      ${scoreStr ? `<div class="muted" style="margin-top:4px">📊 ${scoreStr}</div>` : ''}
      ${r && r.s3_uri ? `<div class="muted" style="font-size:.72rem">S3: ${esc(r.s3_uri)}</div>` : ''}
      <div class="worker-records-title muted">${recordedWorkers} 条 Worker 执行记录</div>
      <div class="hint">
        任务输出是命令 stdout/stderr，Worker 销毁后仍可查看；
        Worker 仍存活时可看 ea-runtime systemd journal（系统日志）。
      </div>
      <div class="table-scroll"><table><thead><tr><th>shard</th><th>worker</th><th>执行状态</th>
        <th>资源状态</th><th>accounts (加粗=当前使用)</th><th>rot</th><th>error</th><th>操作</th></tr></thead><tbody>
      ${wd.length ? wd.map(w => `<tr><td>${Number(w.shard_index)||0}</td>
        <td>${esc((w.worker_id||'').substring(0,14))}</td><td>${badge(w.phase)}</td>
        <td>${workerResourceHtml(w)}</td>
        <td>${(w.accounts&&w.accounts.length) ? w.accounts.map(a => a.active ? '<b>'+esc(a.email||a.account_id)+'</b>' : esc(a.email||a.account_id)).join('<br>') : esc(w.account_email||'--')}</td>
        <td>${Number(w.rotations)||0}</td>
        <td class="muted">${esc(w.error||'')}</td>
        <td>${workerActionsHtml(w,j.job_id)}</td></tr>`).join('')
        : `<tr><td colspan="8" class="muted">${j.in_memory === false
            ? '这是 Manager 重启前的历史记录，没有可操作的在线 Worker。'
            : 'Worker 尚未创建或状态尚未上报。'}</td></tr>`}
      </tbody></table></div>
    </div>
  </details>`;
}
function jobRenderSignature(job, result) {
  const jobId = String(job.job_id);
  const cached = jobResultsCache.get(jobId);
  const resultUiState = [
    Boolean(cached?.loading),
    Number(cached?.errorStatus) || Boolean(cached?.error),
    resultDownloadsInFlight.has(jobId),
  ];
  return JSON.stringify([job, result || null, resultUiState]);
}
function makeJobNode(job) {
  const template = document.createElement('template');
  const result = resultFor(job.job_id);
  template.innerHTML = jobRowHtml(job, result).trim();
  const node = template.content.firstElementChild;
  node._renderSignature = jobRenderSignature(job, result);
  return node;
}
function jobFocusedControl(node) {
  const active = document.activeElement;
  return active && node.contains(active) ? (active.dataset.jobFocus || '') : '';
}
function focusedOtpState(node) {
  const active = document.activeElement;
  if (!active || !node.contains(active) || !active.classList.contains('otp-code')) {
    return null;
  }
  const card = active.closest('.otp-challenge-card');
  return card ? {
    key: card.dataset.otpKey,
    selectionStart: active.selectionStart,
    selectionEnd: active.selectionEnd,
  } : null;
}
function restoreJobFocus(node, focusKey) {
  if (!focusKey) return;
  const control = Array.from(node.querySelectorAll('[data-job-focus]'))
    .find(element => element.dataset.jobFocus === focusKey);
  if (control) control.focus({preventScroll:true});
}
function restoreOtpFocus(node, state) {
  if (!state) return;
  const card = otpCardsByKey.get(state.key);
  const input = card?.querySelector('.otp-code');
  if (!input || !node.contains(input)) return;
  input.focus({preventScroll:true});
  if (state.selectionStart !== null && state.selectionEnd !== null) {
    input.setSelectionRange(state.selectionStart, state.selectionEnd);
  }
}
function reconcileJobCards(jobs) {
  const list = document.getElementById('jobsList');
  if (!jobs.length) {
    if (list.dataset.empty !== 'true') {
      list.textContent = '';
      const empty = document.createElement('p');
      empty.className = 'muted'; empty.textContent = 'No jobs yet.';
      list.appendChild(empty); list.dataset.empty = 'true';
    }
    reconcileLoginAttempts(latestLoginAttempts);
    return;
  }
  delete list.dataset.empty;
  Array.from(list.children).forEach(node => {
    if (!node.classList.contains('job-row')) node.remove();
  });
  const wanted = new Set(jobs.map(job => String(job.job_id)));
  Array.from(list.querySelectorAll('.job-row')).forEach(node => {
    if (!wanted.has(node.dataset.jobId)) node.remove();
  });
  const viewportX = window.scrollX;
  const viewportY = window.scrollY;
  let replacedAny = false;
  let otpFocusTarget = null;
  jobs.forEach((job, index) => {
    const id = String(job.job_id);
    const result = resultFor(id);
    const signature = jobRenderSignature(job, result);
    let node = document.getElementById('jobrow-' + id);
    if (!node) {
      node = makeJobNode(job);
    } else if (node._renderSignature !== signature) {
      const wasOpen = node.open;
      const focusedControl = jobFocusedControl(node);
      const otpFocus = focusedOtpState(node);
      const otpCards = Array.from(
        node.querySelectorAll('.otp-challenge-card'),
      );
      const scrollLefts = Array.from(node.querySelectorAll('.table-scroll'))
        .map(element => element.scrollLeft);
      const replacement = makeJobNode(job);
      replacement.open = wasOpen;
      const otpMount = replacement.querySelector('.job-otp-list');
      otpCards.forEach(card => otpMount.appendChild(card));
      node.replaceWith(replacement);
      const replacementScrolls = replacement.querySelectorAll('.table-scroll');
      scrollLefts.forEach((scrollLeft, index) => {
        if (replacementScrolls[index]) {
          replacementScrolls[index].scrollLeft = scrollLeft;
        }
      });
      restoreJobFocus(replacement, focusedControl);
      if (otpFocus) otpFocusTarget = {node:replacement, state:otpFocus};
      replacedAny = true;
      node = replacement;
    }
    const current = list.children[index] || null;
    if (node !== current) list.insertBefore(node, current);
  });
  reconcileLoginAttempts(latestLoginAttempts);
  if (otpFocusTarget) {
    restoreOtpFocus(otpFocusTarget.node, otpFocusTarget.state);
  }
  if (replacedAny) window.scrollTo(viewportX, viewportY);
}
function nextResultCheck(job, incomingFileCount, previous) {
  if (incomingFileCount > 0) {
    return job.done ? Number.POSITIVE_INFINITY : Date.now() + 30_000;
  }
  const misses = Math.min(6, Number(previous?.misses || 0) + 1);
  const base = job.done ? 15_000 : 5_000;
  const ceiling = job.done ? 300_000 : 30_000;
  return Date.now() + Math.min(ceiling, base * (2 ** (misses - 1)));
}
function commitJobResult(job, value, requestVersion) {
  const jobId = String(job.job_id);
  if (requestVersion !== jobResultsRequestVersions.get(jobId)) return false;
  const previous = jobResultsCache.get(jobId) || {};
  const knownFileCount = resultFileCount(previous.value);
  const incomingFileCount = resultFileCount(value);
  const preserveKnown = knownFileCount > 0 && incomingFileCount <= 0;
  let nextCheck = nextResultCheck(job, incomingFileCount, previous);
  if (preserveKnown && job.done) nextCheck = Number.POSITIVE_INFINITY;
  jobResultsCache.set(jobId, {
    value: preserveKnown ? previous.value : value,
    nextCheck,
    misses: incomingFileCount > 0 ? 0 : Number(previous.misses || 0) + 1,
    loading: false,
    error: null,
    errorStatus: null,
  });
  return true;
}
function commitJobResultError(job, error, requestVersion) {
  const jobId = String(job.job_id);
  if (requestVersion !== jobResultsRequestVersions.get(jobId)) return false;
  const previous = jobResultsCache.get(jobId) || {};
  const knownFileCount = resultFileCount(previous.value);
  jobResultsCache.set(jobId, {
    ...previous,
    nextCheck: knownFileCount > 0 && job.done
      ? Number.POSITIVE_INFINITY
      : nextResultCheck(job, 0, previous),
    misses: Number(previous.misses || 0) + 1,
    loading: false,
    error: error.message || String(error),
    errorStatus: Number(error.status) || null,
  });
  return true;
}
async function refreshJobResults(jobs, force=false) {
  const now = Date.now();
  const candidates = jobs.filter(job => {
    const cached = jobResultsCache.get(job.job_id);
    return force || !cached
      || (!cached.loading && now >= Number(cached.nextCheck || 0));
  }).slice(0, 30);
  await mapLimit(candidates, 3, async job => {
    const jobId = String(job.job_id);
    const requestVersion = Number(jobResultsRequestVersions.get(jobId) || 0) + 1;
    jobResultsRequestVersions.set(jobId, requestVersion);
    jobResultsCache.set(jobId, {
      ...(jobResultsCache.get(jobId) || {}),
      loading: true,
    });
    try {
      const value = await api('GET', '/jobs/' + encodeURIComponent(jobId) + '/results');
      commitJobResult(job, value, requestVersion);
    } catch(error) {
      commitJobResultError(job, error, requestVersion);
    }
  });
  reconcileJobCards(visibleJobs(latestJobs));
  refreshResults();
}
let jobsRequestRunning = false;
async function refreshJobs() {
  if (jobsRequestRunning) return;
  jobsRequestRunning = true;
  try {
    const data = await api('GET', '/jobs');
    latestJobs = data.jobs || [];
    const jobs = visibleJobs(latestJobs);
    reconcileJobCards(jobs);
    document.getElementById('jobsRefresh').textContent =
      '· 已更新 ' + new Date().toLocaleTimeString();
    await refreshJobResults(jobs);
  } catch (error) {
    document.getElementById('jobsRefresh').textContent =
      '· 刷新失败，保留当前快照 · ' + new Date().toLocaleTimeString();
  } finally {
    jobsRequestRunning = false;
  }
}

var _logWid = '';
var _logJobId = '';
var _logMode = 'job';
var _logTimer = null;
var _logPaused = false;
var _logFollowing = true;
var _logLoading = false;
var _logContextVersion = 0;
var _logLastText = '';
function updateLogControls() {
  document.getElementById('logPauseBtn').textContent =
    _logPaused ? '继续刷新' : '暂停刷新';
  document.getElementById('logFollowBtn').textContent =
    (_logFollowing ? '✓ ' : '') + '跟随最新';
}
function scheduleLogRefresh() {
  clearTimeout(_logTimer);
  if (!document.hidden && !_logPaused
      && document.getElementById('logModal').style.display === 'flex') {
    _logTimer = setTimeout(() => refreshOpenLogs(false), 3_000);
  }
}
function showJobLogs(jobId, wid='') {
  if (!jobId) return;
  const changed = _logMode !== 'job' || _logJobId !== jobId || _logWid !== wid;
  if (changed) _logContextVersion += 1;
  _logMode = 'job'; _logJobId = jobId; _logWid = wid;
  _logPaused = false; _logFollowing = true;
  document.getElementById('logModal').style.display = 'flex';
  document.getElementById('logTitle').textContent =
    '任务输出 · ' + jobId + (wid ? ' · ' + wid : '');
  if (changed) {
    document.getElementById('logContent').textContent = '加载中…';
    document.getElementById('logMeta').textContent = '';
  }
  updateLogControls();
  refreshOpenLogs(true);
}
function showWorkerLogs(wid) {
  if (!wid) return;
  const changed = _logMode !== 'worker' || _logWid !== wid;
  if (changed) _logContextVersion += 1;
  _logMode = 'worker'; _logWid = wid; _logJobId = '';
  _logPaused = false; _logFollowing = true;
  document.getElementById('logModal').style.display = 'flex';
  document.getElementById('logTitle').textContent = 'Worker 系统日志 · ' + wid;
  if (changed) {
    document.getElementById('logContent').textContent = '加载中…';
    document.getElementById('logMeta').textContent = '';
  }
  updateLogControls();
  refreshOpenLogs(true);
}
function showLogs(wid) { showWorkerLogs(wid); }
function formatTaskExitSummary(tasks) {
  return tasks.map(task => {
    const parts = [];
    const taskId = String(task.task_id || '');
    if (taskId) parts.push('执行 ' + taskId.split(':').slice(-1)[0]);
    if (task.exit_code !== null && task.exit_code !== undefined) {
      parts.push('退出码 ' + Number(task.exit_code));
    }
    if (task.error_type) parts.push(String(task.error_type));
    if (task.error_message) {
      parts.push(String(task.error_message).replace(/\\s+/g, ' ').slice(0, 240));
    }
    return parts.join(' · ');
  }).filter(Boolean).join(' | ');
}
function formatJobLog(data) {
  const exitSummary = formatTaskExitSummary(data.tasks || []);
  const output = !(data.entries || []).length
    ? (data.message || '(暂无命令输出)')
    : data.entries.map(entry => {
    const parsed = entry.timestamp ? new Date(entry.timestamp) : null;
    const timestamp = parsed && !Number.isNaN(parsed.getTime())
      ? parsed.toLocaleTimeString() : '--:--:--';
    return `[${timestamp}] ${String(entry.stream || 'stdout').padEnd(6)} | ${entry.data || ''}`;
  }).join('\\n');
  return exitSummary ? exitSummary + '\\n\\n' + output : output;
}
function jobLogLineLimit(jobId, workerId) {
  const job = latestJobs.find(item => String(item.job_id) === String(jobId));
  const workers = job?.workers_detail || [];
  const worker = workerId
    ? workers.find(item => String(item.worker_id) === String(workerId))
    : null;
  const terminal = workerId
    ? Boolean(worker && workerExecutionTerminal(worker))
    : Boolean(job && (job.done
      || (workers.length && workers.every(workerExecutionTerminal))));
  return terminal ? 5_000 : 1_000;
}
async function refreshOpenLogs(force=false) {
  if (_logLoading || (_logPaused && !force)) return;
  _logLoading = true;
  const contextVersion = _logContextVersion;
  const mode = _logMode;
  const jobId = _logJobId;
  const workerId = _logWid;
  const pre = document.getElementById('logContent');
  const distance = pre.scrollHeight - pre.scrollTop - pre.clientHeight;
  const wasNearBottom = distance < 70;
  try {
    if (mode === 'job') {
      const lineLimit = jobLogLineLimit(jobId, workerId);
      let path = '/jobs/' + encodeURIComponent(jobId) + '/logs?lines=' + lineLimit;
      if (workerId) path += '&worker_id=' + encodeURIComponent(workerId);
      const data = await api('GET', path);
      if (contextVersion !== _logContextVersion) return;
      _logLastText = formatJobLog(data);
      pre.textContent = _logLastText;
      const source = data.source === 'archive' ? '已归档'
        : data.source === 'live' ? '实时缓冲'
        : data.source === 'mixed' ? '实时 + 归档' : '无日志';
      const exitSummary = formatTaskExitSummary(data.tasks || []);
      document.getElementById('logMeta').textContent =
        `${source} · 显示 ${data.returned || 0}/${data.total || 0} 行`
        + (data.truncated ? ' · 较早内容已裁剪' : '')
        + (exitSummary ? ' · ' + exitSummary : '')
        + (data.message ? ' · ' + data.message : '');
      if (['live','pending'].includes(data.status)) scheduleLogRefresh();
      else clearTimeout(_logTimer);
    } else {
      const data = await api(
        'GET', '/nodes/' + encodeURIComponent(workerId) + '/logs?lines=800',
      );
      if (contextVersion !== _logContextVersion) return;
      _logLastText = data.logs || '(系统日志为空)';
      pre.textContent = _logLastText;
      document.getElementById('logMeta').textContent =
        '临时 Worker 的 systemd journal；Worker 销毁后请改看任务输出。';
      scheduleLogRefresh();
    }
    if (_logFollowing && (wasNearBottom || force)) pre.scrollTop = pre.scrollHeight;
  } catch(error) {
    if (contextVersion !== _logContextVersion) return;
    const workerGone = _logMode === 'worker'
      && [404, 409].includes(Number(error.status));
    if (workerGone) {
      _logPaused = true;
      updateLogControls();
    }
    document.getElementById('logMeta').textContent =
      '刷新失败：' + (error.message || error)
      + (_logMode === 'worker'
        ? '；Worker 可能已销毁，请返回 Job 卡片查看任务输出。' : '');
    if (workerGone) {
      clearTimeout(_logTimer);
      _logTimer = null;
    } else scheduleLogRefresh();
  } finally {
    _logLoading = false;
    if (contextVersion !== _logContextVersion && (_logJobId || _logWid)) {
      refreshOpenLogs(true);
    }
  }
}
function closeLogs() {
  _logContextVersion += 1;
  clearTimeout(_logTimer); _logTimer = null;
  document.getElementById('logModal').style.display = 'none';
  _logWid = ''; _logJobId = ''; _logLastText = '';
}
function toggleLogPause() {
  _logPaused = !_logPaused; updateLogControls();
  if (_logPaused) clearTimeout(_logTimer); else refreshOpenLogs(true);
}
function toggleLogFollow() {
  _logFollowing = !_logFollowing; updateLogControls();
  if (_logFollowing) {
    const pre = document.getElementById('logContent');
    pre.scrollTop = pre.scrollHeight;
  }
}
async function copyLogs() {
  try {
    await navigator.clipboard.writeText(
      _logLastText || document.getElementById('logContent').textContent,
    );
    toast('日志已复制');
  } catch(error) { toast('复制失败：' + error.message, 'error'); }
}
function downloadLogText() {
  const blob = new Blob([
    _logLastText || document.getElementById('logContent').textContent,
  ], {type:'text/plain;charset=utf-8'});
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = (_logJobId || _logWid || 'elastic-agent') + '-logs.txt';
  document.body.appendChild(link); link.click(); link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
function refreshResults() {
  const list = document.getElementById('resultsList');
  const jobs = sortedJobs(latestJobs).map(job => resultFor(job.job_id))
    .filter(result => result && (result.file_count || result.s3_uri));
  const signature = JSON.stringify([
    jobs,
    jobs.map(result => resultDownloadsInFlight.has(String(result.job_id))),
  ]);
  if (list.dataset.signature === signature) return;
  list.dataset.signature = signature;
  if (!jobs.length) {
    list.innerHTML = '<p class="muted">暂无已收集结果；启用周期收集后运行中也会显示，否则会在 Job 结束后显示。</p>';
    return;
  }
  list.innerHTML = jobs.map(result => {
    const scoreStr = (result.scores && result.scores.length)
      ? result.scores.map(score => `${esc(score.task_id)} ${esc(score.prompt_level)}: <b>${Number(score.final_score||0).toFixed(1)}</b>`).join(' · ')
      : '';
    const jobId = String(result.job_id);
    const downloadState = resultDownloadsInFlight.get(jobId);
    const matchingJob = latestJobs.find(job => String(job.job_id) === jobId);
    const snapshotLabel = matchingJob && !matchingJob.done
      ? '<div class="muted" style="font-size:.72rem">当前为运行中已上传的中间结果快照</div>'
      : '';
    const downloadLabel = downloadState
      ? formatResultDownloadLabel(downloadState) : '⬇ 下载全部';
    const action = downloadState
      ? resultDownloadCancellable(downloadState)
        ? `onclick="cancelResultDownload(${jsArg(jobId)})"`
        : 'disabled aria-disabled="true"'
      : `onclick="downloadResults(${jsArg(jobId)})"`;
    return `<div style="display:flex;justify-content:space-between;align-items:center;gap:8px;
        border:1px solid var(--border);border-radius:8px;padding:8px 10px;margin-bottom:8px">
      <div><b>${esc(result.job_id)}</b> <span class="muted">(${Number(result.file_count)||0} 文件)</span>
        ${scoreStr ? '<div class="muted" style="margin-top:2px">📊 '+scoreStr+'</div>' : ''}
        ${result.s3_uri ? '<div class="muted" style="font-size:.72rem">S3: '+esc(result.s3_uri)+'</div>' : ''}
        ${snapshotLabel}</div>
      <button class="${downloadState ? 'btn btn-danger' : 'btn'}"
        data-result-download-job="${esc(jobId)}"
        style="margin:0" ${action}>${downloadLabel}</button>
    </div>`;
  }).join('');
}
async function refreshVisibleResults(force=false) {
  const jobs = visibleJobs(latestJobs);
  if (force) jobs.forEach(job => {
    const cached = jobResultsCache.get(job.job_id);
    if (cached) cached.nextCheck = 0;
  });
  await refreshJobResults(jobs, force);
}
function scheduleDashboardPoll(delay=5_000) {
  clearTimeout(dashboardPollTimer);
  dashboardPollTimer = setTimeout(runDashboardPoll, delay);
}
async function runDashboardPoll() {
  if (dashboardPollRunning) return;
  if (document.hidden) { scheduleDashboardPoll(5_000); return; }
  dashboardPollRunning = true;
  try {
    await Promise.allSettled([refreshJobs(), refreshLoginAttempts()]);
  } finally {
    dashboardPollRunning = false;
    scheduleDashboardPoll(5_000);
  }
}

document.getElementById('logContent').addEventListener('scroll', () => {
  const pre = document.getElementById('logContent');
  if (_logFollowing && pre.scrollHeight - pre.scrollTop - pre.clientHeight > 90) {
    _logFollowing = false; updateLogControls();
  }
});
document.addEventListener('visibilitychange', () => {
  if (document.hidden) clearTimeout(_logTimer);
  else {
    scheduleDashboardPoll(0);
    if (_logJobId || _logWid) scheduleLogRefresh();
  }
});
updateThemeLabel();
updateEipBindingUI(); updateAgentUI();
providerDefaultsReady = initializeProviderDefaults();
refreshAccounts(); refreshResults(); runDashboardPoll();
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
