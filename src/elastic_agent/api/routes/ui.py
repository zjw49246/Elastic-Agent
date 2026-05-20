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
const API_KEY = new URLSearchParams(window.location.search).get('api_key') || '';
const headers = API_KEY ? {'Authorization': `Bearer ${API_KEY}`, 'Content-Type': 'application/json'}
                        : {'Content-Type': 'application/json'};

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


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def dashboard():
    """Serve the Elastic-Agent dashboard UI."""
    return HTMLResponse(content=_DASHBOARD_HTML)


@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_alt():
    """Alias for the dashboard."""
    return HTMLResponse(content=_DASHBOARD_HTML)
