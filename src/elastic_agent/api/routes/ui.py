"""Basic Web UI for Elastic-Agent Manager.

T-029: Self-contained single-page dashboard served as inline HTML.
Shows node list, status cards, and supports manual operations
(scale out, scale in, drain, remove).
"""

from __future__ import annotations

import re
from html import escape
from urllib.parse import parse_qs, urlencode

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import ValidationError

from elastic_agent.api.auth import get_session_principal, require_same_origin
from elastic_agent.api.routes.management_auth import (
    LoginRequest,
    create_browser_session,
)

router = APIRouter(tags=["ui"])

_UI_SECURITY_HEADERS = {
    "Cache-Control": "no-store",
    "Content-Security-Policy": (
        "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
        "connect-src 'self'; img-src 'self' data:; object-src 'none'; "
        "base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
    ),
    "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
}

_DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="zh-CN" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Elastic-Agent Dashboard</title>
<script>
  try { sessionStorage.removeItem('ea_api_key'); } catch (_) {}
  { const legacyUrl = new URL(window.location.href);
    if (legacyUrl.searchParams.has('api_key')) {
      legacyUrl.searchParams.delete('api_key');
      history.replaceState(null, '', legacyUrl.pathname + legacyUrl.search + legacyUrl.hash);
    }
  }
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
      <span id="currentUserEmail" style="margin-right:8px">--</span>
      <a href="#" id="fleetLogout" onclick="logout();return false"
        style="color:var(--accent);text-decoration:none;margin-right:12px">退出登录</a>
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
{const nb = document.getElementById('navBatch'); if (nb) nb.href = '/';}

let refreshTimer = null;
let nodesRefreshRunning = false;

const AUTHENTICATED_UI_PATHS = new Set(['/', '/batch', '/fleet', '/dashboard']);
let csrfToken = '';
function safeCurrentUiPath() {
  return AUTHENTICATED_UI_PATHS.has(window.location.pathname)
    ? window.location.pathname : '/';
}
function redirectToLogin() {
  const next = encodeURIComponent(safeCurrentUiPath());
  window.location.assign('/login?next=' + next);
}
async function initializeAuthentication() {
  const response = await fetch('/api/auth/me', {
    credentials:'same-origin', headers:{'Accept':'application/json'},
  });
  if (response.status === 401) {
    redirectToLogin();
    throw new Error('登录已失效');
  }
  if (!response.ok) throw new Error(`${response.status}: ${await response.text()}`);
  const session = await response.json();
  if (session.must_change_password === true) {
    window.location.assign(
      '/change-password?next=' + encodeURIComponent(safeCurrentUiPath())
    );
    throw new Error('需要先修改初始密码');
  }
  csrfToken = String(session.csrf_token || '');
  document.getElementById('currentUserEmail').textContent = session.email || '';
  return session;
}
const authenticationReady = initializeAuthentication();
async function authenticatedFetch(input, init={}) {
  await authenticationReady;
  const options = {...init, credentials:'same-origin'};
  const method = String(options.method || 'GET').toUpperCase();
  const requestHeaders = new Headers(options.headers || {});
  if (!['GET', 'HEAD', 'OPTIONS'].includes(method)) {
    requestHeaders.set('X-CSRF-Token', csrfToken);
  }
  options.headers = requestHeaders;
  const response = await fetch(input, options);
  if (response.status === 401) redirectToLogin();
  return response;
}
async function logout() {
  try {
    const response = await authenticatedFetch('/api/auth/logout', {method:'POST'});
    if (!response.ok) throw new Error(`${response.status}: ${await response.text()}`);
    window.location.assign('/login');
  } catch (error) {
    toast('退出失败：' + error.message, 'error');
  }
}

async function api(method, path, body) {
  const requestHeaders = new Headers({'Accept':'application/json'});
  const opts = {method, headers:requestHeaders};
  if (body !== undefined && body !== null) {
    requestHeaders.set('Content-Type', 'application/json');
    opts.body = JSON.stringify(body);
  }
  const resp = await authenticatedFetch('/api' + path, opts);
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
  try { sessionStorage.removeItem('ea_api_key'); } catch (_) {}
  { const legacyUrl = new URL(window.location.href);
    if (legacyUrl.searchParams.has('api_key')) {
      legacyUrl.searchParams.delete('api_key');
      history.replaceState(null, '', legacyUrl.pathname + legacyUrl.search + legacyUrl.hash);
    }
  }
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
  label { display:block; font-size:.84rem; color:var(--text); font-weight:600;
    margin:8px 0 5px; line-height:1.35; }
  input, select, textarea { width:100%; background:var(--surface-soft); color:var(--text);
    border:1px solid var(--border); border-radius:7px; padding:8px 10px; font-size:.88rem;
    font-family:inherit; min-height:40px; }
  input:focus, select:focus, textarea:focus { outline:2px solid color-mix(in srgb,var(--accent) 28%,transparent);
    border-color:var(--accent); }
  input:disabled, select:disabled, textarea:disabled { opacity:.68; cursor:not-allowed;
    background:color-mix(in srgb,var(--surface-soft) 72%,var(--border)); }
  input[type="checkbox"] { width:auto; min-height:auto; accent-color:var(--accent); }
  textarea { resize:vertical; min-height:52px; font-family:ui-monospace,Menlo,monospace; }
  .grid2 { display:grid; grid-template-columns:minmax(0,1fr) minmax(0,1fr); gap:10px; }
  .grid3 { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; }
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
  .b-recovered, .b-interrupted, .b-suspending {
    background:rgba(100,116,139,.14); color:var(--muted);
  }
  .b-succeeded { background:rgba(34,197,94,.16); color:var(--green); }
  .b-suspended { background:rgba(249,115,22,.18); color:var(--orange); }
  .b-cancelled { background:rgba(100,116,139,.14); color:var(--muted); }
  .muted { color:var(--muted); font-size:.8rem; }
  .toast { position:fixed; bottom:20px; right:20px; background:var(--surface);
    border:1px solid var(--border); border-radius:8px; padding:12px 18px; opacity:0;
    transition:opacity .3s; pointer-events:none; z-index:1200; box-shadow:var(--shadow); }
  .toast.show { opacity:1; } .toast.error { border-color:var(--red); }
  details { margin-top:6px; } summary { cursor:pointer; }
  .hint { font-size:.78rem; color:var(--muted); margin-top:3px; line-height:1.45; }
  code { background:var(--surface-soft); border:1px solid var(--border); border-radius:4px;
    padding:1px 4px; }
  .account-editor { border-top:1px solid var(--border); margin-top:18px; padding-top:16px; }
  .account-editor h3 { font-size:.95rem; margin-bottom:5px; }
  .check-label { display:flex; gap:7px; align-items:flex-start; font-weight:400;
    color:var(--muted); margin-top:7px; }
  .check-label input { margin-top:2px; flex:none; }
  .job-form { display:grid; gap:14px; }
  .form-section { min-width:0; border:1px solid var(--border); border-radius:10px;
    padding:10px 14px 14px; background:var(--surface-soft); }
  .form-section > legend { padding:0 8px; color:var(--text); font-size:.94rem;
    font-weight:700; letter-spacing:-.01em; }
  .section-intro { color:var(--muted); font-size:.79rem; line-height:1.45;
    margin:0 0 10px; }
  .form-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(235px,1fr));
    gap:10px 12px; }
  .form-grid + .form-grid, .form-grid + .field, .field + .form-grid { margin-top:9px; }
  .field { min-width:0; }
  .field > label { margin-top:0; }
  .field-span-full { grid-column:1 / -1; }
  .field-help { color:var(--muted); font-size:.78rem; line-height:1.45; margin-top:5px; }
  .field-help[data-state] { min-height:1.15em; }
  .field-code { color:var(--muted); font-size:.72rem; font-weight:400; white-space:nowrap; }
  .required-mark { color:var(--red); font-size:.75rem; margin-left:4px; }
  .textarea-setup { min-height:118px; }
  .textarea-command { min-height:108px; }
  .textarea-compact { min-height:78px; }
  .form-details { border-top:1px dashed var(--border); margin-top:12px; padding-top:9px; }
  .form-details > summary { color:var(--accent); font-size:.8rem; font-weight:600;
    width:max-content; max-width:100%; }
  .form-details[open] > summary { margin-bottom:10px; }
  .form-notice { border:1px solid color-mix(in srgb,var(--accent) 28%,var(--border));
    background:color-mix(in srgb,var(--accent) 5%,var(--surface)); border-radius:7px;
    padding:8px 10px; margin-top:10px; }
  .form-notice.warning { border-color:color-mix(in srgb,var(--orange) 42%,var(--border));
    background:color-mix(in srgb,var(--orange) 7%,var(--surface)); color:var(--orange); }
  .form-actions { position:sticky; bottom:8px; z-index:20; display:flex;
    justify-content:flex-end; align-items:center; gap:8px; padding:10px;
    border:1px solid var(--border); border-radius:10px;
    background:color-mix(in srgb,var(--surface) 94%,transparent);
    box-shadow:0 8px 24px rgba(15,23,42,.11); backdrop-filter:blur(8px); }
  .form-actions .btn { margin:0; min-height:40px; }
  .submission-tabs { display:flex; gap:8px; margin:0 0 12px; padding:5px;
    width:max-content; max-width:100%; border:1px solid var(--border); border-radius:10px;
    background:var(--surface); box-shadow:var(--shadow); }
  .submission-tab { margin:0; background:transparent; color:var(--muted);
    border:1px solid transparent; }
  .submission-tab[aria-selected="true"] { color:#fff; background:var(--accent);
    border-color:var(--accent); }
  .batch-upload-row { display:grid; grid-template-columns:minmax(0,1fr) auto;
    gap:10px; align-items:end; }
  .batch-upload-row .btn { margin:0; min-height:40px; }
  .batch-file-meta { margin-top:8px; overflow-wrap:anywhere; }
  .batch-privacy-note { border:1px solid color-mix(in srgb,var(--green) 32%,var(--border));
    background:color-mix(in srgb,var(--green) 6%,var(--surface)); border-radius:8px;
    padding:9px 11px; margin:10px 0; color:var(--muted); font-size:.79rem;
    line-height:1.5; }
  .batch-alert { display:block; border:1px solid var(--border); border-radius:8px;
    padding:9px 11px; margin-top:10px; white-space:pre-wrap; overflow-wrap:anywhere;
    font-size:.8rem; line-height:1.45; }
  .batch-alert-error { color:var(--red);
    border-color:color-mix(in srgb,var(--red) 38%,var(--border));
    background:color-mix(in srgb,var(--red) 7%,var(--surface)); }
  .batch-alert-warning { color:var(--orange);
    border-color:color-mix(in srgb,var(--orange) 42%,var(--border));
    background:color-mix(in srgb,var(--orange) 7%,var(--surface)); }
  .batch-alert-success { color:var(--green);
    border-color:color-mix(in srgb,var(--green) 38%,var(--border));
    background:color-mix(in srgb,var(--green) 7%,var(--surface)); }
  .batch-summary-grid { display:grid; grid-template-columns:repeat(5,minmax(0,1fr));
    gap:8px; margin-top:12px; }
  .batch-summary-stat { min-width:0; border:1px solid var(--border); border-radius:8px;
    padding:9px; background:var(--surface-soft); }
  .batch-summary-stat b { display:block; margin-top:3px; overflow-wrap:anywhere;
    font-size:.95rem; font-variant-numeric:tabular-nums; }
  .batch-instance-list { display:flex; flex-wrap:wrap; gap:6px; list-style:none;
    margin:9px 0 0; }
  .batch-instance-list li { border:1px solid var(--border); border-radius:999px;
    padding:3px 8px; background:var(--surface-soft); font-size:.76rem; }
  .batch-plan-items,.batch-receipt-items { display:grid; gap:8px; margin-top:10px; }
  .batch-item { border:1px solid var(--border); border-radius:9px; padding:10px;
    background:var(--surface-soft); min-width:0; }
  .batch-item-head { display:flex; justify-content:space-between; align-items:flex-start;
    gap:10px; }
  .batch-item-title { min-width:0; overflow-wrap:anywhere; }
  .batch-item-messages { margin:6px 0 0 19px; font-size:.78rem; line-height:1.45; }
  .batch-item-messages li { margin-top:3px; overflow-wrap:anywhere; }
  .batch-message-error { color:var(--red); }
  .batch-message-warning { color:var(--orange); }
  .batch-confirm-actions { display:flex; justify-content:flex-end; gap:8px;
    align-items:center; margin-top:12px; }
  .batch-confirm-actions .btn { margin:0; min-height:42px; }
  .batch-receipt-head { display:flex; justify-content:space-between; align-items:flex-start;
    gap:10px; margin-top:12px; }
  .batch-receipt-identifiers { overflow-wrap:anywhere; }
  [hidden] { display:none !important; }
  .plan-result { white-space:pre-wrap; background:var(--bg); border:1px solid var(--border);
    border-radius:7px; padding:10px; margin-top:0; font-size:.75rem; max-height:420px;
    overflow:auto; }
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
  .job-config { margin-top:12px; border:1px solid var(--border); border-radius:9px;
    background:var(--surface-soft); overflow:hidden; }
  .job-config > summary { display:flex; justify-content:space-between; align-items:center;
    gap:10px; padding:9px 11px; list-style:none; color:var(--accent);
    font-size:.82rem; font-weight:700; }
  .job-config > summary::-webkit-details-marker { display:none; }
  .job-config > summary::after { content:'查看 JSON ▾'; color:var(--muted);
    font-size:.72rem; font-weight:500; white-space:nowrap; }
  .job-config[open] > summary { border-bottom:1px solid var(--border);
    background:var(--surface); }
  .job-config[open] > summary::after { content:'收起 JSON ▴'; }
  .job-config-body { padding:10px; }
  .job-config-toolbar { display:flex; justify-content:space-between; align-items:flex-start;
    gap:10px; margin-bottom:8px; }
  .job-config-toolbar .btn,.job-config-message .btn { margin:0; padding:5px 9px; }
  .job-config-note { color:var(--muted); font-size:.74rem; line-height:1.45; }
  .job-config-message { display:flex; justify-content:space-between; align-items:center;
    gap:10px; min-height:38px; color:var(--muted); font-size:.78rem; }
  .job-config-json { max-height:420px; overflow:auto; white-space:pre; tab-size:2;
    margin:0; padding:11px; border:1px solid var(--border); border-radius:7px;
    background:var(--terminal); color:var(--terminal-text);
    font:12px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; }
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
  @media (max-width:960px) {
    .form-grid { grid-template-columns:repeat(2,minmax(0,1fr)); }
  }
  @media (max-width:800px) {
    .grid2,.grid3 { grid-template-columns:1fr; }
    .workflow { grid-template-columns:repeat(2,1fr); }
    .container { padding:12px; }
    header,.job-head,.job-summary { align-items:flex-start; flex-direction:column; }
    .job-actions { justify-content:flex-start; }
    .job-config-toolbar,.job-config-message { align-items:flex-start; flex-direction:column; }
    .log-dialog { width:96%; height:90vh; }
    .otp-action-card { right:10px; bottom:10px; width:calc(100vw - 20px);
      max-height:52vh; }
    .otp-action-head,.job-otp-region-head { align-items:flex-start;
      flex-direction:column; }
    .otp-action-card.otp-minimized .otp-action-head { align-items:center;
      flex-direction:row; }
    .otp-controls { grid-template-columns:1fr; }
    .batch-summary-grid { grid-template-columns:repeat(2,minmax(0,1fr)); }
    .batch-item-head,.batch-receipt-head { flex-direction:column; }
  }
  @media (max-width:620px) {
    input,select,textarea { min-height:44px; font-size:16px; }
    input[type="checkbox"] { min-height:auto; }
    .form-grid { grid-template-columns:1fr; }
    .field-span-full { grid-column:auto; }
    .form-section { padding:8px 10px 12px; }
    .form-actions { position:static; display:grid; grid-template-columns:1fr; }
    .form-actions .btn { width:100%; min-height:44px; }
    .submission-tabs { display:grid; width:100%; }
    .batch-upload-row { grid-template-columns:1fr; }
    .batch-upload-row .btn,.batch-confirm-actions .btn { width:100%; }
    .batch-confirm-actions { display:grid; }
    .batch-summary-grid { grid-template-columns:1fr; }
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
      <span id="currentUserEmail" class="muted">--</span>
      &nbsp;·&nbsp;
      <a href="#" onclick="logout();return false" style="color:var(--muted)">退出登录</a>
    </div>
  </header>

  <div class="card">
    <h2>Job 怎么运行</h2>
    <p class="muted">填写代码与命令后可先点「仅校验并查看计划」；「校验并启动 Job」也会先执行相同预检。日志用于排错，只有明确填写的结果目录才会被收集；Job 结束后临时 Worker 会自动销毁。</p>
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
    <div style="display:flex;justify-content:space-between;align-items:center;gap:8px">
      <h2>Accounts <span class="muted" id="accountsRefresh"
                         role="status" aria-live="polite"></span></h2>
      <button class="btn btn-ghost" style="margin:0"
              onclick="refreshAccounts(true)">刷新账号状态</button>
    </div>
    <p class="hint">
      Manager 会把登录密码与接码查询 token 保存到权限 0600 的账号文件，提交后均不回显。
      Claude/Codex OAuth 凭证只在 worker 生成且不回传。Codex 至少配置 OpenAI 密码或接码查询 Token
      之一，也可同时配置。查询 Token 不是 OpenAI 登录凭据，只用于从接码平台读取 OpenAI 发出的邮箱验证码；
      仅有 Token 时会切换到邮箱验证码并自动取码。没有可用查询 Token、自动查询失败，或自动验证码被拒绝时，
      只有对应 Worker 才会弹出人工验证码卡。
    </p>
    <div class="table-scroll">
      <table><thead><tr><th>ID</th><th>类型 / 支持 Agent</th><th>账号</th><th>Secrets</th>
        <th>Group</th><th>Enabled</th><th>额度</th><th>EIP / 当前 Worker</th><th></th></tr></thead>
        <tbody id="acctRows"></tbody></table>
    </div>
    <section class="account-editor" aria-labelledby="nativeAccountTitle">
      <h3 id="nativeAccountTitle">添加登录账号</h3>
      <p class="hint">Claude 或 Codex 在 Worker 上完成浏览器登录；秘密写入后不会在页面回显。</p>
    <div class="grid3" style="margin-top:8px">
      <div><label for="acctId">账号 ID</label><input id="acctId" maxlength="128" placeholder="acc-1"></div>
      <div><label for="acctEmail">登录邮箱</label><input id="acctEmail" type="email" maxlength="320" placeholder="a@x.com"></div>
      <div><label for="acctAgent">使用的 Agent</label><select id="acctAgent">
        <option value="claude">Claude</option><option value="codex">Codex</option>
      </select></div>
    </div>
    <div class="grid3">
      <div><label for="acctPassword">登录密码</label>
        <input id="acctPassword" type="password" autocomplete="new-password" maxlength="16384"
               placeholder="OpenAI password">
        <div class="field-help">Codex 至少填写一项，可同时填写：登录密码或接码查询 Token。</div>
        <label class="check-label" for="acctClearPassword"><input id="acctClearPassword" type="checkbox">
          清除该账号已有登录密码</label></div>
      <div><label for="acctToken">接码查询 Token</label>
        <input id="acctToken" type="password" autocomplete="new-password" maxlength="16384"
               placeholder="171mail / MailCatcher query token">
        <div class="field-help">只用于读取邮箱验证码，不是 OpenAI 登录凭据。</div>
        <label class="check-label" for="acctClearToken"><input id="acctClearToken" type="checkbox">
          清除该账号已有查询 token</label></div>
      <div><label for="acctGroup">账号组</label><input id="acctGroup" maxlength="100" value="standard">
        <div class="field-help">Job 可按这个 Group 自动挑选账号。</div></div>
    </div>
    <button class="btn" onclick="addAccount()">添加登录账号</button>
    </section>

    <section class="account-editor" aria-labelledby="agentApiAccountTitle">
      <h3 id="agentApiAccountTitle">添加 Agent API 账号 <span class="field-code">Agent API accounts</span></h3>
      <p class="hint" id="apiAcctHint"></p>
      <div class="grid3" style="margin-top:8px">
        <div><label for="apiAcctProvider">API Provider</label><select id="apiAcctProvider" onchange="updateAgentApiProviderUI()">
          <option value="cloudrouter">CloudRouter</option>
          <option value="apex">ApexRouter</option>
        </select></div>
        <div><label for="apiAcctName">显示名称</label><input id="apiAcctName" maxlength="100" placeholder="research-router"></div>
        <div><label for="apiAcctGroup">账号组</label><input id="apiAcctGroup" maxlength="100" value="standard"></div>
      </div>
      <label for="apiAcctKey">API Key（写入后不回显）</label>
      <input id="apiAcctKey" type="password" autocomplete="new-password" maxlength="16384"
             placeholder="CloudRouter API Key">
      <button class="btn" id="apiAcctAdd" onclick="addAgentApiAccount()">Add CloudRouter API</button>
    </section>
  </div>

  <!-- Job submission -->
  <div class="submission-tabs" role="tablist" aria-label="Job 提交方式">
    <button type="button" class="btn submission-tab" id="singleSubmissionTab"
            role="tab" aria-selected="true" aria-controls="jobSubmissionCard"
            onclick="selectSubmissionMode('single')">单个 Job 表单</button>
    <button type="button" class="btn submission-tab" id="batchJsonSubmissionTab"
            role="tab" aria-selected="false" aria-controls="batchJsonSubmissionCard"
            onclick="selectSubmissionMode('batch-json')">批量 JSON</button>
  </div>

  <div class="card" id="jobSubmissionCard" role="tabpanel"
       aria-labelledby="singleSubmissionTab">
    <h2>提交 Job <span class="field-code">Submit Job</span></h2>
    <p class="section-intro">先完成必需项，再按任务需要展开高级设置。页面中的原始 JobSpec 字段名以小字标出。</p>
    <div class="job-form">
    <fieldset class="form-section" data-job-section="basics">
      <legend>1 · 基本信息</legend>
      <p class="section-intro">给本次任务命名，并选择所有 Worker 共用的版本化基础环境。</p>
      <div class="form-grid">
        <div class="field"><label for="jName">Job 名称</label>
          <input id="jName" placeholder="ai4sci-opus48-seed128" autocomplete="off">
          <div class="field-help">用于页面识别、云资源标签和默认机器名称。</div></div>
        <div class="field"><label for="jProfile">Worker 基础环境 <span class="field-code">environment.profile</span></label>
          <select id="jProfile" aria-describedby="jProfileHelp">
            <option value="ubuntu-agent-v1">标准 Agent 环境（ubuntu-agent-v1）</option>
            <option value="ubuntu-agent-docker-v1">预装 Docker 环境（ubuntu-agent-docker-v1）</option>
          </select>
          <div class="field-help" id="jProfileHelp">版本化、不可变的通用环境；Job 专属依赖请在“代码与初始化”中安装。</div></div>
      </div>
      <details class="form-details">
        <summary>机器命名与 Region</summary>
        <div class="form-grid">
          <div class="field"><label for="jNamePrefix">机器名称前缀 <span class="field-code">fanout.name_prefix</span></label>
            <input id="jNamePrefix" placeholder="留空则使用 Job 名称">
            <div class="field-help">EC2 Name 会写成“前缀-i”；不影响 Job ID。</div></div>
          <div class="field"><label for="jRegion">运行 Region <span class="field-code">fanout.region</span></label>
            <input id="jRegion" placeholder="留空则使用当前 Manager Region">
            <div class="field-help">目前不支持跨区，填写值必须与 Manager 的 provider Region 一致。</div></div>
        </div>
      </details>
    </fieldset>

    <fieldset class="form-section" data-job-section="compute">
      <legend>2 · 计算资源</legend>
      <p class="section-intro">这些资源按每台 Worker 计算；Worker 数量会同时影响机器数、账号数和总成本。</p>
      <div class="form-grid">
        <div class="field"><label for="jWorkers">Worker 数量 <span class="field-code">fanout.workers</span></label>
          <input id="jWorkers" type="number" value="1" min="1" max="100"
                 aria-describedby="jWorkersHelp">
          <div class="field-help" id="jWorkersHelp">每台 Worker 各运行一次命令；上限 100，仍受部署容量策略限制。</div></div>
      <div class="field"><label for="jInstanceType">每台 Worker 的实例类型 <span class="field-code">fanout.instance_type</span></label>
        <select id="jInstanceType">
          <option value="">使用 Manager 默认实例类型</option>
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
        </select>
        <div class="field-help">仅允许部署白名单中的类型；AI4Sci 等内存任务通常选 r5 系列。</div></div>
      <div class="field"><label for="jDiskGb">每台 Worker 根盘（GiB） <span class="field-code">fanout.disk_gb</span></label>
        <input id="jDiskGb" type="number" value="0" min="0" max="2048"
               aria-describedby="jDiskGbHelp">
        <div class="field-help" id="jDiskGbHelp">0 使用 Manager 默认值；吃盘任务建议至少 60 GiB。Worker 销毁时根盘一并删除。</div></div>
      <div class="field"><label for="jSpot">购买方式 <span class="field-code">fanout.spot</span></label>
        <select id="jSpot"><option value="false">按需实例（推荐）</option><option value="true">Spot 实例</option></select>
        <div class="field-help">Spot 成本较低，但云厂商可能随时回收实例。</div></div>
      <div class="field"><label for="jNeedsDocker">运行时是否需要 Docker <span class="field-code">setup.needs_docker</span></label>
        <select id="jNeedsDocker"><option value="false">不需要</option><option value="true">需要</option></select>
        <div class="field-help">运行命令会使用 Docker（例如 AI4Sci <code>--sandbox os</code>）时选择“需要”。</div></div>
      </div>
    </fieldset>

    <fieldset class="form-section" data-job-section="source">
      <legend>3 · 代码与初始化</legend>
      <p class="section-intro">指定代码来源和启动前命令；Setup 与 Run 默认都在 Worker 代码目录中执行。</p>
      <div class="form-grid">
        <div class="field field-span-full"><label for="jRepo">代码仓库 URL <span class="field-code">setup.repo</span></label>
          <input id="jRepo" aria-describedby="jRepoHelp"
                 placeholder="https://github.com/ApexIntelligence-AI/Agent-AI4Sci-Bench.git"
                 oninput="updateSourceUI()">
          <div class="field-help" id="jRepoHelp">可留空直接执行命令；填写后仓库会放到下方 Worker 代码目录。</div></div>
        <div class="field"><label for="jDeliver">代码分发方式 <span class="field-code">setup.deliver</span></label>
          <select id="jDeliver" onchange="updateDeliveryUI()" aria-describedby="jDeliverHelp">
            <option value="manager_rsync">Manager 安全分发（私库推荐）</option>
            <option value="worker_clone">Worker 直接克隆（公开仓库）</option>
          </select>
          <div class="field-help" id="jDeliverHelp" data-state></div></div>
        <div class="field"><label for="jTargetDir">Worker 代码目录 <span class="field-code">setup.target_dir</span></label>
          <input id="jTargetDir" value="/opt/elastic-agent/harness">
          <div class="field-help">Repo 克隆到这里；Setup 与 Run 默认都从这里开始。</div></div>
      </div>
      <div class="field" style="margin-top:10px"><label for="jSetup">初始化命令 <span class="field-code">setup.commands</span></label>
        <textarea id="jSetup" class="textarea-setup" placeholder="uv sync">curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH" && uv python pin 3.13 && uv sync --python 3.13</textarea>
        <div class="field-help">每行一条并按顺序执行。默认命令会安装 uv、固定 Python 3.13 并同步依赖。</div>
      </div>
      <details class="form-details">
        <summary>版本锁定、结构化步骤与 S3 数据集</summary>
        <div class="form-grid">
          <div class="field"><label for="jRepoRef">分支或标签 <span class="field-code">setup.ref</span></label>
            <input id="jRepoRef" value="archive/youchengsong-managed-agent-api-20260728"
                   aria-describedby="jRepoVersionHelp">
            <div class="field-help" id="jRepoVersionHelp">
              AI4Sci Bench 默认使用已锁定的归档分支；其他仓库请改成它实际存在的分支或标签。仅填写 Repo 时生效。
            </div></div>
          <div class="field"><label for="jResolvedCommit">锁定 Commit SHA <span class="field-code">setup.resolved_commit</span></label>
            <input id="jResolvedCommit" placeholder="完整 40 位 SHA（推荐）"
                   aria-describedby="jRepoVersionHelp">
            <div class="field-help">锁定精确代码版本，便于复现。</div></div>
          <div class="field field-span-full"><label for="jSetupSteps">结构化初始化步骤（JSON） <span class="field-code">setup.steps</span></label>
            <textarea id="jSetupSteps" class="textarea-compact" placeholder='[{"name":"install","command":"uv sync","env":{"UV_LINK_MODE":"copy"},"cwd":".","timeout":1200,"retries":1}]'></textarea>
            <div class="field-help">逐行命令执行完后再执行这些步骤；可分别设置 env、cwd、timeout 和 retries，始终以 Job 用户运行。</div></div>
          <div class="field field-span-full"><label for="jS3">S3 数据集 <span class="field-code">setup.s3_datasets</span></label>
            <textarea id="jS3" class="textarea-compact" placeholder="s3://my-bucket/shard-{{ shard_id }}.tar /home/ubuntu/data files"></textarea>
            <div class="field-help">每行格式为 <code>s3://桶/对象 目标路径</code>；模板内可有空格，目标路径也可包含空格。worker 用实例角色直拉，不经 Manager。</div></div>
        </div>
      </details>
      <div class="form-notice field-help">路径契约：可以把命令写成与本地 <code>git clone &amp;&amp; cd repo &amp;&amp; …</code> 相同的相对路径逻辑。</div>
    </fieldset>
    <fieldset class="form-section" data-job-section="account">
      <legend>4 · Agent 与账号</legend>
      <p class="section-intro">选择运行命令使用的 Agent、凭据来源和账号池；Agent API 账号也会按能力自动进入对应账号池。</p>
      <div class="form-grid">
        <div class="field"><label for="jAgentType">Agent</label>
          <select id="jAgentType" onchange="updateAgentUI()">
            <option value="claude">Claude Code</option><option value="codex">Codex</option>
          </select>
          <div class="field-help">决定安装、登录和注入哪一种 Agent 凭据。</div></div>
        <div class="field"><label for="jAcctMode">账号使用方式 <span class="field-code">account.mode</span></label>
          <select id="jAcctMode" onchange="updateAccountModeUI()" aria-describedby="jAcctModeHelp">
            <option value="worker_local_login">Worker 本地登录（推荐）</option>
            <option value="none">不配置账号</option>
          </select>
          <div class="field-help" id="jAcctModeHelp">账号和 Agent API Key 均在 Worker 本地准备；不透明命令自带凭据时可选择“不配置账号”。</div></div>
        <div class="field"><label for="jAcctGroup">账号组 <span class="field-code">account.group</span></label>
          <input id="jAcctGroup" value="standard">
          <div class="field-help">未指定具体账号时，Manager 从这个组中自动分配。</div></div>
        <div class="field"><label for="jAgentModel">Agent 模型（可选） <span class="field-code">account.model</span></label>
          <input id="jAgentModel" placeholder="如 gpt-5.4 或 claude-opus-4-8">
          <div class="field-help">Agent API 账号会按 Provider 返回的模型列表精确校验。</div></div>
      </div>
      <div class="field-help" id="jAccountStateHint" data-state role="status" aria-live="polite"></div>

      <div class="form-grid" style="margin-top:10px">
        <div class="field"><label for="jAcctBinding">固定公网出口 <span class="field-code">account.binding</span></label>
          <select id="jAcctBinding" onchange="markEipBindingTouched()"
                  aria-describedby="jAcctBindingHelp jEipHint">
            <option value="none">普通临时公网出口</option>
            <option value="eip">账号固定 EIP（一号一 IP）</option>
          </select>
          <div class="field-help" id="jAcctBindingHelp">只适用于 Worker 本地登录；AWS Manager 默认选择固定 EIP。</div></div>
        <div class="field"><label for="jAcctIds">指定账号（可选） <span class="field-code">account.ids</span></label>
          <select id="jAcctIds" multiple size="4" disabled
                  aria-describedby="jAcctIdsHelp jEipHint"></select>
          <div class="field-help" id="jAcctIdsHelp">
            Ctrl/Cmd 多选，所选唯一账号按列表顺序映射；留空则按账号组自动选择。
            普通出口下，单个 Agent API Key 可自动填满全部槽；任意排序或重复映射请直接提交 JobSpec。
          </div></div>
      </div>
      <div class="form-notice field-help" id="jEipHint" role="status" aria-live="polite">
        固定 EIP 模式下，每台临时 EC2 只使用一个账号；指定账号数必须等于 Worker 数。
        新 EC2 会重新准备账号凭据：OAuth 账号本地登录，Agent API 账号配置 Key。
        Job 结束后会销毁 EC2，但保留并继续计费 EIP。
      </div>

      <details class="form-details">
        <summary>登录目录、每机账号池与超时</summary>
        <div class="form-grid">
          <div class="field"><label for="jConfigDir">凭据目录 <span class="field-code">account.config_dir</span></label>
            <input id="jConfigDir" placeholder="留空则使用 Agent 默认目录">
            <div class="field-help">多账号槽必须填写 Worker 上可写的绝对路径。</div></div>
          <div class="field"><label for="jPerWorker">每台 Worker 预登录账号数 <span class="field-code">account.per_worker</span></label>
            <input id="jPerWorker" type="number" value="1" min="1" max="32"
                   aria-describedby="jPerWorkerHelp">
            <div class="field-help" id="jPerWorkerHelp">普通模式可预登录多个账号以便快速切换；固定 EIP 模式强制为 1。</div></div>
          <div class="field"><label for="jLoginTimeout">自动登录页面超时（秒） <span class="field-code">account.login_timeout_seconds</span></label>
            <input id="jLoginTimeout" type="number" value="900" min="60" max="1200">
            <div class="field-help">仅控制浏览器自动登录阶段；范围 60–1200 秒。</div></div>
        </div>
      </details>
    </fieldset>

    <fieldset class="form-section" data-job-section="run">
      <legend>5 · 运行命令</legend>
      <p class="section-intro">这是每台 Worker 真正执行的命令。模板变量会在分发时解析，Shell 变量留给 Worker。</p>
      <div class="field"><label for="jRun">运行命令<span class="required-mark">必填</span> <span class="field-code">run.command</span></label>
        <textarea id="jRun" class="textarea-command" required aria-describedby="jRunHelp"
                  oninput="updateResumeCommandSuggestion()"
                  placeholder='uv run ai4sci-bench run --output-dir "results/opus48_shard-{{shard_id}}_seed128"'></textarea>
        <div class="field-help" id="jRunHelp">支持稳定的 Manager 模板 <code>{{shard_id}}</code>/<code>{{shard_index}}</code>/<code>{{num_shards}}</code>。启用 checkpoint 时禁止 hostname 派生路径，因为替换 Worker 的 hostname 会变化。</div>
      </div>
      <div class="field" style="margin-top:10px">
        <label for="jRunResumeCommand">中断后续跑命令 <span class="field-code">run.resume_command</span></label>
        <textarea id="jRunResumeCommand" class="textarea-compact"
                  aria-describedby="jRunResumeCommandHelp"
                  oninput="markResumeCommandTouched()"
                  placeholder='与运行命令使用相同 --output-dir，并追加 --resume "<同一目录>"'></textarea>
        <div class="field-help" id="jRunResumeCommandHelp">
          只有配置此命令且发布了完整原子检查点，Job 中断完成后才会开放“一键续跑”。
          AI4Sci Bench 命令包含稳定 <code>--output-dir</code> 时会自动生成同目录的
          <code>--resume &lt;output-dir&gt;</code> 命令，你仍可手工修改。
        </div>
        <button type="button" class="btn btn-ghost" id="jAi4SciRecoveryPreset"
                style="margin-top:7px" onclick="applyAi4SciRecoveryPreset()">
          应用 AI4Sci 长任务可恢复预设
        </button>
      </div>
      <div class="form-grid" style="margin-top:10px">
        <div class="field"><label for="jCwd">命令工作目录 <span class="field-code">run.cwd</span></label>
          <input id="jCwd" value=".">
          <div class="field-help">空或 <code>.</code> 为代码目录；相对路径表示其子目录。</div></div>
        <div class="field"><label for="jShard">Worker 区分方式 <span class="field-code">fanout.shard_by</span></label>
          <select id="jShard">
            <option value="hostname">按主机名</option>
            <option value="shard_index">按分片序号</option>
            <option value="none">不区分</option>
          </select>
          <div class="field-help">决定多 Worker 任务使用哪种稳定标识。</div></div>
        <div class="field"><label for="jShell">命令解析方式 <span class="field-code">run.shell</span></label>
          <select id="jShell"><option value="true">Shell（bash -lc，推荐）</option>
            <option value="false">直接 argv（不展开 Shell 语法）</option></select>
          <div class="field-help">命令含管道、重定向或变量时使用 Shell。</div></div>
        <div class="field"><label for="jRunTimeout">运行超时（秒） <span class="field-code">run.timeout</span></label>
          <input id="jRunTimeout" type="number" value="86400" min="60" max="2592000"
                 aria-describedby="jRunTimeoutHelp">
          <div class="field-help" id="jRunTimeoutHelp">默认 24 小时，最长 30 天；仅计算命令执行阶段。</div></div>
        <div class="field"><label for="jTtl">Job 总生命周期（秒） <span class="field-code">ttl_seconds</span></label>
          <input id="jTtl" type="number" value="172800" min="300" max="2592000"
                 aria-describedby="jTtlHelp">
          <div class="field-help" id="jTtlHelp">包含申请机器、初始化、登录、运行和结果收集；不得短于运行超时。</div></div>
      </div>
      <details class="form-details">
        <summary>环境变量与秘密引用</summary>
        <div class="form-grid">
          <div class="field"><label for="jEnv">普通环境变量 <span class="field-code">run.env</span></label>
            <textarea id="jEnv" class="textarea-compact" placeholder="AI4SCI_SANDBOX_CPU=1&#10;AI4SCI_SANDBOX_MEM=4g"></textarea>
            <div class="field-help">每行一个 <code>KEY=VALUE</code>；会保存在 JobSpec 中。</div></div>
          <div class="field"><label for="jSecretEnv">秘密环境变量引用 <span class="field-code">run.secret_env</span></label>
            <textarea id="jSecretEnv" class="textarea-compact" placeholder="OPENAI_API_KEY=aws-secretsmanager://prod/openai#api_key&#10;DB_PASSWORD=aws-ssm:///prod/db/password"></textarea>
            <div class="field-help">每行一个 AWS Secrets Manager 或 SSM 引用；明文只在下发前解析，不写回 JobSpec。</div></div>
        </div>
      </details>
    </fieldset>

    <fieldset class="form-section" data-job-section="results">
      <legend>6 · 结果收集</legend>
      <p class="section-intro">只有这里明确列出的目录会成为可下载结果；命令 stdout/stderr 属于任务日志，不会自动上传。</p>
      <div class="form-grid">
        <div class="field"><label for="jCollect">需要保存的结果目录 <span class="field-code">collect.paths</span></label>
          <textarea id="jCollect" class="textarea-compact" placeholder="results"
                    aria-describedby="jCollectHelp">results</textarea>
          <div class="field-help" id="jCollectHelp">每行一个、相对代码目录。为空表示不收集任何结果。</div></div>
        <div class="field"><label for="jCollectInterval">运行中收集间隔（秒） <span class="field-code">collect.interval_seconds</span></label>
          <input id="jCollectInterval" type="number" value="0" min="0" max="86400"
                 oninput="updateCollectUI()" aria-describedby="jCollectIntervalHelp">
          <div class="field-help" id="jCollectIntervalHelp" data-state role="status" aria-live="polite">
            0 表示只在成功、失败或取消时做最终收集。
          </div></div>
        <div class="field"><label for="jCollectCheckpoint">S3 原子检查点 <span class="field-code">collect.checkpoint</span></label>
          <select id="jCollectCheckpoint" onchange="updateCollectUI()"
                  aria-describedby="jCollectCheckpointHelp">
            <option value="false">关闭</option>
            <option value="true">开启（长任务推荐）</option>
          </select>
          <div class="field-help" id="jCollectCheckpointHelp">开启后每次成功收集都会写入不可变、带 SHA-256 校验的 S3 generation；需要 Manager 结果桶。</div></div>
        <div class="field"><label for="jCollectExclude">收集排除规则 <span class="field-code">collect.exclude</span></label>
          <textarea id="jCollectExclude" class="textarea-compact"
                    placeholder=".venv/**&#10;**/core"
                    aria-describedby="jCollectExcludeHelp"></textarea>
          <div class="field-help" id="jCollectExcludeHelp">每行一个相对 glob；用于排除缓存、虚拟环境和崩溃转储。</div></div>
        <div class="field"><label for="jCheckpointRetention">保留完整检查点数 <span class="field-code">collect.checkpoint_keep_generations</span></label>
          <input id="jCheckpointRetention" type="number" value="3" min="1" max="100">
          <div class="field-help">保留最近的完整 Job 级恢复集合；相同内容按 SHA-256 去重，不会每次全量重复存储。</div></div>
      </div>
      <div class="form-notice field-help">长任务建议设为 120 秒并开启原子检查点。间隔大于 0 时页面可下载最近一次完整快照；配置结果桶时会同步到 S3，否则保留在 Manager 本地。</div>
      <details class="form-details">
        <summary>从先前 Job 的检查点恢复</summary>
        <div class="form-grid">
          <div class="field"><label for="jRecoveryPolicy">恢复来源 <span class="field-code">recovery.policy</span></label>
            <select id="jRecoveryPolicy" onchange="updateRecoveryUI()"
                    aria-describedby="jRecoveryHelp">
              <option value="none">不恢复（全新运行）</option>
              <option value="checkpoint">已校验的原子检查点</option>
            </select>
            <div class="field-help" id="jRecoveryHelp" data-state role="status" aria-live="polite">不读取先前 Job 的文件。</div></div>
          <div class="field"><label for="jRecoveryJob">来源 Job ID <span class="field-code">recovery.source_job_id</span></label>
            <input id="jRecoveryJob" disabled placeholder="job-..."
                   aria-describedby="jRecoveryJobHelp">
            <div class="field-help" id="jRecoveryJobHelp">来源必须已经终止，Worker 数、仓库和 resolved commit 必须与当前 Job 一致。</div></div>
          <div class="field"><label for="jRecoveryPaths">恢复目录 <span class="field-code">recovery.paths</span></label>
            <textarea id="jRecoveryPaths" class="textarea-compact" disabled
                      aria-describedby="jRecoveryPathsHelp">results</textarea>
            <div class="field-help" id="jRecoveryPathsHelp">每行一个，必须是来源 Job 已收集的目录；在登录和运行命令前恢复。</div></div>
          <div class="field"><label for="jRecoveryGeneration">指定恢复集合（可选） <span class="field-code">recovery.generation</span></label>
            <input id="jRecoveryGeneration" disabled
                   aria-describedby="jRecoveryGenerationHelp"
                   placeholder="留空使用最新完整 checkpoint set">
            <div class="field-help" id="jRecoveryGenerationHelp">仅原子检查点模式可用；一个 set 会固定引用全部 Worker 的已校验 generation，缺任一分片都不会发布。</div></div>
        </div>
      </details>
    </fieldset>

    <fieldset class="form-section" data-job-section="rotation">
      <legend>7 · 额度耗尽与续跑</legend>
      <p class="section-intro">仅用于 Elastic 能从输出中识别额度耗尽、并由新账号恢复执行的普通 Worker 模式。</p>
      <div class="form-grid">
        <div class="field"><label for="jRot">额度耗尽后的处理 <span class="field-code">rotation.strategy</span></label>
          <select id="jRot" onchange="updateRotationUI()" aria-describedby="jRotationHint">
            <option value="none">不自动切换账号</option>
            <option value="on_exhaust_restart_resume">换号、重启并追加续跑参数</option>
          </select></div>
        <div class="field"><label for="jResume">换号重启时追加的参数 <span class="field-code">rotation.resume_args</span></label>
          <input id="jResume" aria-describedby="jRotationHint"
                 placeholder='--resume "results/opus48_shard-{{shard_id}}_seed128"'></div>
        <div class="field"><label for="jMaxRotations">最多自动换号次数 <span class="field-code">rotation.max_rotations</span></label>
          <input id="jMaxRotations" type="number" value="20" min="0" max="100"></div>
      </div>
      <div class="field-help" id="jRotationHint" data-state role="status" aria-live="polite"></div>
    </fieldset>

    <fieldset class="form-section" data-job-section="advanced">
      <legend>8 · 高级：自定义 Harness</legend>
      <p class="section-intro">绝大多数 Job 不需要填写。留空时使用上面配置生成的声明式 JobSpec。</p>
      <details class="form-details">
        <summary>上传并使用 Harness Python 代码</summary>
        <div class="form-notice warning field-help">
          仅限受信任管理员：Harness 是 Manager 任意代码执行边界，生产默认关闭上传接口。
        </div>
        <div class="form-grid" style="margin-top:10px">
          <div class="field"><label for="hFile">文件名（<code>&lt;name&gt;.py</code>）</label>
            <input id="hFile" maxlength="128" placeholder="my_harness.py"></div>
          <div class="field"><label for="hClass">Harness 类名</label>
            <input id="hClass" maxlength="128" placeholder="MyHarness"></div>
          <div class="field field-span-full"><label for="hCode">Harness Python 代码</label>
            <textarea id="hCode" class="textarea-command" maxlength="1048576"></textarea></div>
        </div>
        <button class="btn btn-ghost" onclick="uploadHarness()">上传并写入 harness_ref</button>
        <div class="field" style="margin-top:10px"><label for="jHarnessRef">已上传 Harness 引用 <span class="field-code">harness_ref</span></label>
          <input id="jHarnessRef" placeholder="留空则使用声明式配置">
          <div class="field-help">设置后由上传的 Harness 驱动 Job；上方声明式字段仍用于预览，但执行边界以 Harness 为准。</div></div>
      </details>
    </fieldset>

    <div class="form-actions">
      <button class="btn btn-ghost" id="jPlanBtn" onclick="previewJob()">仅校验并查看计划</button>
      <button class="btn" id="jSubmitBtn" onclick="submitJob()">校验并启动 Job</button>
    </div>
    <pre id="jPlanOutput" class="plan-result" role="status" aria-live="polite"
         tabindex="0" style="display:none"></pre>
    </div>
  </div>

  <div class="card" id="batchJsonSubmissionCard" role="tabpanel"
       aria-labelledby="batchJsonSubmissionTab" hidden>
    <h2>批量提交 Job <span class="field-code">Batch JSON</span></h2>
    <p class="section-intro">
      选择一个 schema v1 JSON 文件，先对全部 Job 做无副作用预检；只有全部有效后才会开放确认启动。
      页面不会自动创建 Job。
    </p>
    <div class="batch-privacy-note">
      文件不会作为附件上传，也不会持久保存到浏览器存储；仅在你点击预检或确认提交时，
      将同一份 UTF-8 JSON 内容发送给 Manager。页面不会展示 <code>run.env</code> 或
      <code>run.secret_env</code> 的值。
    </div>
    <div class="batch-upload-row">
      <div class="field">
        <label for="batchJsonFile">Job batch manifest（最大 2 MiB）</label>
        <input id="batchJsonFile" type="file" accept=".json,application/json"
               onchange="batchJsonFileChanged()" aria-describedby="batchJsonFormatHint">
      </div>
      <button type="button" class="btn btn-ghost" id="batchJsonPlanBtn"
              onclick="planBatchJson()" disabled>解析并校验全部</button>
    </div>
    <div class="field-help" id="batchJsonFormatHint">
      严格格式：<code>schema_version: 1</code>、<code>batch_id</code>、可选
      <code>policy</code>，以及 <code>jobs: [{client_id, spec}]</code>。v1 policy 只支持
      <code>max_active_jobs</code>（1–10）和 <code>on_job_failure: "continue"</code>。
      <code>batch_id</code> 同时是幂等身份；修改内容后必须换一个新 batch_id。
    </div>
    <div class="batch-file-meta muted" id="batchJsonFileMeta" role="status"
         aria-live="polite">尚未选择文件。</div>
    <div id="batchJsonAlert" role="alert" hidden></div>

    <section id="batchJsonPlanResult" aria-labelledby="batchJsonPlanTitle" hidden>
      <h3 id="batchJsonPlanTitle" style="font-size:.95rem;margin-top:14px">批量预检结果</h3>
      <div class="batch-summary-grid">
        <div class="batch-summary-stat"><span class="muted">文件 SHA-256</span>
          <b id="batchSummaryHash">--</b></div>
        <div class="batch-summary-stat"><span class="muted">Jobs</span>
          <b id="batchSummaryJobs">0</b></div>
        <div class="batch-summary-stat"><span class="muted">总 Workers</span>
          <b id="batchSummaryWorkers">0</b></div>
        <div class="batch-summary-stat"><span class="muted">总 Worker-hours</span>
          <b id="batchSummaryWorkerHours">0</b></div>
        <div class="batch-summary-stat"><span class="muted">最大并发 Jobs</span>
          <b id="batchSummaryConcurrency">0</b></div>
      </div>
      <ul class="batch-instance-list" id="batchSummaryInstances"
          aria-label="实例类型分布"></ul>
      <div class="batch-plan-items" id="batchJsonPlanItems"></div>
      <div class="batch-confirm-actions">
        <span class="muted" id="batchJsonConfirmHint">预检尚未通过，不会创建任何资源。</span>
        <button type="button" class="btn" id="batchJsonSubmitBtn"
                onclick="submitBatchJson()" disabled>确认启动批量 Jobs</button>
      </div>
    </section>

    <section id="batchJsonReceipt" aria-labelledby="batchJsonReceiptTitle" hidden>
      <div class="batch-receipt-head">
        <div class="batch-receipt-identifiers">
          <h3 id="batchJsonReceiptTitle" style="font-size:.95rem">批次提交回执</h3>
          <div class="muted" id="batchJsonReceiptIds"></div>
        </div>
        <span class="badge b-pending" id="batchJsonReceiptState">--</span>
      </div>
      <div class="batch-receipt-items" id="batchJsonReceiptItems"></div>
    </section>
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
<div class="toast" id="toast" role="status" aria-live="polite" aria-atomic="true"></div>

<script>
{const nav = document.getElementById('navFleet'); if (nav) nav.href = '/fleet';}
let eipBindingTouched = false;
let resumeCommandTouched = false;
let providerType = '';
let providerDefaultsReady;
let latestJobs = [];
let showLegacyHistory = false;
let dashboardPollRunning = false;
let dashboardPollTimer = null;
let accountsRefreshInFlight = null;
let accountsRefreshQueued = false;
let accountsRequestVersion = 0;
let lastAccountsRefreshAt = 0;
const jobResultsCache = new Map();
const jobResultsRequestVersions = new Map();
const resultDownloadsInFlight = new Map();
const JOB_SPEC_CACHE_MAX_ENTRIES = 8;
const JOB_SPEC_CACHE_MAX_CHARS = 4_000_000;
const JOB_SPEC_TEXT_MAX_CHARS = 1_000_000;
const JOB_SPEC_REQUEST_CONCURRENCY = 2;
const JOB_SPEC_REQUEST_QUEUE_MAX = 8;
const jobSpecCache = new Map();
const jobSpecRequests = new Map();
const jobSpecRequestQueue = [];
let jobSpecCacheChars = 0;
let jobSpecRevision = 0;
let jobSpecRequestActive = 0;
const PENDING_JOB_SUBMISSION_KEY = 'ea_pending_job_submission';
let latestLoginAttempts = [];
const otpCardsByKey = new Map();
const openedOtpChallenges = new Set();
const otpSubmitting = new Set();
function loadPendingJobSubmission() {
  try {
    const value = JSON.parse(
      sessionStorage.getItem(PENDING_JOB_SUBMISSION_KEY) || 'null'
    );
    return value && typeof value.spec === 'string' && typeof value.key === 'string'
      ? value
      : null;
  } catch(e) {
    sessionStorage.removeItem(PENDING_JOB_SUBMISSION_KEY);
    return null;
  }
}
function savePendingJobSubmission(value) {
  window._pendingJobSubmission = value;
  sessionStorage.setItem(PENDING_JOB_SUBMISSION_KEY, JSON.stringify(value));
}
function clearPendingJobSubmission() {
  window._pendingJobSubmission = null;
  sessionStorage.removeItem(PENDING_JOB_SUBMISSION_KEY);
}
function parsePendingJobSpec(pending) {
  const spec = JSON.parse(pending.spec);
  if (!spec || typeof spec !== 'object' || Array.isArray(spec)) {
    throw new Error('保存的待重试 Job 配置已损坏；请明确丢弃后重新提交。');
  }
  return spec;
}
window._pendingJobSubmission = loadPendingJobSubmission();
const AUTHENTICATED_UI_PATHS = new Set(['/', '/batch', '/fleet', '/dashboard']);
let csrfToken = '';
function safeCurrentUiPath() {
  return AUTHENTICATED_UI_PATHS.has(window.location.pathname)
    ? window.location.pathname : '/';
}
function redirectToLogin() {
  const next = encodeURIComponent(safeCurrentUiPath());
  window.location.assign('/login?next=' + next);
}
async function initializeAuthentication() {
  const response = await fetch('/api/auth/me', {
    credentials:'same-origin', headers:{'Accept':'application/json'},
  });
  if (response.status === 401) {
    redirectToLogin();
    throw new Error('登录已失效');
  }
  if (!response.ok) throw new Error(`${response.status}: ${await response.text()}`);
  const session = await response.json();
  if (session.must_change_password === true) {
    window.location.assign(
      '/change-password?next=' + encodeURIComponent(safeCurrentUiPath())
    );
    throw new Error('需要先修改初始密码');
  }
  csrfToken = String(session.csrf_token || '');
  document.getElementById('currentUserEmail').textContent = session.email || '';
  return session;
}
const authenticationReady = initializeAuthentication();
async function authenticatedFetch(input, init={}) {
  await authenticationReady;
  const options = {...init, credentials:'same-origin'};
  const method = String(options.method || 'GET').toUpperCase();
  const requestHeaders = new Headers(options.headers || {});
  if (!['GET', 'HEAD', 'OPTIONS'].includes(method)) {
    requestHeaders.set('X-CSRF-Token', csrfToken);
  }
  options.headers = requestHeaders;
  const response = await fetch(input, options);
  if (response.status === 401) redirectToLogin();
  return response;
}
async function logout() {
  try {
    const response = await authenticatedFetch('/api/auth/logout', {method:'POST'});
    if (!response.ok) throw new Error(`${response.status}: ${await response.text()}`);
    window.location.assign('/login');
  } catch (error) {
    toast('退出失败：' + error.message, 'error');
  }
}
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
  const requestHeaders = new Headers(extraHeaders);
  requestHeaders.set('Accept', 'application/json');
  const opts = {method, headers:requestHeaders};
  if (body !== undefined && body !== null) {
    requestHeaders.set('Content-Type', 'application/json');
    opts.body = JSON.stringify(body);
  }
  const resp = await authenticatedFetch('/api' + path, opts);
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

// ---- Batch JSON submission ----
const BATCH_JSON_MAX_BYTES = 2 * 1024 * 1024;
const BATCH_JSON_MAX_JOBS = 100;
const BATCH_JSON_PLACEHOLDERS = ['[REDACTED]', '[SECRET_REFERENCE]'];
const batchJsonState = {
  generation: 0,
  manifest: null,
  rawSource: '',
  fileHash: '',
  idempotencyKey: '',
  plan: null,
  summary: null,
  planValid: false,
  submitted: false,
  jobBatchId: '',
  batchTerminal: false,
  refreshRunning: false,
};

function selectSubmissionMode(mode) {
  const batchMode = mode === 'batch-json';
  document.getElementById('jobSubmissionCard').hidden = batchMode;
  document.getElementById('batchJsonSubmissionCard').hidden = !batchMode;
  document.getElementById('singleSubmissionTab').setAttribute(
    'aria-selected', String(!batchMode)
  );
  document.getElementById('batchJsonSubmissionTab').setAttribute(
    'aria-selected', String(batchMode)
  );
}

function isBatchPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function batchDisplayNumber(value, digits=2) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return '0';
  return parsed.toLocaleString(undefined, {maximumFractionDigits: digits});
}

function setBatchAlert(message='', kind='error') {
  const alert = document.getElementById('batchJsonAlert');
  alert.replaceChildren();
  if (!message) {
    alert.hidden = true;
    alert.className = '';
    return;
  }
  alert.textContent = String(message);
  alert.className = 'batch-alert batch-alert-' + (
    kind === 'success' ? 'success' : kind === 'warning' ? 'warning' : 'error'
  );
  alert.hidden = false;
}

function batchHiddenEnvironmentValues() {
  const hidden = new Set();
  const jobs = Array.isArray(batchJsonState.manifest?.jobs)
    ? batchJsonState.manifest.jobs : [];
  for (const entry of jobs) {
    const run = isBatchPlainObject(entry?.spec?.run) ? entry.spec.run : {};
    for (const envName of ['env', 'secret_env']) {
      const values = isBatchPlainObject(run[envName]) ? run[envName] : {};
      for (const value of Object.values(values)) {
        if (typeof value === 'string' && value) hidden.add(value);
      }
    }
  }
  return Array.from(hidden).sort((left, right) => right.length - left.length);
}

function safeBatchServerText(value) {
  let textValue = typeof value === 'string' ? value : '';
  for (const hidden of batchHiddenEnvironmentValues()) {
    textValue = textValue.split(hidden).join('[已隐藏环境变量值]');
  }
  textValue = textValue
    .replace(/\\b(?:sk|key)-[A-Za-z0-9_-]{12,}\\b/gi, '[已隐藏凭据]')
    .replace(/\\s+/g, ' ')
    .trim();
  return textValue.slice(0, 800);
}

function batchIssueMessages(value) {
  const values = Array.isArray(value) ? value : value ? [value] : [];
  const messages = [];
  for (const issue of values.slice(0, 100)) {
    if (typeof issue === 'string') {
      const safe = safeBatchServerText(issue);
      if (safe) messages.push(safe);
      continue;
    }
    if (!isBatchPlainObject(issue)) continue;
    const location = Array.isArray(issue.loc)
      ? issue.loc.map(part => String(part).slice(0, 100)).join('.') : '';
    const message = safeBatchServerText(issue.msg || issue.message || issue.detail || '');
    if (message) messages.push(location ? location + ': ' + message : message);
  }
  return messages;
}

function safeBatchHttpDetail(payload, statusCode) {
  const detail = payload?.detail;
  const messages = [];
  if (isBatchPlainObject(detail)) {
    messages.push(...batchIssueMessages(detail.message));
    messages.push(...batchIssueMessages(detail.errors));
    const items = Array.isArray(detail.items) ? detail.items : [];
    for (const item of items.slice(0, 100)) {
      const prefix = typeof item?.client_id === 'string'
        ? String(item.client_id).slice(0, 128) + ': ' : '';
      messages.push(...batchIssueMessages(item?.errors).map(message => prefix + message));
    }
  } else {
    messages.push(...batchIssueMessages(detail || payload?.errors));
  }
  if (messages.length) return messages.join('\\n');
  return '批量请求失败（HTTP ' + Number(statusCode || 0) + '）。';
}

async function batchJsonApi(method, path, body, extraHeaders={}, rawJson=false) {
  const requestHeaders = new Headers(headers);
  for (const [name, value] of Object.entries(extraHeaders)) {
    requestHeaders.set(name, value);
  }
  requestHeaders.set('Accept', 'application/json');
  const options = {method, headers:requestHeaders};
  if (body !== undefined && body !== null) {
    requestHeaders.set('Content-Type', 'application/json');
    options.body = rawJson ? String(body) : JSON.stringify(body);
  }
  const response = await fetch('/api' + path, options);
  let payload = null;
  try { payload = await response.json(); } catch (_) {}
  if (!response.ok) {
    const error = new Error(safeBatchHttpDetail(payload, response.status));
    error.status = response.status;
    throw error;
  }
  return payload || {};
}

function batchUnexpectedKeys(value, allowed, path, errors) {
  if (!isBatchPlainObject(value)) return;
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) {
      errors.push(path + ' 包含不支持的字段：' + String(key).slice(0, 120));
    }
  }
}

function findBatchPlaceholders(root) {
  const findings = [];
  const pending = [{value: root, path: 'spec'}];
  while (pending.length && findings.length < 50) {
    const current = pending.pop();
    if (typeof current.value === 'string') {
      const upper = current.value.toUpperCase();
      for (const placeholder of BATCH_JSON_PLACEHOLDERS) {
        if (upper.includes(placeholder)) findings.push(current.path + ' 含 ' + placeholder);
      }
    } else if (Array.isArray(current.value)) {
      current.value.forEach((value, index) => pending.push({
        value, path: current.path + '[' + index + ']'
      }));
    } else if (isBatchPlainObject(current.value)) {
      for (const [key, value] of Object.entries(current.value)) {
        pending.push({value, path: current.path + '.' + String(key).slice(0, 100)});
      }
    }
  }
  return findings;
}

function assertNoDuplicateJsonKeys(source) {
  let position = 0;
  const skipWhitespace = () => {
    while (position < source.length && /\\s/.test(source[position])) position += 1;
  };
  const scanString = () => {
    const start = position;
    position += 1;
    while (position < source.length) {
      if (source[position] === '\\\\') {
        position += 2;
      } else if (source[position] === '"') {
        position += 1;
        return JSON.parse(source.slice(start, position));
      } else {
        position += 1;
      }
    }
    throw new Error('JSON 字符串未闭合。');
  };
  const scanValue = depth => {
    if (depth > 128) throw new Error('JSON 嵌套层级超过 128。');
    skipWhitespace();
    const token = source[position];
    if (token === '"') {
      scanString();
      return;
    }
    if (token === '{') {
      position += 1;
      skipWhitespace();
      const keys = new Set();
      if (source[position] === '}') { position += 1; return; }
      while (position < source.length) {
        skipWhitespace();
        const key = scanString();
        if (keys.has(key)) {
          throw new Error('JSON 中存在重复 object key；为避免歧义已拒绝该文件。');
        }
        keys.add(key);
        skipWhitespace();
        if (source[position] !== ':') throw new Error('JSON object 缺少冒号。');
        position += 1;
        scanValue(depth + 1);
        skipWhitespace();
        if (source[position] === '}') { position += 1; return; }
        if (source[position] !== ',') throw new Error('JSON object 分隔符无效。');
        position += 1;
      }
      throw new Error('JSON object 未闭合。');
    }
    if (token === '[') {
      position += 1;
      skipWhitespace();
      if (source[position] === ']') { position += 1; return; }
      while (position < source.length) {
        scanValue(depth + 1);
        skipWhitespace();
        if (source[position] === ']') { position += 1; return; }
        if (source[position] !== ',') throw new Error('JSON array 分隔符无效。');
        position += 1;
      }
      throw new Error('JSON array 未闭合。');
    }
    const start = position;
    while (position < source.length) {
      const character = source[position];
      if (/\\s/.test(character) || [',', '}', ']'].includes(character)) break;
      position += 1;
    }
    if (position === start) throw new Error('JSON value 无效。');
  };
  scanValue(0);
  skipWhitespace();
  if (position !== source.length) throw new Error('JSON 根节点后存在多余内容。');
}

function suspiciousBatchEnvKeys(spec) {
  const run = isBatchPlainObject(spec?.run) ? spec.run : {};
  const env = isBatchPlainObject(run.env) ? run.env : {};
  const suspicious = new RegExp(
    '(?:^|_)(?:API_?KEY|TOKEN|SECRET|PASSWORD|PASSWD|PRIVATE_?KEY|'
    + 'ACCESS_?KEY|CREDENTIALS?|AUTH)(?:_|$)',
    'i'
  );
  return Object.keys(env).filter(key => suspicious.test(key)).slice(0, 50);
}

function localBatchSummary(manifest) {
  let totalWorkers = 0;
  let totalWorkerHours = 0;
  const instanceTypes = {};
  const jobs = Array.isArray(manifest?.jobs) ? manifest.jobs : [];
  for (const entry of jobs) {
    const spec = isBatchPlainObject(entry?.spec) ? entry.spec : {};
    const fanout = isBatchPlainObject(spec.fanout) ? spec.fanout : {};
    const workers = Number.isInteger(fanout.workers) && fanout.workers > 0
      ? fanout.workers : 1;
    const ttl = Number.isFinite(Number(spec.ttl_seconds)) && Number(spec.ttl_seconds) > 0
      ? Number(spec.ttl_seconds) : 172800;
    const instanceType = typeof fanout.instance_type === 'string' && fanout.instance_type.trim()
      ? fanout.instance_type.trim() : 'Manager 默认';
    totalWorkers += workers;
    totalWorkerHours += workers * ttl / 3600;
    instanceTypes[instanceType] = Number(instanceTypes[instanceType] || 0) + workers;
  }
  const policy = isBatchPlainObject(manifest?.policy) ? manifest.policy : {};
  return {
    job_count: jobs.length,
    total_workers: totalWorkers,
    total_worker_hours: totalWorkerHours,
    max_active_jobs: Number.isInteger(policy.max_active_jobs)
      ? policy.max_active_jobs : 3,
    instance_types: instanceTypes,
  };
}

function validateBatchManifest(manifest) {
  const validation = {errors: [], warnings: [], items: [], summary: localBatchSummary(manifest)};
  if (!isBatchPlainObject(manifest)) {
    validation.errors.push('JSON 顶层必须是 object。');
    return validation;
  }
  batchUnexpectedKeys(
    manifest,
    new Set(['schema_version', 'batch_id', 'policy', 'jobs']),
    'manifest',
    validation.errors
  );
  if (manifest.schema_version !== 1) {
    validation.errors.push('schema_version 必须严格等于 1。');
  }
  if (typeof manifest.batch_id !== 'string' || !manifest.batch_id.trim()) {
    validation.errors.push('batch_id 必须是非空字符串。');
  } else if (manifest.batch_id.length > 128) {
    validation.errors.push('batch_id 不能超过 128 个字符。');
  } else if (!/^[A-Za-z0-9._-]+$/.test(manifest.batch_id)) {
    validation.errors.push('batch_id 只允许英文字母、数字、点、下划线和连字符。');
  }
  if (Object.prototype.hasOwnProperty.call(manifest, 'policy')) {
    if (!isBatchPlainObject(manifest.policy)) {
      validation.errors.push('policy 必须是 object。');
    } else {
      batchUnexpectedKeys(
        manifest.policy,
        new Set(['max_active_jobs', 'on_job_failure']),
        'policy',
        validation.errors
      );
      if (Object.prototype.hasOwnProperty.call(manifest.policy, 'max_active_jobs')
          && (!Number.isInteger(manifest.policy.max_active_jobs)
              || manifest.policy.max_active_jobs < 1
              || manifest.policy.max_active_jobs > 10)) {
        validation.errors.push('policy.max_active_jobs 必须是 1–10 的整数。');
      }
      if (Object.prototype.hasOwnProperty.call(manifest.policy, 'on_job_failure')
          && manifest.policy.on_job_failure !== 'continue') {
        validation.errors.push('schema v1 的 policy.on_job_failure 只支持 "continue"。');
      }
    }
  }
  if (!Array.isArray(manifest.jobs) || !manifest.jobs.length) {
    validation.errors.push('jobs 必须是非空 array。');
    return validation;
  }
  if (manifest.jobs.length > BATCH_JSON_MAX_JOBS) {
    validation.errors.push('浏览器单次最多校验 100 个 Jobs。');
  }
  const clientIds = new Set();
  manifest.jobs.slice(0, BATCH_JSON_MAX_JOBS).forEach((entry, index) => {
    const item = {client_id: 'jobs[' + index + ']', name: '', valid: true,
                  warnings: [], errors: []};
    if (!isBatchPlainObject(entry)) {
      item.errors.push('Job 项必须是 object。');
    } else {
      batchUnexpectedKeys(
        entry, new Set(['client_id', 'spec']), 'jobs[' + index + ']', item.errors
      );
      if (typeof entry.client_id !== 'string' || !entry.client_id.trim()) {
        item.errors.push('client_id 必须是非空字符串。');
      } else {
        item.client_id = entry.client_id;
        if (entry.client_id.length > 128) {
          item.errors.push('client_id 不能超过 128 个字符。');
        } else if (!/^[A-Za-z0-9._-]+$/.test(entry.client_id)) {
          item.errors.push('client_id 只允许英文字母、数字、点、下划线和连字符。');
        } else if (clientIds.has(entry.client_id)) {
          item.errors.push('client_id 在同一批次中必须唯一。');
        }
        clientIds.add(entry.client_id);
      }
      if (!isBatchPlainObject(entry.spec)) {
        item.errors.push('spec 必须是完整 JobSpec object。');
      } else {
        item.name = typeof entry.spec.name === 'string' ? entry.spec.name : '';
        item.errors.push(...findBatchPlaceholders(entry.spec).map(
          finding => finding + ' 脱敏占位符；请重新填写真实 AWS Secret/SSM 引用。'
        ));
        const suspiciousKeys = suspiciousBatchEnvKeys(entry.spec);
        if (suspiciousKeys.length) {
          item.warnings.push(
            '强警告：run.env 中疑似秘密字段 ' + suspiciousKeys.join(', ')
            + '；请移到 run.secret_env 并使用 AWS 引用。字段值未显示。'
          );
        }
      }
    }
    item.valid = item.errors.length === 0;
    validation.items.push(item);
  });
  return validation;
}

async function sha256Hex(buffer) {
  if (!window.crypto?.subtle) throw new Error('当前浏览器不支持安全 SHA-256。');
  const digest = await window.crypto.subtle.digest('SHA-256', buffer);
  return Array.from(new Uint8Array(digest))
    .map(byte => byte.toString(16).padStart(2, '0')).join('');
}

async function batchIdempotencyKey(batchId) {
  const bytes = new TextEncoder().encode('batch-json-v1\\n' + String(batchId));
  return 'batch-json-v1-' + await sha256Hex(bytes);
}

function clearBatchPlan() {
  batchJsonState.manifest = null;
  batchJsonState.rawSource = '';
  batchJsonState.fileHash = '';
  batchJsonState.idempotencyKey = '';
  batchJsonState.plan = null;
  batchJsonState.summary = null;
  batchJsonState.planValid = false;
  batchJsonState.submitted = false;
  document.getElementById('batchJsonPlanResult').hidden = true;
  document.getElementById('batchJsonPlanItems').replaceChildren();
  updateBatchConfirmButton();
}

function clearBatchReceipt() {
  batchJsonState.jobBatchId = '';
  batchJsonState.batchTerminal = false;
  const receipt = document.getElementById('batchJsonReceipt');
  receipt.hidden = true;
  document.getElementById('batchJsonReceiptIds').textContent = '';
  document.getElementById('batchJsonReceiptItems').replaceChildren();
}

function batchJsonFileChanged() {
  batchJsonState.generation += 1;
  clearBatchPlan();
  clearBatchReceipt();
  setBatchAlert();
  const file = document.getElementById('batchJsonFile').files[0];
  const button = document.getElementById('batchJsonPlanBtn');
  const meta = document.getElementById('batchJsonFileMeta');
  if (!file) {
    button.disabled = true;
    meta.textContent = '尚未选择文件。';
    return;
  }
  if (file.size > BATCH_JSON_MAX_BYTES) {
    button.disabled = true;
    meta.textContent = file.name + ' · ' + batchDisplayNumber(file.size) + ' bytes';
    setBatchAlert('文件超过 2 MiB 上限，未读取。', 'error');
    return;
  }
  button.disabled = file.size === 0;
  meta.textContent = file.name + ' · ' + batchDisplayNumber(file.size) + ' bytes · 尚未读取';
  if (file.size === 0) setBatchAlert('JSON 文件不能为空。', 'error');
}

function normalizedBatchSummary(summary, fallback) {
  const incoming = isBatchPlainObject(summary) ? summary : {};
  const totalWorkers = Number.isFinite(Number(incoming.total_workers))
    ? Number(incoming.total_workers) : fallback.total_workers;
  let instanceTypes = fallback.instance_types;
  if (isBatchPlainObject(incoming.instance_types)) {
    instanceTypes = incoming.instance_types;
  } else if (Array.isArray(incoming.instance_types)) {
    const effective = incoming.instance_types.filter(
      value => typeof value === 'string' && value
    );
    const distribution = {...fallback.instance_types};
    const defaultWorkers = Number(distribution['Manager 默认'] || 0);
    delete distribution['Manager 默认'];
    if (defaultWorkers) {
      const unresolved = effective.filter(
        instanceType => !Object.prototype.hasOwnProperty.call(distribution, instanceType)
      );
      if (unresolved.length === 1) {
        distribution[unresolved[0]] = Number(distribution[unresolved[0]] || 0)
          + defaultWorkers;
      } else if (!unresolved.length && effective.length === 1) {
        distribution[effective[0]] = Number(distribution[effective[0]] || 0)
          + defaultWorkers;
      } else {
        distribution['Manager 默认'] = defaultWorkers;
      }
    }
    instanceTypes = distribution;
  }
  return {
    job_count: Number.isFinite(Number(incoming.job_count))
      ? Number(incoming.job_count) : fallback.job_count,
    total_workers: totalWorkers,
    total_worker_hours: Number.isFinite(Number(incoming.total_worker_hours))
      ? Number(incoming.total_worker_hours) : fallback.total_worker_hours,
    max_active_jobs: Number.isFinite(Number(incoming.max_active_jobs))
      ? Number(incoming.max_active_jobs) : fallback.max_active_jobs,
    instance_types: instanceTypes,
  };
}

function renderBatchSummary(summary) {
  document.getElementById('batchSummaryHash').textContent = batchJsonState.fileHash || '--';
  document.getElementById('batchSummaryJobs').textContent = batchDisplayNumber(summary.job_count, 0);
  document.getElementById('batchSummaryWorkers').textContent = batchDisplayNumber(summary.total_workers, 0);
  document.getElementById('batchSummaryWorkerHours').textContent =
    batchDisplayNumber(summary.total_worker_hours);
  document.getElementById('batchSummaryConcurrency').textContent =
    batchDisplayNumber(summary.max_active_jobs, 0);
  const instances = document.getElementById('batchSummaryInstances');
  instances.replaceChildren();
  const entries = Object.entries(summary.instance_types || {});
  if (!entries.length) {
    const item = document.createElement('li');
    item.textContent = '实例类型：等待服务端计划';
    instances.append(item);
  } else {
    for (const [instanceType, count] of entries) {
      const item = document.createElement('li');
      item.textContent = String(instanceType) + ' × ' + batchDisplayNumber(count, 0) + ' Workers';
      instances.append(item);
    }
  }
}

function appendBatchMessages(container, messages, kind) {
  for (const message of messages) {
    const item = document.createElement('li');
    item.className = kind === 'error' ? 'batch-message-error' : 'batch-message-warning';
    item.textContent = (kind === 'error' ? '错误：' : '警告：') + safeBatchServerText(message);
    container.append(item);
  }
}

function renderBatchPlanItems(items) {
  const container = document.getElementById('batchJsonPlanItems');
  container.replaceChildren(document.createDocumentFragment());
  for (const item of items) {
    const card = document.createElement('article');
    card.className = 'batch-item';
    const head = document.createElement('div');
    head.className = 'batch-item-head';
    const title = document.createElement('div');
    title.className = 'batch-item-title';
    const client = document.createElement('b');
    client.textContent = String(item.client_id || '未命名项');
    title.append(client);
    if (item.name) {
      const name = document.createElement('div');
      name.className = 'muted';
      name.textContent = String(item.name);
      title.append(name);
    }
    const badge = document.createElement('span');
    badge.className = 'badge ' + (item.valid
      ? item.warnings.length ? 'b-rotating' : 'b-succeeded' : 'b-failed');
    badge.textContent = item.valid
      ? item.warnings.length ? 'VALID · WARNING' : 'VALID' : 'ERROR';
    head.append(title, badge);
    card.append(head);
    if (item.warnings.length || item.errors.length) {
      const messages = document.createElement('ul');
      messages.className = 'batch-item-messages';
      appendBatchMessages(messages, item.warnings, 'warning');
      appendBatchMessages(messages, item.errors, 'error');
      card.append(messages);
    }
    container.append(card);
  }
}

function updateBatchConfirmButton() {
  const button = document.getElementById('batchJsonSubmitBtn');
  const hint = document.getElementById('batchJsonConfirmHint');
  const summary = batchJsonState.summary;
  button.disabled = !batchJsonState.planValid || batchJsonState.submitted;
  if (!summary || !batchJsonState.planValid) {
    button.textContent = '确认启动批量 Jobs';
    hint.textContent = '预检尚未通过，不会创建任何资源。';
    return;
  }
  const resourceText = batchDisplayNumber(summary.job_count, 0) + ' Jobs · '
    + batchDisplayNumber(summary.total_workers, 0) + ' Workers · '
    + batchDisplayNumber(summary.total_worker_hours) + ' Worker-hours';
  button.textContent = batchJsonState.submitted
    ? '此 manifest 已提交' : '确认启动 ' + resourceText;
  hint.textContent = batchJsonState.submitted
    ? '已收到服务器回执；请在下方查看队列状态。'
    : '点击后才会创建资源；最大并发 ' + batchDisplayNumber(
      summary.max_active_jobs, 0
    ) + ' 个 Jobs。batch_id ' + String(batchJsonState.manifest?.batch_id || '')
      + ' 是稳定幂等身份。';
}

function mergeBatchPlan(validation, plan) {
  const serverItems = Array.isArray(plan?.items) ? plan.items : [];
  return validation.items.map((localItem, index) => {
    const serverItem = serverItems[index] || {};
    const serverWarnings = batchIssueMessages(serverItem.warnings);
    const serverErrors = batchIssueMessages(serverItem.errors);
    const valid = localItem.valid && serverErrors.length === 0 && serverItem.valid === true;
    return {
      client_id: serverItem.client_id ?? localItem.client_id,
      name: serverItem.name ?? localItem.name,
      valid,
      warnings: localItem.warnings.concat(serverWarnings),
      errors: localItem.errors.concat(serverErrors),
    };
  });
}

function renderBatchPlan(validation, plan=null) {
  const fallback = validation.summary;
  const summary = normalizedBatchSummary(plan?.summary, fallback);
  const items = plan ? mergeBatchPlan(validation, plan) : validation.items;
  const globalErrors = validation.errors.concat(batchIssueMessages(plan?.errors));
  const itemWarningSet = new Set(items.flatMap(item => item.warnings));
  const globalWarnings = validation.warnings.concat(
    batchIssueMessages(plan?.warnings).filter(warning => !itemWarningSet.has(warning))
  );
  batchJsonState.summary = summary;
  renderBatchSummary(summary);
  renderBatchPlanItems(items);
  document.getElementById('batchJsonPlanResult').hidden = false;
  const allItemsValid = items.length === fallback.job_count && items.every(item => item.valid);
  batchJsonState.planValid = Boolean(
    plan && plan.valid === true && globalErrors.length === 0 && allItemsValid
  );
  const warningCount = globalWarnings.length
    + items.reduce((count, item) => count + item.warnings.length, 0);
  if (globalErrors.length) {
    setBatchAlert(globalErrors.join('\\n'), 'error');
  } else if (plan && !batchJsonState.planValid) {
    setBatchAlert('至少一个 Job 未通过预检；未创建任何资源。', 'error');
  } else if (warningCount) {
    setBatchAlert('全部 Job 有效，但有 ' + warningCount
      + ' 条警告。请逐项检查后再确认；当前尚未创建资源。'
      + (globalWarnings.length ? '\\n' + globalWarnings.join('\\n') : ''), 'warning');
  } else if (plan) {
    setBatchAlert('全部 Job 预检通过；当前尚未创建任何资源。', 'success');
  }
  updateBatchConfirmButton();
}

async function planBatchJson() {
  const input = document.getElementById('batchJsonFile');
  const file = input.files[0];
  if (!file) {
    setBatchAlert('请先选择 JSON 文件。', 'error');
    return;
  }
  if (file.size === 0 || file.size > BATCH_JSON_MAX_BYTES) {
    setBatchAlert('文件必须大于 0 bytes 且不超过 2 MiB。', 'error');
    return;
  }
  const generation = ++batchJsonState.generation;
  clearBatchPlan();
  clearBatchReceipt();
  const button = document.getElementById('batchJsonPlanBtn');
  const originalLabel = button.textContent;
  button.disabled = true;
  button.textContent = '正在解析与预检…';
  try {
    const bytes = await file.arrayBuffer();
    if (generation !== batchJsonState.generation) return;
    if (bytes.byteLength > BATCH_JSON_MAX_BYTES) {
      throw new Error('UTF-8 JSON 内容超过 2 MiB 上限。');
    }
    const fileHash = await sha256Hex(bytes);
    let source;
    try {
      source = new TextDecoder('utf-8', {fatal:true}).decode(bytes);
    } catch (_) {
      throw new Error('文件必须是有效 UTF-8 JSON。');
    }
    let manifest;
    try {
      manifest = JSON.parse(source);
    } catch (_) {
      throw new Error('JSON 语法无效；请在本地编辑器修正后重新选择文件。');
    }
    assertNoDuplicateJsonKeys(source);
    if (generation !== batchJsonState.generation) return;
    batchJsonState.manifest = manifest;
    batchJsonState.rawSource = source;
    batchJsonState.fileHash = fileHash;
    document.getElementById('batchJsonFileMeta').textContent =
      file.name + ' · ' + batchDisplayNumber(file.size) + ' bytes · SHA-256 ' + fileHash;
    const validation = validateBatchManifest(manifest);
    renderBatchPlan(validation);
    if (validation.errors.length || validation.items.some(item => !item.valid)) return;
    const plan = await batchJsonApi('POST', '/job-batches/plan', source, {}, true);
    if (generation !== batchJsonState.generation) return;
    batchJsonState.plan = plan;
    batchJsonState.idempotencyKey = await batchIdempotencyKey(manifest.batch_id);
    renderBatchPlan(validation, plan);
  } catch (error) {
    if (generation === batchJsonState.generation) {
      batchJsonState.planValid = false;
      setBatchAlert(error.message || '批量预检失败。', 'error');
      updateBatchConfirmButton();
    }
  } finally {
    if (generation === batchJsonState.generation) {
      button.disabled = false;
      button.textContent = originalLabel;
    }
  }
}

function batchReceiptStateClass(rawState) {
  const state = String(rawState || '').toLowerCase();
  if (['succeeded', 'completed', 'complete', 'terminal'].includes(state)) return 'b-succeeded';
  if (['failed', 'error', 'partial_failure'].includes(state)) return 'b-failed';
  if (['cancelled', 'canceled'].includes(state)) return 'b-cancelled';
  if (['running', 'accepted'].includes(state)) return 'b-running';
  return 'b-pending';
}

function batchReceiptIsTerminal(receipt) {
  if (receipt?.done === true) return true;
  return ['succeeded', 'completed', 'complete', 'terminal', 'failed', 'partial_failure',
          'cancelled', 'canceled'].includes(String(receipt?.state || '').toLowerCase());
}

function renderBatchReceipt(receipt) {
  const section = document.getElementById('batchJsonReceipt');
  section.hidden = false;
  const internalId = String(receipt?.job_batch_id || batchJsonState.jobBatchId || '');
  const externalId = String(receipt?.batch_id || batchJsonState.manifest?.batch_id || '');
  const ids = [];
  if (externalId) ids.push('batch_id: ' + externalId);
  if (internalId) ids.push('job_batch_id: ' + internalId);
  if (receipt?.idempotent_replay === true) ids.push('幂等重放：是');
  document.getElementById('batchJsonReceiptIds').textContent = ids.join(' · ');
  const state = String(receipt?.state || 'accepted');
  const items = Array.isArray(receipt?.items) ? receipt.items : [];
  const failedItemCount = items.filter(item => (
    String(item?.state || '').toLowerCase() === 'error'
    || (
      String(item?.state || '').toLowerCase() === 'terminal'
      && String(item?.job_state || '').toLowerCase() === 'failed'
    )
  )).length;
  const cancelledItemCount = items.filter(item => (
    String(item?.state || '').toLowerCase() === 'terminal'
    && ['cancelled', 'canceled'].includes(
      String(item?.job_state || '').toLowerCase()
    )
  )).length;
  const batchErrorCount = Math.max(
    Number(receipt?.summary?.error || 0), failedItemCount
  );
  const terminalBatch = state.toLowerCase() === 'terminal';
  const visualState = terminalBatch && batchErrorCount > 0
    ? 'error'
    : terminalBatch && cancelledItemCount > 0 ? 'cancelled' : state;
  const stateBadge = document.getElementById('batchJsonReceiptState');
  stateBadge.className = 'badge ' + batchReceiptStateClass(visualState);
  stateBadge.textContent = terminalBatch && batchErrorCount > 0
    ? state + ' · ' + batchDisplayNumber(batchErrorCount, 0) + ' error'
    : terminalBatch && cancelledItemCount > 0
      ? state + ' · ' + batchDisplayNumber(cancelledItemCount, 0) + ' cancelled'
      : state;
  const container = document.getElementById('batchJsonReceiptItems');
  container.replaceChildren(document.createDocumentFragment());
  if (!items.length) {
    const empty = document.createElement('p');
    empty.className = 'muted';
    empty.textContent = '服务器尚未返回逐项回执，页面会继续轮询。';
    container.append(empty);
  }
  for (const item of items) {
    const card = document.createElement('article');
    card.className = 'batch-item';
    const head = document.createElement('div');
    head.className = 'batch-item-head';
    const title = document.createElement('div');
    title.className = 'batch-item-title';
    const client = document.createElement('b');
    client.textContent = String(item?.client_id || '未命名项');
    title.append(client);
    if (item?.job_id) {
      const jobId = document.createElement('div');
      jobId.className = 'muted';
      jobId.textContent = 'Job: ' + String(item.job_id);
      title.append(jobId);
    }
    const itemState = String(item?.state || 'queued');
    const terminalJobState = itemState === 'terminal' && item?.job_state
      ? String(item.job_state) : '';
    const badge = document.createElement('span');
    badge.className = 'badge ' + batchReceiptStateClass(terminalJobState || itemState);
    badge.textContent = terminalJobState
      ? itemState + ' · ' + terminalJobState : itemState;
    head.append(title, badge);
    card.append(head);
    const itemErrors = batchIssueMessages(item?.error || item?.errors);
    if (itemErrors.length) {
      const messages = document.createElement('ul');
      messages.className = 'batch-item-messages';
      appendBatchMessages(messages, itemErrors, 'error');
      card.append(messages);
    }
    container.append(card);
  }
}

async function submitBatchJson() {
  if (!batchJsonState.planValid || !batchJsonState.manifest
      || !batchJsonState.rawSource || !batchJsonState.idempotencyKey
      || !batchJsonState.summary) {
    setBatchAlert('必须先重新解析并通过全部预检。', 'error');
    return;
  }
  const summary = batchJsonState.summary;
  const resourceText = batchDisplayNumber(summary.job_count, 0) + ' Jobs、'
    + batchDisplayNumber(summary.total_workers, 0) + ' Workers、'
    + batchDisplayNumber(summary.total_worker_hours) + ' Worker-hours';
  if (!window.confirm('确认启动 ' + resourceText + '？\\n此操作会创建真实云资源。')) return;
  const generation = batchJsonState.generation;
  const rawSource = batchJsonState.rawSource;
  const idempotencyKey = batchJsonState.idempotencyKey;
  const button = document.getElementById('batchJsonSubmitBtn');
  const planButton = document.getElementById('batchJsonPlanBtn');
  const fileInput = document.getElementById('batchJsonFile');
  button.disabled = true;
  planButton.disabled = true;
  fileInput.disabled = true;
  button.textContent = '正在提交批次…';
  try {
    const receipt = await batchJsonApi('POST', '/job-batches', rawSource, {
      'Idempotency-Key': idempotencyKey,
    }, true);
    const jobBatchId = String(receipt?.job_batch_id || '');
    if (!jobBatchId) throw new Error('服务器回执缺少 job_batch_id，未改变本页幂等键，可安全重试。');
    if (generation !== batchJsonState.generation
        || rawSource !== batchJsonState.rawSource) {
      toast('先前选择的批次已接收：' + jobBatchId, 'warning');
      refreshJobs();
      return;
    }
    batchJsonState.jobBatchId = jobBatchId;
    batchJsonState.batchTerminal = batchReceiptIsTerminal(receipt);
    batchJsonState.submitted = true;
    renderBatchReceipt(receipt);
    updateBatchConfirmButton();
    setBatchAlert('服务器已接收批次；正在轮询逐项 queued / accepted / terminal / error 状态。', 'success');
    toast('批次已接收：' + jobBatchId);
    refreshJobs();
    refreshAccounts(true);
  } catch (error) {
    if (generation !== batchJsonState.generation
        || rawSource !== batchJsonState.rawSource) return;
    const message = Number(error.status) === 409
      ? '同一 batch_id 已经绑定到另一份 manifest。未创建重复批次；若这是新批次，请修改 batch_id 后重新预检。'
      : error.message || '批次提交失败；原幂等键仍保留，可安全重试。';
    setBatchAlert(message, 'error');
  } finally {
    fileInput.disabled = false;
    if (generation === batchJsonState.generation) {
      planButton.disabled = false;
    }
    if (generation === batchJsonState.generation && !batchJsonState.submitted) {
      button.disabled = false;
      updateBatchConfirmButton();
    }
  }
}

async function refreshActiveJobBatch() {
  if (!batchJsonState.jobBatchId || batchJsonState.batchTerminal
      || batchJsonState.refreshRunning) return;
  const requestedId = batchJsonState.jobBatchId;
  const requestedGeneration = batchJsonState.generation;
  batchJsonState.refreshRunning = true;
  try {
    const receipt = await batchJsonApi(
      'GET', '/job-batches/' + encodeURIComponent(requestedId)
    );
    if (batchJsonState.jobBatchId !== requestedId
        || batchJsonState.generation !== requestedGeneration) return;
    renderBatchReceipt(receipt);
    batchJsonState.batchTerminal = batchReceiptIsTerminal(receipt);
    if (batchJsonState.batchTerminal) {
      toast('批次已进入终态：' + String(receipt?.state || 'completed'));
      refreshJobs();
    }
  } catch (error) {
    if (batchJsonState.jobBatchId === requestedId
        && batchJsonState.generation === requestedGeneration) {
      setBatchAlert(
        '批次状态刷新失败，将自动重试：' + safeBatchServerText(error.message),
        'warning'
      );
    }
  } finally {
    if (batchJsonState.jobBatchId === requestedId
        && batchJsonState.generation === requestedGeneration) {
      batchJsonState.refreshRunning = false;
    }
  }
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
function agentApiProviderMeta(provider) {
  if (String(provider || '').toLowerCase() === 'apex') {
    return {
      id: 'apex',
      label: 'ApexRouter',
      accountLabel: 'ApexRouter · API',
      pickerLabel: 'ApexRouter API',
      hint: 'ApexRouter API Key 仅支持 Codex；模型来自固定 /models 接口。'
        + ' 不需要浏览器登录，也不会触发验证码。Key 只写入 Manager 私有账号文件，提交后不回显。',
    };
  }
  return {
    id: 'cloudrouter',
    label: 'CloudRouter',
    accountLabel: 'CloudRouter · API',
    pickerLabel: 'CloudRouter API',
    hint: 'CloudRouter API Key 会按可用模型自动加入 Claude、Codex 或两者的账号池；'
      + ' 不需要浏览器登录，也不会触发验证码。Key 只写入 Manager 私有账号文件，提交后不回显。',
  };
}
function agentApiProviderLabel(provider) {
  return agentApiProviderMeta(provider).accountLabel;
}
function updateAgentApiProviderUI() {
  const provider = document.getElementById('apiAcctProvider').value;
  const meta = agentApiProviderMeta(provider);
  document.getElementById('apiAcctHint').textContent = meta.hint;
  document.getElementById('apiAcctKey').placeholder = `${meta.label} API Key`;
  document.getElementById('apiAcctAdd').textContent = `Add ${meta.label} API`;
}
function accountSupportedAgentTypes(a) {
  const declared = Array.isArray(a.supported_agent_types)
    ? a.supported_agent_types
    : [];
  const supported = declared.length ? declared : (a.agent_type ? [a.agent_type] : []);
  return [...new Set(supported.map(value => String(value).toLowerCase())
    .filter(value => value === 'claude' || value === 'codex'))];
}
function formatAgentApiModels(a) {
  const modelGroups = a.supported_models || a.models || {};
  if (!modelGroups || typeof modelGroups !== 'object') return '—';
  const labels = [];
  for (const agentType of accountSupportedAgentTypes(a)) {
    const values = Array.isArray(modelGroups[agentType])
      ? modelGroups[agentType].map(String)
      : [];
    if (values.length) labels.push(`${agentType}: ${values.join(', ')}`);
  }
  return labels.length ? labels.join(' · ') : '—';
}
function formatAgentApiUsage(usage) {
  if (!usage || typeof usage !== 'object') {
    return '<span class="muted">未检查</span>';
  }
  const state = String(usage.state || usage.status || '');
  if (usage.known === false) {
    const stale = usage.stale ? '（沿用上次状态）' : '';
    return `<span class="muted">暂时未知${stale}</span>`;
  }
  if (usage.available === false) {
    return `<span style="color:var(--red)">不可用 · ${esc(usage.reason || state || '额度耗尽')}</span>`;
  }
  const quota = usage.quota && typeof usage.quota === 'object'
    ? usage.quota
    : null;
  const windows = Array.isArray(usage.windows) ? usage.windows : [];
  const unlimited = usage.remaining_unlimited || usage.balance_unlimited
    || (quota && quota.unlimited) || windows.some(window => window && window.unlimited);
  if (unlimited) return '<span style="color:var(--green)">无限额度</span>';
  const remaining = usage.remaining ?? usage.balance ?? (quota && quota.remaining);
  const limit = quota && quota.limit;
  const unit = usage.currency || usage.unit || '';
  if (remaining !== undefined && remaining !== null) {
    const total = limit !== undefined && limit !== null ? ` / ${esc(limit)}` : '';
    return `剩余 ${esc(remaining)}${total}${unit ? ' ' + esc(unit) : ''}`;
  }
  const primary = windows.find(window => window
    && window.remaining !== undefined && window.remaining !== null);
  if (primary) {
    return `${esc(primary.label || primary.id || '额度')}：剩余 ${esc(primary.remaining)}`
      + `${primary.limit !== undefined ? ' / ' + esc(primary.limit) : ''}`;
  }
  return `<span class="muted">${esc(state || '可用')}</span>`;
}
async function refreshAccountsOnce(requestVersion) {
  try {
    const d = await api('GET', '/accounts');
    const accounts = d.accounts || [];
    let alloc = null;
    let eipBindings = null;
    try {
      alloc = (await api('GET', '/accounts/allocations')).allocations || {};
    } catch(e) {}
    try {
      const response = await api('GET', '/accounts/bindings');
      eipBindings = {};
      (response.bindings || []).forEach(binding => {
        eipBindings[binding.account_id] = binding;
      });
    } catch(e) {}
    if (requestVersion !== accountsRequestVersion) return;
    document.getElementById('acctRows').innerHTML = accounts.map(a => {
      const isAgentApi = a.auth_kind === 'agent_api';
      const supported = accountSupportedAgentTypes(a);
      const b = alloc === null ? null : (alloc[a.id] || []);
      const active = b === null
        ? '<span class="muted">占用状态暂不可用</span>'
        : (b.length
          ? b.map(x => `${esc((x.worker_id||'').replace('aws:',''))} `
            + `<span class="muted">(${esc(x.job_name||x.job_id)}·`
            + `${esc(x.phase)}${x.active?'·当前':''}`
            + `${x.cleanup_pending?'·清理中':''})</span>`).join('<br>')
          : '<span class="muted">空闲</span>');
      const durable = eipBindings === null ? null : eipBindings[a.id];
      const eipValue = durable
        ? durable.eip_ip || durable.eip_allocation_id || '分配中'
        : '';
      const eip = eipBindings === null
        ? '<span class="muted">EIP 状态暂不可用</span>'
        : (durable
          ? `${esc(eipValue)} <span class="muted">(${esc(durable.state)})</span>`
          : '<span class="muted">无 EIP</span>');
      const secrets = isAgentApi
        ? (a.has_api_key ? 'API key' : '—')
        : (`${a.has_password ? 'password' : ''}`
          + `${a.has_password && a.has_email_token ? ' + ' : ''}`
          + `${a.has_email_token ? 'mail token' : ''}` || '—');
      const providerLabel = agentApiProviderLabel(a.api_provider);
      const accountType = isAgentApi
        ? `<b>${esc(providerLabel)}</b><br>${esc(supported.join(' / ') || '—')}`
          + `<br><span class="muted">${esc(formatAgentApiModels(a))}</span>`
        : esc(a.agent_type);
      const identity = a.name || a.email || a.id;
      const quota = isAgentApi
        ? formatAgentApiUsage(a.api_usage)
        : '<span class="muted">OAuth</span>';
      const action = isAgentApi
        ? `<button class="btn btn-ghost" style="margin:0;padding:3px 9px"
            onclick="refreshAgentApiAccount(${jsArg(a.id)})">刷新</button>
           <button class="btn btn-danger" style="margin:3px 0 0;padding:3px 9px"
            onclick="removeAgentApiAccount(${jsArg(a.id)})">删除</button>`
        : `<button class="btn btn-danger" style="margin:0;padding:3px 9px"
            onclick="removeAccount(${jsArg(a.id)})">✕</button>`;
      return `<tr><td>${esc(a.id)}</td><td style="font-size:.72rem">${accountType}</td>
        <td>${esc(identity)}</td><td>${esc(secrets)}</td><td>${esc(a.group)}</td>
        <td>${esc(a.enabled !== false)}</td><td style="font-size:.72rem">${quota}</td>
        <td style="font-size:.72rem">${eip}<br>${active}</td><td>${action}</td></tr>`;
    }).join('') || '<tr><td colspan="9" class="muted">No accounts.</td></tr>';

    const picker = document.getElementById('jAcctIds');
    const selectedAgent = document.getElementById('jAgentType').value;
    const selected = new Set(Array.from(picker.selectedOptions).map(o => o.value));
    picker.replaceChildren();
    accounts.forEach(a => {
      const supported = accountSupportedAgentTypes(a);
      const enabled = a.enabled !== false;
      const isAgentApi = a.auth_kind === 'agent_api';
      const option = document.createElement('option');
      option.value = a.id;
      option.dataset.agentTypes = supported.join(',');
      option.dataset.enabled = String(enabled);
      option.dataset.authKind = a.auth_kind || 'oauth';
      const durable = eipBindings === null ? null : eipBindings[a.id];
      const eipLabel = eipBindings === null
        ? ' · EIP状态未知'
        : (durable
          ? ` · EIP ${durable.eip_ip || durable.eip_allocation_id || durable.state}`
          : '');
      const typeLabel = isAgentApi
        ? `${agentApiProviderMeta(a.api_provider).pickerLabel} · ${supported.join('/')}`
        : supported.join('/');
      option.textContent = `${typeLabel} · ${a.name || a.email || a.id}`
        + ` · ${a.group || 'standard'} (${a.id})${eipLabel}`;
      option.disabled = !enabled || !supported.includes(selectedAgent);
      option.selected = selected.has(a.id) && !option.disabled;
      picker.appendChild(option);
    });
    updateEipBindingUI();
    document.getElementById('accountsRefresh').textContent =
      alloc === null || eipBindings === null
        ? '· 部分状态暂不可用'
        : '· 已更新 ' + new Date().toLocaleTimeString();
  } catch(e) {
    if (requestVersion !== accountsRequestVersion) return;
    document.getElementById('accountsRefresh').textContent =
      '· 刷新失败，保留当前快照';
  }
}
async function runAccountRefreshes() {
  try {
    do {
      accountsRefreshQueued = false;
      const requestVersion = ++accountsRequestVersion;
      lastAccountsRefreshAt = Date.now();
      document.getElementById('accountsRefresh').textContent = '· 刷新中…';
      await refreshAccountsOnce(requestVersion);
    } while (accountsRefreshQueued);
  } finally {
    accountsRefreshInFlight = null;
  }
}
function refreshAccounts(queueAfterCurrent=false) {
  if (accountsRefreshInFlight) {
    if (queueAfterCurrent) accountsRefreshQueued = true;
    return accountsRefreshInFlight;
  }
  accountsRefreshInFlight = runAccountRefreshes();
  return accountsRefreshInFlight;
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
    toast('Account added'); refreshAccounts(true);
  } catch(e) { toast(e.message, 'error'); }
}
async function addAgentApiAccount() {
  const provider = document.getElementById('apiAcctProvider').value;
  const meta = agentApiProviderMeta(provider);
  const name = document.getElementById('apiAcctName').value.trim();
  const group = document.getElementById('apiAcctGroup').value.trim() || 'standard';
  const apiKey = document.getElementById('apiAcctKey').value.trim();
  if (!name || !apiKey) return toast('name + API key required', 'error');
  try {
    await api('POST', '/agent-api/accounts', {
      provider: provider, name: name, group: group, api_key: apiKey
    });
    document.getElementById('apiAcctKey').value = '';
    document.getElementById('apiAcctName').value = '';
    toast(`${meta.label} API account added`);
    await refreshAccounts(true);
  } catch(e) { toast(e.message, 'error'); }
}
async function refreshAgentApiAccount(id) {
  try {
    await api('POST', '/agent-api/accounts/' + encodeURIComponent(id) + '/refresh');
    toast('Agent API models and quota refreshed');
    await refreshAccounts(true);
  } catch(e) { toast(e.message, 'error'); }
}
async function bindingReleaseIsVisible(accountPath) {
  try {
    await api('GET', accountPath + '/binding');
    return false;
  } catch(e) {
    return e.status === 404;
  }
}
async function removeAgentApiAccount(id) {
  const accountPath = '/accounts/' + encodeURIComponent(id);
  let binding = null;
  let releasedEip = '';
  let attemptedEip = '';
  let identityRemoved = false;
  try {
    try {
      binding = await api('GET', accountPath + '/binding');
    } catch(e) {
      if (e.status !== 404) throw e;
    }
    if (binding) {
      const eip = binding.eip_ip || binding.eip_allocation_id || '当前绑定地址';
      if (!window.confirm(
        `Agent API 账号 ${id} 仍保留 EIP ${eip}。继续会永久释放该 IP 并删除 Key。`
      )) return;
      const confirmation = window.prompt(
        `请输入完整账号 ID 以确认永久释放 EIP 并删除 Agent API Key：\\n${id}`
      );
      if (confirmation === null || confirmation.trim() !== id) {
        toast('账号 ID 不匹配；EIP 和 Key 均未删除。', 'error');
        return;
      }
      attemptedEip = eip;
      const retired = await api('POST', accountPath + '/binding/decommission', {
        release_eip: true,
        confirm_account_id: id,
        delete_identity: true,
      });
      releasedEip = eip;
      identityRemoved = retired.identity_removed === true;
    } else if (!window.confirm(
      `删除 Agent API 账号 ${id}？Key 会从 Manager 永久移除，且无法恢复。`
    )) return;
    if (!identityRemoved) {
      await api('DELETE', '/agent-api/accounts/' + encodeURIComponent(id));
    }
    toast(`已删除 Agent API 账号 ${id}`);
    await refreshAccounts(true);
  } catch(e) {
    if (
      binding && !releasedEip && attemptedEip
      && await bindingReleaseIsVisible(accountPath)
    ) {
      releasedEip = attemptedEip;
    }
    if (releasedEip) {
      toast(`EIP ${releasedEip} 已永久释放；Agent API Key 删除状态需刷新确认：`
        + e.message, 'error');
      await refreshAccounts(true);
    } else if (e.status === 409) {
      toast(`Agent API 账号 ${id} 仍有活动任务或清理流程占用，暂不能删除。`
        + ` ${e.message}`, 'error');
      await refreshAccounts(true);
    } else {
      toast(e.message, 'error');
    }
  }
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
  const accountPath = '/accounts/' + encodeURIComponent(id);
  let binding;
  let releasedEip = '';
  let attemptedEip = '';
  let identityRemoved = false;
  try {
    try {
      binding = await api('GET', accountPath + '/binding');
    } catch(e) {
      if (e.status === 404) binding = null;
      else throw e;
    }

    if (binding) {
      const eip = binding.eip_ip || binding.eip_allocation_id || '当前绑定地址';
      const warning = `账号 ${id} 仍保留持久 EIP ${eip}。\n\n`
        + '继续会永久释放这个公网 IP（不可恢复），然后删除账号。'
        + '\\n失败 Job 结束后 EIP 仍会按设计保留；只有这里的明确确认才会释放。';
      if (!window.confirm(warning)) return;
      const confirmation = window.prompt(
        `危险操作：请输入完整账号 ID 以确认永久释放 EIP 并删除账号：\n${id}`
      );
      if (confirmation === null) return;
      if (confirmation.trim() !== id) {
        toast('账号 ID 不匹配；EIP 未释放，账号未删除。', 'error');
        return;
      }
      attemptedEip = eip;
      const retired = await api('POST', accountPath + '/binding/decommission', {
        release_eip: true,
        confirm_account_id: id,
        delete_identity: true,
      });
      releasedEip = eip;
      identityRemoved = retired.identity_removed === true;
    } else if (!window.confirm(`删除账号 ${id}？`)) {
      return;
    }

    if (!identityRemoved) {
      await api('DELETE', accountPath);
    }
    toast(releasedEip
      ? `已永久释放 EIP ${releasedEip} 并删除账号 ${id}`
      : `已删除账号 ${id}`);
    await refreshAccounts(true);
  } catch(e) {
    if (
      binding && !releasedEip && attemptedEip
      && await bindingReleaseIsVisible(accountPath)
    ) {
      releasedEip = attemptedEip;
    }
    if (releasedEip) {
      if (e.status === 404) {
        toast(`已永久释放 EIP ${releasedEip}，账号已删除`);
      } else {
        toast(`EIP ${releasedEip} 已永久释放；账号删除状态需刷新确认：`
          + e.message, 'error');
      }
      await refreshAccounts(true);
    } else if (e.status === 409) {
      toast(`账号 ${id} 仍有任务或清理流程占用，当前未释放 EIP、未删除账号。`
        + ` ${e.message}`, 'error');
      await refreshAccounts(true);
    } else {
      toast(e.message, 'error');
    }
  }
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
  const control = document.getElementById(id);
  const label = id === 'jSecretEnv' ? '秘密环境变量' : '普通环境变量';
  // Environment names such as "__proto__" are valid. A null-prototype map
  // prevents JavaScript object setters from silently dropping such a key.
  const env = Object.create(null);
  const rawLines = control.value.split('\\n');
  for (let lineNumber = 0; lineNumber < rawLines.length; lineNumber += 1) {
    const line = rawLines[lineNumber].trim();
    if (!line) continue;
    const separator = line.indexOf('=');
    if (separator < 1) {
      throw new Error(`${label}第 ${lineNumber + 1} 行必须是 KEY=VALUE。`);
    }
    const key = line.slice(0, separator);
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
      throw new Error(
        `${label}第 ${lineNumber + 1} 行的变量名 ${key || '(空)'} 无效。`
      );
    }
    if (Object.prototype.hasOwnProperty.call(env, key)) {
      throw new Error(`${label}中的变量 ${key} 重复定义。`);
    }
    env[key] = line.slice(separator + 1);
  }
  return env;
}
function buildEnv() { return buildKeyValueLines('jEnv'); }
function buildSecretEnv() { return buildKeyValueLines('jSecretEnv'); }
function buildSelectedAccountIds(workers, perWorker, binding, accountEnabled) {
  if (!accountEnabled) return [];
  const selected = Array.from(
    document.getElementById('jAcctIds').selectedOptions
  );
  if (!selected.length) return [];
  const required = binding === 'eip' ? workers : workers * perWorker;
  if (
    binding !== 'eip'
    && selected.length === 1
    && selected[0].dataset.authKind === 'agent_api'
    && required > 1
  ) {
    return Array(required).fill(selected[0].value);
  }
  if (selected.length === required) {
    return selected.map(option => option.value);
  }
  const modeLabel = binding === 'eip' ? '固定 EIP' : '普通出口';
  throw new Error(
    `${modeLabel}模式需要 ${required} 个账号映射，当前选择 ${selected.length} 个。`
    + ' 只有单个 Agent API Key 可以自动共享到全部 Worker 槽。'
  );
}
function markEipBindingTouched() {
  eipBindingTouched = true;
  updateEipBindingUI();
}
function updateDeliveryUI() {
  const delivery = document.getElementById('jDeliver').value;
  document.getElementById('jDeliverHelp').textContent = delivery === 'manager_rsync'
    ? 'Manager 克隆后移除凭据和 .git，再 rsync 到 Worker；私库推荐，Git token 不会上机。'
    : 'Worker 自行 git clone；只适合 Worker 无需 Manager 凭据即可访问的公开仓库。';
}
function updateSourceUI() {
  const hasRepo = Boolean(document.getElementById('jRepo').value.trim());
  document.getElementById('jRepoRef').disabled = !hasRepo;
  document.getElementById('jResolvedCommit').disabled = !hasRepo;
  document.getElementById('jRepoVersionHelp').textContent = hasRepo
    ? '当前默认是 AI4Sci Bench 的归档分支；其他仓库请改成实际分支或标签。'
      + '填写完整 Commit SHA 可进一步锁定精确版本。'
    : '当前未填写 Repo，分支、标签和 Commit SHA 不会生效；'
      + 'AI4Sci Bench 填写 Repo 后默认走已锁定归档分支。';
}
function ai4sciResumeCommand(command) {
  const value = String(command || '').trim();
  if (
    !/\\bai4sci-bench\\s+(?:run|batch-run|codex-run|codex-replay-run)\\b/
      .test(value)
  ) return '';
  const output = value.match(
    /(?:^|\\s)--output-dir(?:=|\\s+)("[^"]+"|'[^']+'|[^\\s]+)/
  );
  const outputToken = output && output[1];
  const outputDir = outputToken
    && /^(['"]).*\\1$/.test(outputToken)
    ? outputToken.slice(1, -1)
    : outputToken;
  if (!outputDir || /\\{\\{\\s*hostname\\s*\\}\\}/.test(outputDir)) return '';
  if (
    /(?:^|\\s)--resume(?:=|\\s+)("[^"]+"|'[^']+'|(?!-)[^\\s]+)/.test(value)
  ) return value;
  if (/(?:^|\\s)--resume(?=\\s|$)/.test(value)) {
    return value.replace(
      /(^|\\s)--resume(?=\\s|$)/,
      (_match, prefix) => `${prefix}--resume ${outputToken}`
    );
  }
  return value + ' --resume ' + outputToken;
}
function updateResumeCommandSuggestion() {
  if (resumeCommandTouched) return;
  const target = document.getElementById('jRunResumeCommand');
  target.value = ai4sciResumeCommand(
    document.getElementById('jRun').value
  );
  updateCollectUI();
}
function markResumeCommandTouched() {
  resumeCommandTouched = true;
  updateCollectUI();
}
function applyAi4SciRecoveryPreset() {
  const repo = document.getElementById('jRepo').value.trim();
  if (repo && !/Agent-AI4Sci-Bench(?:\\.git)?(?:$|[?#])/i.test(repo)) {
    if (!window.confirm('当前 Repo 看起来不是 Agent-AI4Sci-Bench，仍应用预设？')) {
      return;
    }
  }
  document.getElementById('jShard').value = 'shard_index';
  document.getElementById('jCollectCheckpoint').value = 'true';
  document.getElementById('jCollectInterval').value = '120';
  const collect = document.getElementById('jCollect');
  const paths = lines('jCollect');
  if (!paths.includes('results')) {
    collect.value = [...paths, 'results'].join('\\n');
  }
  resumeCommandTouched = false;
  updateResumeCommandSuggestion();
  updateCollectUI();
  toast('已应用 AI4Sci 可恢复预设；请确认运行命令与续跑命令使用同一 output-dir。');
}
function updateCollectUI() {
  const value = parseInt(document.getElementById('jCollectInterval').value) || 0;
  const checkpoint = document.getElementById('jCollectCheckpoint').value === 'true';
  const hasResumeCommand = Boolean(
    document.getElementById('jRunResumeCommand').value.trim()
  );
  document.getElementById('jCollectIntervalHelp').textContent = value > 0
    ? `运行期间每 ${value} 秒收集一次；下载按钮读取最近一次已完成的快照。`
    : '0 表示只在成功、失败或取消时做最终收集。';
  document.getElementById('jCollectCheckpointHelp').textContent = checkpoint
    ? '每次成功收集都会先校验文件；全部分片齐备后才发布完整 set。'
      + (hasResumeCommand
        ? ' 已配置中断续跑命令，运行中可使用“中断并保存进度”。'
        : ' 未配置 run.resume_command：仍可手动恢复检查点，但不会开放一键中断续跑。')
    : '当前只维护普通结果副本；它可以下载，但不能作为强校验的自动续跑检查点。';
  document.getElementById('jCheckpointRetention').disabled = !checkpoint;
  if (
    checkpoint
    && document.getElementById('jShard').value === 'hostname'
  ) {
    document.getElementById('jShard').value = 'shard_index';
  }
}
function updateRecoveryUI() {
  const policy = document.getElementById('jRecoveryPolicy').value;
  const enabled = policy !== 'none';
  document.getElementById('jRecoveryJob').disabled = !enabled;
  document.getElementById('jRecoveryPaths').disabled = !enabled;
  document.getElementById('jRecoveryGeneration').disabled =
    policy !== 'checkpoint';
  document.getElementById('jRecoveryHelp').textContent = policy === 'checkpoint'
    ? '只接受包含全部稳定 shard index 的不可变 checkpoint set；损坏或缺分片时不会创建新机器。'
    : '不读取先前 Job 的文件。旧 Job 的普通 S3 结果无法证明已删除文件，必须从头重跑。';
}
function updateRotationUI() {
  const rotation = document.getElementById('jRot');
  const resume = document.getElementById('jResume');
  const maxRotations = document.getElementById('jMaxRotations');
  const accountEnabled = document.getElementById('jAcctMode').value !== 'none';
  const enabled = accountEnabled
    && rotation.value === 'on_exhaust_restart_resume';
  resume.disabled = !enabled;
  maxRotations.disabled = !enabled;
  document.getElementById('jRotationHint').textContent = !accountEnabled
    ? '当前未配置 Elastic 托管账号，因此不能自动切换账号。'
    : enabled
      ? '已启用：检测到额度耗尽后会切换账号、重启原命令，并追加上面的续跑参数。'
      : '当前不会自动切换账号；续跑参数和次数不会写入有效执行路径。';
}
async function initializeProviderDefaults() {
  try {
    const health = await api('GET', '/health');
    providerType = health.provider || '';
    updateAccountModeUI();
  } catch(e) {}
}
function updateAccountModeUI() {
  const mode = document.getElementById('jAcctMode').value;
  const workerLocal = mode === 'worker_local_login';
  const binding = document.getElementById('jAcctBinding');
  const rotation = document.getElementById('jRot');
  const accountDisabled = mode === 'none';
  for (const id of [
    'jAcctGroup', 'jAgentModel', 'jConfigDir', 'jPerWorker', 'jLoginTimeout',
  ]) {
    document.getElementById(id).disabled = accountDisabled;
  }
  binding.disabled = !workerLocal;
  rotation.disabled = accountDisabled;
  if (accountDisabled) rotation.value = 'none';
  if (!workerLocal) binding.value = 'none';
  else if (providerType === 'aws' && !eipBindingTouched) binding.value = 'eip';
  const hint = document.getElementById('jAccountStateHint');
  hint.textContent = accountDisabled
    ? '当前不配置 Elastic 托管账号；账号组、模型、登录目录和登录超时均不会生效。'
    : 'Worker 会在任务开始前准备账号凭据：OAuth 账号本地登录，Agent API 账号配置 Key。';
  updateEipBindingUI();
}
function updateEipBindingUI() {
  const mode = document.getElementById('jAcctMode').value;
  const accountDisabled = mode === 'none';
  const workerLocal = mode === 'worker_local_login';
  const enabled = workerLocal
    && document.getElementById('jAcctBinding').value === 'eip';
  const picker = document.getElementById('jAcctIds');
  const rotation = document.getElementById('jRot');
  const perWorker = document.getElementById('jPerWorker');
  picker.disabled = accountDisabled;
  if (accountDisabled) {
    Array.from(picker.options).forEach(option => { option.selected = false; });
  }
  if (enabled) perWorker.value = '1';
  perWorker.disabled = enabled || mode === 'none';
  const restartOption = Array.from(rotation.options)
    .find(o => o.value === 'on_exhaust_restart_resume');
  if (restartOption) restartOption.disabled = enabled || mode === 'none';
  if (enabled && rotation.value === 'on_exhaust_restart_resume') rotation.value = 'none';
  const eipHint = document.getElementById('jEipHint');
  if (enabled) {
    eipHint.textContent = '固定 EIP 已启用：每台临时 EC2 只使用一个账号；指定账号数必须等于 Worker 数。'
      + ' 新 EC2 会重新准备账号凭据：OAuth 账号本地登录，Agent API 账号配置 Key。'
      + ' Job 结束后会销毁 EC2，但保留并继续计费 EIP。';
  } else if (!workerLocal) {
    eipHint.textContent = '当前账号模式不使用固定 EIP；Worker 会使用普通临时公网出口。';
  } else {
    eipHint.textContent = providerType === 'aws'
      ? '当前 AWS Manager 默认建议固定 EIP；你已选择普通临时公网出口。'
      : '启用后可让一个账号固定绑定一个 IPv4 EIP；Job 结束销毁 EC2，但会保留 EIP。';
  }
  updateRotationUI();
}
function updateAgentUI() {
  const agentType = document.getElementById('jAgentType').value;
  const picker = document.getElementById('jAcctIds');
  updateAccountModeUI();
  Array.from(picker.options).forEach(option => {
    option.disabled = option.dataset.enabled !== 'true'
      || !option.dataset.agentTypes.split(',').includes(agentType);
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
function parseS3DatasetLine(line) {
  let inTemplate = false;
  let separator = -1;
  for (let index = 0; index < line.length; index += 1) {
    if (!inTemplate && line.startsWith('{{', index)) {
      inTemplate = true;
      index += 1;
      continue;
    }
    if (inTemplate && line.startsWith('}}', index)) {
      inTemplate = false;
      index += 1;
      continue;
    }
    if (!inTemplate && /\\s/.test(line[index])) {
      separator = index;
      break;
    }
  }
  if (inTemplate || separator < 0) {
    throw new Error('S3 数据集每行必须包含完整 URI 和目标路径。');
  }
  const uri = line.slice(0, separator).trim();
  let index = separator;
  while (index < line.length && /\\s/.test(line[index])) index += 1;
  const dest = line.slice(index).trim();
  if (!uri.startsWith('s3://') || !dest) {
    throw new Error('S3 数据集每行必须是“s3://桶/路径 目标路径”。');
  }
  return {uri, dest};
}
function parseS3Datasets() {
  return lines('jS3').map(parseS3DatasetLine);
}
function validateJobForm() {
  const run = document.getElementById('jRun');
  run.setCustomValidity(
    run.value.trim() ? '' : '请填写运行命令。'
  );
  const setupSteps = document.getElementById('jSetupSteps');
  setupSteps.setCustomValidity('');
  try {
    parseSetupSteps();
  } catch(error) {
    setupSteps.setCustomValidity(
      error instanceof SyntaxError
        ? '结构化初始化步骤必须是有效的 JSON array。'
        : error.message
    );
  }
  const ttl = document.getElementById('jTtl');
  const runTimeout = document.getElementById('jRunTimeout');
  ttl.setCustomValidity(
    Number(ttl.value) < Number(runTimeout.value)
      ? 'Job 总生命周期不能短于运行超时。'
      : ''
  );
  const s3 = document.getElementById('jS3');
  s3.setCustomValidity('');
  try {
    parseS3Datasets();
  } catch(error) {
    s3.setCustomValidity(error.message);
  }
  for (const id of ['jEnv', 'jSecretEnv']) {
    const control = document.getElementById(id);
    control.setCustomValidity('');
    try {
      buildKeyValueLines(id);
    } catch(error) {
      control.setCustomValidity(error.message);
    }
  }
  const checkpoint = document.getElementById('jCollectCheckpoint').value === 'true';
  const collectPaths = lines('jCollect');
  document.getElementById('jCollect').setCustomValidity(
    checkpoint && !collectPaths.length
      ? '开启原子检查点时至少填写一个结果目录。'
      : ''
  );
  const recoveryEnabled =
    document.getElementById('jRecoveryPolicy').value !== 'none';
  const recoveryJob = document.getElementById('jRecoveryJob');
  recoveryJob.setCustomValidity(
    recoveryEnabled && !recoveryJob.value.trim()
      ? '启用恢复时必须填写来源 Job ID。'
      : ''
  );
  const recoveryPaths = document.getElementById('jRecoveryPaths');
  recoveryPaths.setCustomValidity(
    recoveryEnabled && !lines('jRecoveryPaths').length
      ? '启用恢复时至少填写一个恢复目录。'
      : ''
  );
  const controls = document.querySelectorAll(
    '#jobSubmissionCard input, #jobSubmissionCard select, #jobSubmissionCard textarea'
  );
  for (const control of controls) {
    if (!control.disabled && !control.checkValidity()) {
      const details = control.closest('details');
      if (details) details.open = true;
      control.reportValidity();
      control.focus();
      return false;
    }
  }
  return true;
}
function buildJobSpec() {
  const ref = document.getElementById('jHarnessRef').value.trim();
  const workers = parseInt(document.getElementById('jWorkers').value) || 1;
  const accountMode = document.getElementById('jAcctMode').value;
  const accountEnabled = accountMode !== 'none';
  const accountBinding = accountMode === 'worker_local_login'
    ? document.getElementById('jAcctBinding').value
    : 'none';
  const perWorker = accountEnabled
    ? parseInt(document.getElementById('jPerWorker').value) || 1
    : 1;
  const accountIds = buildSelectedAccountIds(
    workers, perWorker, accountBinding, accountEnabled
  );
  const repo = document.getElementById('jRepo').value.trim() || null;
  const setup = {
    repo: repo,
    target_dir: document.getElementById('jTargetDir').value.trim(),
    commands: lines('jSetup'), steps: parseSetupSteps(),
    deliver: document.getElementById('jDeliver').value,
    needs_docker: document.getElementById('jNeedsDocker').value === 'true',
    s3_datasets: parseS3Datasets()
  };
  if (repo) {
    setup.ref = document.getElementById('jRepoRef').value.trim();
    setup.resolved_commit = document.getElementById('jResolvedCommit').value.trim();
  }
  const rotationStrategy = accountEnabled
    ? document.getElementById('jRot').value
    : 'none';
  const rotationEnabled = rotationStrategy === 'on_exhaust_restart_resume';
  const recoveryPolicy = document.getElementById('jRecoveryPolicy').value;
  const recoveryEnabled = recoveryPolicy !== 'none';
  const spec = {
    name: document.getElementById('jName').value.trim() || 'job',
    environment: {profile: document.getElementById('jProfile').value},
    setup: setup,
    run: {command: document.getElementById('jRun').value.trim(),
          resume_command:
            document.getElementById('jRunResumeCommand').value.trim(),
          cwd: document.getElementById('jCwd').value.trim() || '.', env: buildEnv(),
          secret_env: buildSecretEnv(),
          timeout: parseInt(document.getElementById('jRunTimeout').value) || 86400,
          shell: document.getElementById('jShell').value === 'true'},
    ttl_seconds: parseInt(document.getElementById('jTtl').value) || 172800,
    account: {mode: accountMode,
              agent_type: document.getElementById('jAgentType').value,
              model: accountEnabled ? document.getElementById('jAgentModel').value.trim() : '',
              group: accountEnabled
                ? document.getElementById('jAcctGroup').value.trim() || 'standard'
                : 'standard',
              per_worker: perWorker,
              config_dir: accountEnabled
                ? document.getElementById('jConfigDir').value.trim()
                : '',
              login_timeout_seconds: accountEnabled
                ? parseInt(document.getElementById('jLoginTimeout').value) || 900
                : 900,
              binding: accountBinding,
              ids: accountIds},
    rotation: {strategy: rotationStrategy,
               resume_args: rotationEnabled
                 ? document.getElementById('jResume').value.trim()
                 : '',
               max_rotations: rotationEnabled
                 ? parseInt(document.getElementById('jMaxRotations').value) || 0
                 : 0},
    fanout: {workers: workers,
             shard_by: document.getElementById('jShard').value,
             name_prefix: document.getElementById('jNamePrefix').value.trim(),
             instance_type: document.getElementById('jInstanceType').value.trim(),
             region: document.getElementById('jRegion').value.trim(),
             disk_gb: parseInt(document.getElementById('jDiskGb').value) || 0,
             spot: document.getElementById('jSpot').value === 'true'},
    collect: {paths: lines('jCollect'),
              exclude: lines('jCollectExclude'),
              checkpoint: document.getElementById('jCollectCheckpoint').value === 'true',
              checkpoint_keep_generations:
                parseInt(document.getElementById('jCheckpointRetention').value) || 3,
              interval_seconds: parseInt(document.getElementById('jCollectInterval').value) || 0},
    recovery: {policy: recoveryPolicy,
               source_job_id: recoveryEnabled
                 ? document.getElementById('jRecoveryJob').value.trim()
                 : '',
               paths: recoveryEnabled ? lines('jRecoveryPaths') : [],
               generation: recoveryPolicy === 'checkpoint'
                 ? document.getElementById('jRecoveryGeneration').value.trim()
                 : ''},
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
  if (!validateJobForm()) {
    toast('请先修正标出的 Job 配置。', 'error');
    return null;
  }
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
    let pending = window._pendingJobSubmission || loadPendingJobSubmission();
    let currentSpec = null;
    let currentSerialized = null;
    // Build opportunistically only to compare with a refresh-surviving
    // pending submission. Invalid/default form state must not prevent recovery
    // of the exact historical spec and idempotency key.
    try {
      currentSpec = buildJobSpec();
      currentSerialized = JSON.stringify(currentSpec);
    } catch(e) {}
    let retryPending = false;
    let spec;
    if (pending && pending.spec === currentSerialized) {
      spec = parsePendingJobSpec(pending);
      retryPending = true;
    } else if (pending) {
      const retryOriginal = window.confirm(
        '检测到上次提交可能已被服务器接受，但响应在返回前中断。\\n\\n' +
        '点击“确定”将使用原始配置和原 Idempotency-Key 安全重试（推荐）；' +
        '点击“取消”可选择丢弃该待重试记录。'
      );
      if (retryOriginal) {
        spec = parsePendingJobSpec(pending);
        retryPending = true;
      } else {
        const discardPending = window.confirm(
          '确认丢弃上次待重试记录，并按当前表单创建一个全新的 Job？\\n' +
          '丢弃记录不会取消服务器上可能已经创建的原 Job。'
        );
        if (!discardPending) {
          toast('已保留原始待重试记录，未提交新 Job。');
          return;
        }
        clearPendingJobSubmission();
        pending = null;
      }
    }
    if (!retryPending) {
      // Provider discovery may apply defaults (for example AWS EIP binding),
      // so freeze a new JobSpec only after that initialization completes.
      await providerDefaultsReady;
      if (!validateJobForm()) {
        toast('请先修正标出的 Job 配置。', 'error');
        return;
      }
      spec = buildJobSpec();
      currentSerialized = JSON.stringify(spec);
    }
    if (retryPending) {
      // A response may have been lost after the backend accepted this key.
      // Retry the exact submission directly: current capacity/account policy
      // and provider-default discovery must not block the backend's historical
      // idempotent lookup.
      window._pendingJobSubmission = pending;
    } else {
      // A new spec still gets a pure preflight before the key is persisted.
      const plan = await api('POST', '/jobs/plan', spec);
      showJobPlan(plan);
      savePendingJobSubmission({
        spec: currentSerialized,
        key: (crypto.randomUUID ? crypto.randomUUID() : String(Date.now()) + '-' + Math.random())
      });
    }
    const j = await api('POST', '/jobs', spec, {
      'Idempotency-Key': window._pendingJobSubmission.key
    });
    clearPendingJobSubmission();
    toast('Launched ' + j.job_id); refreshJobs(); refreshAccounts(true); }
  catch(e) { toast(e.message, 'error'); }
  finally { if (btn) { btn.disabled = false; btn.textContent = label; } }
}

function recoverySubmissionDefinitivelyRejected(status) {
  const code = Number(status) || 0;
  // A recovery may already have reached durable ``prepared`` even when a
  // later authorization/source/S3 preflight retry returns 4xx. Preserve the
  // accepted key for every uncertain response; only an explicit identity
  // conflict proves that this exact key cannot accept this request.
  return code === 409;
}

async function recoverJob(rawSourceJobId, rawLatestGeneration) {
  const sourceJobId = String(rawSourceJobId || '');
  const pendingKey = 'ea_checkpoint_recovery_pending_v1';
  let pending = null;
  try {
    pending = JSON.parse(sessionStorage.getItem(pendingKey) || 'null');
  } catch(error) {
    sessionStorage.removeItem(pendingKey);
  }
  if (pending && pending.source_job_id !== sourceJobId) {
    toast(
      `来源 ${pending.source_job_id || '未知'} 还有一次响应不确定的恢复提交；`
      + '请先在该 Job 卡片重试，避免意外创建两组 Worker。',
      'error'
    );
    return;
  }
  if (pending && pending.source_job_id === sourceJobId
      && pending.body && pending.idempotency_key) {
    if (!window.confirm(
      '检测到这个来源 Job 有一次响应不确定的恢复提交。\\n\\n'
      + '点击“确定”将使用原请求和 Idempotency-Key 安全重试；'
      + '点击“取消”将保留记录且不创建新 Job。'
    )) {
      if (window.confirm(
        '是否明确丢弃这条待重试记录？\\n\\n'
        + '只有确认后台从未接受该请求时才应丢弃；否则换新 Key 可能重复创建 Worker。'
      )) {
        sessionStorage.removeItem(pendingKey);
        toast('已丢弃恢复待重试记录；未提交新 Job。');
      }
      return;
    }
  } else {
    const suggested = String(rawLatestGeneration || '');
    const generation = window.prompt(
      '恢复哪个完整 checkpoint set？\\n'
      + `当前记录为 ${suggested || '未知'}。留空由服务器选择 S3 中真正最新的完整版本；`
      + '只有需要固定旧版本时才填写 generation。',
      ''
    );
    if (generation === null) return;
    const command = window.prompt(
      '可选：输入新的续跑命令。\\n'
      + '留空会使用来源 Job 的原命令；不要把密钥写入命令。',
      ''
    );
    if (command === null) return;
    const timeoutText = window.prompt(
      '可选：新的运行超时（秒，60–2592000）。留空沿用来源 Job。',
      ''
    );
    if (timeoutText === null) return;
    const ttlText = window.prompt(
      '可选：新的 Job 总生命周期（秒，300–2592000）。'
      + '留空沿用来源 Job，且必须不短于运行超时。',
      ''
    );
    if (ttlText === null) return;
    const parseOptionalInteger = (raw, minimum, label) => {
      const value = String(raw || '').trim();
      if (!value) return null;
      if (!/^\\d+$/.test(value)) throw new Error(label + '必须是整数秒数');
      const parsed = Number(value);
      if (!Number.isSafeInteger(parsed) || parsed < minimum || parsed > 2592000) {
        throw new Error(`${label}必须在 ${minimum}–2592000 秒之间`);
      }
      return parsed;
    };
    let timeout;
    let ttl;
    try {
      timeout = parseOptionalInteger(timeoutText, 60, '运行超时');
      ttl = parseOptionalInteger(ttlText, 300, 'Job 生命周期');
    } catch(error) {
      toast(error.message || String(error), 'error');
      return;
    }
    const body = {
      source_job_id: sourceJobId,
      generation: String(generation).trim(),
      run: {},
    };
    if (String(command).trim()) body.run.command = String(command).trim();
    if (timeout !== null) body.run.timeout = timeout;
    if (ttl !== null) body.ttl_seconds = ttl;
    if (!window.confirm(
      `将从 ${sourceJobId} 的 S3 完整检查点创建一组新 Worker。\\n`
      + '检查点之后尚未上传的工作会重新执行；来源 Job 的私有环境配置'
      + '只在服务器端复制，不会回显到浏览器。\\n\\n确认继续？'
    )) return;
    pending = {
      source_job_id: sourceJobId,
      body,
      idempotency_key: crypto.randomUUID
        ? crypto.randomUUID()
        : String(Date.now()) + '-' + Math.random(),
    };
    try {
      sessionStorage.setItem(pendingKey, JSON.stringify(pending));
    } catch(error) {
      toast('浏览器无法保存恢复用 Idempotency-Key，已取消提交。', 'error');
      return;
    }
  }

  try {
    const recovered = await api(
      'POST',
      '/jobs/recover',
      pending.body,
      {'Idempotency-Key': pending.idempotency_key}
    );
    sessionStorage.removeItem(pendingKey);
    toast('已创建恢复 Job ' + recovered.job_id);
    refreshJobs();
    refreshAccounts(true);
  } catch(error) {
    if (recoverySubmissionDefinitivelyRejected(error?.status)) {
      sessionStorage.removeItem(pendingKey);
      toast('恢复提交被服务器拒绝：' + (error.message || error), 'error');
    } else {
      toast(
        '恢复提交结果不确定：' + (error.message || error)
        + '。再次点击可用同一 Idempotency-Key 安全重试。',
        'error'
      );
    }
  }
}

function newIdempotencyKey() {
  return crypto.randomUUID
    ? crypto.randomUUID()
    : String(Date.now()) + '-' + Math.random();
}

function interruptPendingStorageKey(jobId) {
  return 'ea_job_interrupt_pending_v1_' + String(jobId || '');
}
function hasPendingInterruptRequest(jobId) {
  try {
    return Boolean(
      sessionStorage.getItem(interruptPendingStorageKey(jobId))
    );
  } catch(error) {
    return false;
  }
}
function reconcileInterruptRequestKeys(jobs) {
  for (const job of jobs || []) {
    const state = String(job?.state || '').toLowerCase();
    if (job?.done !== true || !['suspended', 'failed'].includes(state)) continue;
    try {
      sessionStorage.removeItem(interruptPendingStorageKey(job.job_id));
    } catch(error) {
      // A disabled session store already prevents action submission; rendering
      // the durable server state must still continue.
    }
  }
}

async function interruptJob(rawJobId) {
  const jobId = String(rawJobId || '');
  const pendingKey = interruptPendingStorageKey(jobId);
  let idempotencyKey = '';
  try {
    idempotencyKey = sessionStorage.getItem(pendingKey) || '';
  } catch(error) {
    toast('浏览器无法读取中断操作的 Idempotency-Key，已取消操作。', 'error');
    return;
  }
  const retrying = Boolean(idempotencyKey);
  if (!window.confirm(
    retrying
      ? `重试 Job ${jobId} 的同一次中断请求？`
      : `中断 Job ${jobId} 并保存进度？\\n\\n`
        + '系统会停止新工作并中断进程组，确认写入静止后尝试发布完整检查点，'
        + '然后销毁 Worker。提交失败时会回退到上一个完整版本；没有旧版本则不可续跑。'
        + '检查点后未完成的单元会重新执行。'
  )) return;
  if (!idempotencyKey) {
    idempotencyKey = newIdempotencyKey();
    try {
      sessionStorage.setItem(pendingKey, idempotencyKey);
    } catch(error) {
      toast('浏览器无法保存中断操作的 Idempotency-Key，已取消操作。', 'error');
      return;
    }
  }
  try {
    await api(
      'POST',
      '/jobs/' + encodeURIComponent(jobId) + '/interrupt',
      {},
      {'Idempotency-Key': idempotencyKey}
    );
    toast('已开始中断并保存进度：' + jobId + '；重试标识会保留到事务终态。');
    refreshJobs();
    refreshAccounts(true);
  } catch(error) {
    toast(
      '中断请求失败：' + (error.message || error)
      + (Number(error?.status) === 409
        ? ''
        : '。结果不确定时再次点击会使用同一 Idempotency-Key 安全重试。'),
      'error'
    );
  }
}

async function resumeJob(rawSourceJobId, rawResumeGeneration) {
  const sourceJobId = String(rawSourceJobId || '');
  const resumeGeneration = String(rawResumeGeneration || '');
  if (!resumeGeneration) {
    toast('服务器没有提供已校验的中断检查点，不能一键续跑。', 'error');
    return;
  }
  const pendingKey = 'ea_suspended_resume_pending_v1_' + sourceJobId;
  let pending = null;
  try {
    pending = JSON.parse(sessionStorage.getItem(pendingKey) || 'null');
  } catch(error) {
    sessionStorage.removeItem(pendingKey);
  }
  if (
    pending
    && (
      pending.source_job_id !== sourceJobId
      || pending.resume_generation !== resumeGeneration
    )
  ) {
    toast(
      '这个 Job 的可续跑版本已经变化；请先刷新页面并确认新的检查点。',
      'error'
    );
    return;
  }
  if (!pending) {
    if (!window.confirm(
      `从 Job ${sourceJobId} 的中断检查点 ${resumeGeneration} 创建新 attempt？\\n\\n`
      + '已提交的原 Job、日志和配置不会被覆盖；检查点之后尚未发布的工作会重新执行。'
    )) return;
    pending = {
      source_job_id: sourceJobId,
      resume_generation: resumeGeneration,
      idempotency_key: newIdempotencyKey(),
    };
    try {
      sessionStorage.setItem(pendingKey, JSON.stringify(pending));
    } catch(error) {
      toast('浏览器无法保存续跑操作的 Idempotency-Key，已取消操作。', 'error');
      return;
    }
  } else if (!window.confirm(
    '检测到一次响应不确定的续跑提交。使用原 Idempotency-Key 安全重试？'
  )) {
    return;
  }
  try {
    const resumed = await api(
      'POST',
      '/jobs/' + encodeURIComponent(sourceJobId) + '/resume',
      {resume_generation: pending.resume_generation},
      {'Idempotency-Key': pending.idempotency_key}
    );
    sessionStorage.removeItem(pendingKey);
    toast('已创建续跑 Job ' + resumed.job_id);
    refreshJobs();
    refreshAccounts(true);
  } catch(error) {
    if (Number(error?.status) === 409) {
      sessionStorage.removeItem(pendingKey);
    }
    toast(
      '续跑提交失败：' + (error.message || error)
      + (Number(error?.status) === 409
        ? ''
        : '。结果不确定时再次点击会使用同一 Idempotency-Key 安全重试。'),
      'error'
    );
  }
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
    suspending:'正在中断并保存', suspended:'已中断，可续跑',
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
  // Record the one-time idle → active signature transition. Progress paints
  // after this update only mutate the matching buttons in place; on completion
  // the active → idle transition then reliably restores the ordinary action.
  reconcileJobCards(visibleJobs(latestJobs));
  refreshResults();
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
    const response = await authenticatedFetch(
      '/api/jobs/' + encodeURIComponent(jobId) + '/results/download/stream',
      {signal: state.controller.signal}
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
    toast('Job 已取消并进入清理：' + jobId);
    refreshJobs(); refreshAccounts(true);
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
function jobSpecTextFromDetail(detail) {
  const spec = detail && detail.spec;
  if (!spec || typeof spec !== 'object' || Array.isArray(spec)
      || !Object.keys(spec).length) {
    return null;
  }
  return JSON.stringify(spec, null, 2);
}
function setJobSpecState(rawJobId, state) {
  const jobId = String(rawJobId);
  const previous = jobSpecCache.get(jobId);
  if (previous) jobSpecCacheChars -= Number(previous.text_chars) || 0;
  jobSpecCache.delete(jobId);
  const entry = {...state, revision:++jobSpecRevision};
  if (typeof entry.text === 'string') {
    entry.text_chars = entry.text.length;
  } else {
    delete entry.text;
    entry.text_chars = 0;
  }
  jobSpecCache.set(jobId, entry);
  jobSpecCacheChars += entry.text_chars;
  while (jobSpecCache.size > JOB_SPEC_CACHE_MAX_ENTRIES
         || jobSpecCacheChars > JOB_SPEC_CACHE_MAX_CHARS) {
    const oldestJobId = jobSpecCache.keys().next().value;
    const evicted = jobSpecCache.get(oldestJobId);
    jobSpecCacheChars -= Number(evicted?.text_chars) || 0;
    jobSpecCache.delete(oldestJobId);
  }
  return entry;
}
function touchJobSpecState(rawJobId) {
  const jobId = String(rawJobId);
  const cached = jobSpecCache.get(jobId);
  if (!cached) return null;
  jobSpecCache.delete(jobId);
  jobSpecCache.set(jobId, cached);
  return cached;
}
function drainJobSpecRequestQueue() {
  while (jobSpecRequestActive < JOB_SPEC_REQUEST_CONCURRENCY
         && jobSpecRequestQueue.length) {
    const queued = jobSpecRequestQueue.shift();
    jobSpecRequestActive += 1;
    Promise.resolve().then(queued.run).then(
      queued.resolve, queued.reject,
    ).finally(() => {
      jobSpecRequestActive -= 1;
      drainJobSpecRequestQueue();
    });
  }
}
function requestJobSpecDetail(jobId) {
  return new Promise((resolve, reject) => {
    if (jobSpecRequestQueue.length >= JOB_SPEC_REQUEST_QUEUE_MAX) {
      const error = new Error(
        '等待加载的 Job 配置过多，请稍后重试。',
      );
      error.status = 429;
      reject(error);
      return;
    }
    jobSpecRequestQueue.push({
      run:() => api('GET', '/jobs/' + encodeURIComponent(jobId)),
      resolve,
      reject,
    });
    drainJobSpecRequestQueue();
  });
}
function loadJobSpec(rawJobId, force=false) {
  const jobId = String(rawJobId);
  const cached = jobSpecCache.get(jobId);
  if (!force && ['ready','too_large'].includes(cached?.status)) {
    touchJobSpecState(jobId);
    return Promise.resolve(cached);
  }
  const existing = jobSpecRequests.get(jobId);
  if (existing) return existing;

  setJobSpecState(jobId, {status:'loading'});
  reconcileJobCards(visibleJobs(latestJobs));
  const request = requestJobSpecDetail(jobId).then(detail => {
    if (!detail || typeof detail !== 'object' || Array.isArray(detail)) {
      throw new Error('Job 详情响应格式无效');
    }
    if (detail.job_id !== undefined && detail.job_id !== null
        && String(detail.job_id) !== jobId) {
      throw new Error('Job 详情响应与请求不匹配');
    }
    const text = jobSpecTextFromDetail(detail);
    if (text === null) {
      return setJobSpecState(jobId, {
        status:'missing',
        message:'此旧 Job 记录没有可读取的提交配置。',
      });
    }
    if (text.length > JOB_SPEC_TEXT_MAX_CHARS) {
      return setJobSpecState(jobId, {
        status:'too_large',
        message:'提交配置超过页面的安全展示上限，请通过单 Job 详情 API 检查。',
      });
    }
    return setJobSpecState(jobId, {status:'ready', text});
  }).catch(error => {
    const status = Number(error?.status) || null;
    if (status === 404) {
      return setJobSpecState(jobId, {
        status:'missing',
        error_status:status,
        message:'此 Job 已不存在，或旧记录没有可读取的提交配置。',
      });
    }
    return setJobSpecState(jobId, {
      status:'error',
      error_status:status,
      message:String(error?.message || error).replace(/\\s+/g, ' ').slice(0, 300),
    });
  }).finally(() => {
    jobSpecRequests.delete(jobId);
    reconcileJobCards(visibleJobs(latestJobs));
  });
  jobSpecRequests.set(jobId, request);
  return request;
}
function requestJobConfigLoad(config) {
  if (!config.open) config.dataset.jobConfigLoadRequested = 'true';
}
function handleJobConfigToggle(config, rawJobId) {
  const requested = config.dataset.jobConfigLoadRequested === 'true';
  delete config.dataset.jobConfigLoadRequested;
  if (config.open && requested) loadJobSpec(rawJobId);
}
async function copyJobSpec(rawJobId) {
  const jobId = String(rawJobId);
  const cached = touchJobSpecState(jobId);
  if (!cached || cached.status !== 'ready' || typeof cached.text !== 'string') {
    toast('提交配置尚未加载。', 'error');
    return;
  }
  try {
    if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
      await navigator.clipboard.writeText(cached.text);
    } else {
      const textarea = document.createElement('textarea');
      textarea.value = cached.text;
      textarea.setAttribute('readonly', '');
      textarea.style.position = 'fixed';
      textarea.style.opacity = '0';
      document.body.appendChild(textarea);
      textarea.select();
      const copied = document.execCommand('copy');
      textarea.remove();
      if (!copied) throw new Error('浏览器拒绝剪贴板访问');
    }
    toast('已复制脱敏后的提交配置 JSON');
  } catch(error) {
    toast('复制失败：' + (error.message || error), 'error');
  }
}
function jobConfigHtml(job) {
  const jobId = String(job.job_id);
  const cached = jobSpecCache.get(jobId);
  let content = `
    <div class="job-config-message">
      <span>配置仅在你展开本区域后按需加载。</span>
      <button class="btn btn-ghost" data-job-focus="job-config-load"
        onclick="loadJobSpec(${jsArg(jobId)},true)">加载配置</button>
    </div>`;
  if (cached?.status === 'loading') {
    content = `<div class="job-config-message" role="status">
      <span>正在加载脱敏后的提交配置…</span>
    </div>`;
  } else if (cached?.status === 'ready') {
    content = `
      <div class="job-config-toolbar">
        <p class="job-config-note">
          <code>[REDACTED]</code> / <code>[SECRET_REFERENCE]</code> 是脱敏占位符，
          不是真实值，不能直接复制重提；命令文本会原样显示，请勿把密钥直接写进命令。
          普通重提请使用服务端 resubmit；有完整检查点时请使用 Job 卡片的“从检查点恢复”，
          由服务器复制未回显的私有配置。
        </p>
        <button class="btn btn-ghost" data-job-focus="job-config-copy"
          onclick="copyJobSpec(${jsArg(jobId)})">复制 JSON</button>
      </div>
      <pre class="job-config-json" data-job-focus="job-config-json"
           tabindex="0" aria-label="脱敏后的 Job 提交配置 JSON"></pre>`;
  } else if (cached?.status === 'missing') {
    content = `<div class="job-config-message" role="status">
      <span>${esc(cached.message || '此 Job 没有可读取的提交配置。')}</span>
      <button class="btn btn-ghost" data-job-focus="job-config-retry"
        onclick="loadJobSpec(${jsArg(jobId)},true)">重试</button>
    </div>`;
  } else if (cached?.status === 'too_large') {
    content = `<div class="job-config-message" role="status">
      <span>${esc(cached.message || '提交配置过大，无法在页面安全展示。')}</span>
    </div>`;
  } else if (cached?.status === 'error') {
    content = `<div class="job-config-message" role="alert">
      <span>加载失败：${esc(cached.message || '暂时不可用')}</span>
      <button class="btn btn-ghost" data-job-focus="job-config-retry"
        onclick="loadJobSpec(${jsArg(jobId)},true)">重试</button>
    </div>`;
  }
  return `<details class="job-config" data-job-config=""
      ontoggle="handleJobConfigToggle(this,${jsArg(jobId)})">
    <summary data-job-focus="job-config-summary"
      onclick="requestJobConfigLoad(this.parentElement)">提交时生效配置（已脱敏）</summary>
    <div class="job-config-body">${content}</div>
  </details>`;
}
function hydrateJobConfigNode(node, rawJobId) {
  const cached = jobSpecCache.get(String(rawJobId));
  const json = node.querySelector('.job-config-json');
  if (json && cached?.status === 'ready' && typeof cached.text === 'string') {
    json.textContent = cached.text;
  }
}
function captureJobConfigUiState(node) {
  const config = node.querySelector('[data-job-config]');
  const json = node.querySelector('.job-config-json');
  return {
    open:Boolean(config?.open),
    scrollTop:Number(json?.scrollTop) || 0,
    scrollLeft:Number(json?.scrollLeft) || 0,
  };
}
function restoreJobConfigUiState(node, state) {
  if (!state) return;
  const config = node.querySelector('[data-job-config]');
  const json = node.querySelector('.job-config-json');
  if (config) config.open = state.open;
  if (json) {
    json.scrollTop = state.scrollTop;
    json.scrollLeft = state.scrollLeft;
  }
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
  const cancelBtn = !j.done && j.in_memory !== false && state !== 'suspending'
    ? `<button class="btn btn-danger" data-job-focus="job-cancel"
        onclick="cancelJob(${jsArg(j.job_id)})">取消 Job</button>`
    : '';
  const interruptRetry = state === 'suspending'
    && hasPendingInterruptRequest(j.job_id);
  const interruptBtn = (
      j.interrupt_available === true || interruptRetry
    ) && !j.done && j.in_memory !== false
    ? `<button class="btn btn-ghost" data-job-focus="job-interrupt"
        title="${interruptRetry
          ? '使用已保存的 Idempotency-Key 查询或重试同一次中断'
          : '优雅停止任务，等待写入静止，发布完整检查点后销毁 Worker'}"
        onclick="interruptJob(${jsArg(j.job_id)})">${interruptRetry
          ? '↻ 重试同一次中断'
          : '⏸ 中断并保存进度'}</button>`
    : '';
  const resumeAvailable = j.resume_available === true
    && Boolean(j.resume_generation);
  const resumeBtn = resumeAvailable
    ? `<button class="btn" data-job-focus="job-resume"
        title="从已校验的中断检查点创建一个新的 Job attempt"
        onclick="resumeJob(${jsArg(j.job_id)},${jsArg(j.resume_generation)})">▶ 一键续跑</button>`
    : '';
  const manualRecoveryAvailable = j.checkpoint_recovery_available === true
    && j.done
    && !resumeAvailable
    && ['failed','cancelled','succeeded'].includes(state);
  const recoveryBtn = manualRecoveryAvailable
    ? `<button class="btn btn-ghost" data-job-focus="job-recover"
        title="服务端复制私有原始配置，并从完整 S3 检查点创建新 Job"
        onclick="recoverJob(${jsArg(j.job_id)},${jsArg(j.latest_checkpoint_generation || '')})">↻ 手动检查点恢复</button>`
    : '';
  const errors = [...new Set([
    j.error, j.note, j.cancel_reason, j.suspend_warning,
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
  const attemptNo = Math.max(1, Number(j.attempt_no) || 1);
  const resumedFrom = j.resumed_from_job_id || j.source_job_id || '';
  const lineage = resumedFrom
    ? ` · attempt ${attemptNo} · 续跑自 ${esc(resumedFrom)}`
    : ` · attempt ${attemptNo}`;
  return `
  <details id="jobrow-${esc(j.job_id)}" class="job-row job-${esc(state)}"
      data-job-id="${esc(j.job_id)}">
    <summary class="job-summary" data-job-focus="job-summary">
      <span class="job-summary-main">
        <span class="job-summary-title"><b>${esc(j.name||'')}</b> ${badge(state)}
          <span class="muted">${esc(j.job_id)}</span>
          <span class="job-otp-summary-badge" hidden></span></span>
        <span class="job-summary-meta muted" style="margin-top:5px">${phases || jobStateLabel(state)}
          ${created ? ` · 提交 ${esc(created)}` : ''}
          ${lineage}
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
          ${dlBtn}${resumeBtn}${recoveryBtn}${interruptBtn}${cancelBtn}
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
      ${j.latest_checkpoint_generation
        ? `<div class="muted" style="font-size:.72rem">最新完整 checkpoint set: ${esc(j.latest_checkpoint_generation)}</div>`
        : ''}
      ${j.resume_available === true && j.resume_generation
        ? `<div class="muted" style="font-size:.72rem">已校验续跑 checkpoint: ${esc(j.resume_generation)}</div>`
        : ''}
      ${jobConfigHtml(j)}
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
  const specRevision = Number(jobSpecCache.get(jobId)?.revision) || 0;
  return JSON.stringify([job, result || null, resultUiState, specRevision]);
}
function makeJobNode(job) {
  const template = document.createElement('template');
  const result = resultFor(job.job_id);
  template.innerHTML = jobRowHtml(job, result).trim();
  const node = template.content.firstElementChild;
  hydrateJobConfigNode(node, job.job_id);
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
      const configUiState = captureJobConfigUiState(node);
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
      restoreJobConfigUiState(replacement, configUiState);
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
  jobResultsCache.set(jobId, {
    value: preserveKnown ? previous.value : value,
    // A terminal empty response may be a final-collect visibility gap. Keep
    // the known intermediate snapshot downloadable, but continue bounded
    // polling until one successful non-empty terminal read can be frozen.
    nextCheck: nextResultCheck(job, incomingFileCount, previous),
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
  jobResultsCache.set(jobId, {
    ...previous,
    // Never let one terminal 404/5xx freeze an older intermediate snapshot.
    // Preserve its value while retrying with the same bounded backoff.
    nextCheck: nextResultCheck(job, 0, previous),
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
    reconcileInterruptRequestKeys(latestJobs);
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
    const refreshes = [refreshJobs(), refreshLoginAttempts()];
    if (batchJsonState.jobBatchId && !batchJsonState.batchTerminal) {
      refreshes.push(refreshActiveJobBatch());
    }
    if (Date.now() - lastAccountsRefreshAt >= 15_000) {
      refreshes.push(refreshAccounts());
    }
    await Promise.allSettled(refreshes);
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
updateDeliveryUI(); updateSourceUI(); updateCollectUI(); updateRecoveryUI();
updateRotationUI();
updateEipBindingUI(); updateAgentUI(); updateAgentApiProviderUI();
providerDefaultsReady = initializeProviderDefaults();
refreshAccounts(); refreshResults(); runDashboardPoll();
</script>
</body>
</html>
"""


_LOGIN_HTML = """\
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>登录 · Elastic-Agent</title>
<style>
  :root { color-scheme:light; --bg:#eef4ff; --surface:#fff; --border:#d7dee9;
    --text:#172033; --muted:#5b6678; --accent:#2563eb; --red:#c62828; }
  * { box-sizing:border-box; }
  body { margin:0; min-height:100vh; display:grid; place-items:center; padding:20px;
    background:linear-gradient(145deg,var(--bg),#f8fafc); color:var(--text);
    font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; }
  main { width:min(100%,420px); background:var(--surface); border:1px solid var(--border);
    border-radius:14px; padding:28px; box-shadow:0 20px 55px rgba(36,49,73,.12); }
  h1 { margin:0 0 6px; font-size:1.55rem; }
  p { margin:0 0 22px; color:var(--muted); line-height:1.5; }
  label { display:block; margin:12px 0 6px; font-size:.88rem; font-weight:600; }
  input { width:100%; min-height:44px; border:1px solid var(--border); border-radius:8px;
    padding:9px 11px; color:var(--text); background:#fff; font:inherit; }
  input:focus { outline:3px solid rgba(37,99,235,.18); border-color:var(--accent); }
  button { width:100%; min-height:44px; margin-top:20px; border:0; border-radius:8px;
    background:var(--accent); color:#fff; cursor:pointer; font-family:inherit;
    font-size:.95rem; font-weight:600; }
  button:disabled { opacity:.58; cursor:not-allowed; }
  #message { min-height:1.4em; margin:14px 0 0; color:var(--red); font-size:.86rem; }
</style>
</head>
<body>
<main>
  <h1>Elastic-Agent</h1>
  <p>使用管理员账号登录控制台。</p>
  <form id="loginForm" method="post" action="/login">
    <input name="next" type="hidden" value="__LOGIN_NEXT__">
    <label for="email">邮箱</label>
    <input id="email" name="email" type="email" autocomplete="username"
           maxlength="254" placeholder="name@example.com" required autofocus>
    <label for="password">密码</label>
    <input id="password" name="password" type="password"
           autocomplete="current-password" maxlength="4096" required>
    <button id="submitButton" type="submit">登录</button>
    <div id="message" role="alert" aria-live="polite">__LOGIN_MESSAGE__</div>
  </form>
</main>
<script>
try { sessionStorage.removeItem('ea_api_key'); } catch (_) {}
{ const legacyUrl = new URL(window.location.href);
  if (legacyUrl.searchParams.has('api_key')) {
    legacyUrl.searchParams.delete('api_key');
    history.replaceState(null, '', legacyUrl.pathname + legacyUrl.search + legacyUrl.hash);
  }
}
</script>
</body>
</html>
"""


_CHANGE_PASSWORD_HTML = """\
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>修改密码 · Elastic-Agent</title>
<style>
  :root { color-scheme:light; --bg:#eef4ff; --surface:#fff; --border:#d7dee9;
    --text:#172033; --muted:#5b6678; --accent:#2563eb; --red:#c62828;
    --green:#16803c; }
  * { box-sizing:border-box; }
  body { margin:0; min-height:100vh; display:grid; place-items:center; padding:20px;
    background:linear-gradient(145deg,var(--bg),#f8fafc); color:var(--text);
    font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; }
  main { width:min(100%,440px); background:var(--surface); border:1px solid var(--border);
    border-radius:14px; padding:28px; box-shadow:0 20px 55px rgba(36,49,73,.12); }
  h1 { margin:0 0 6px; font-size:1.45rem; }
  p { margin:0 0 20px; color:var(--muted); line-height:1.5; }
  label { display:block; margin:12px 0 6px; font-size:.88rem; font-weight:600; }
  input { width:100%; min-height:44px; border:1px solid var(--border); border-radius:8px;
    padding:9px 11px; color:var(--text); background:#fff; font:inherit; }
  input:focus { outline:3px solid rgba(37,99,235,.18); border-color:var(--accent); }
  button { width:100%; min-height:44px; margin-top:20px; border:0; border-radius:8px;
    background:var(--accent); color:#fff; cursor:pointer; font-family:inherit;
    font-size:.95rem; font-weight:600; }
  button:disabled { opacity:.58; cursor:not-allowed; }
  #message { min-height:1.4em; margin:14px 0 0; color:var(--red); font-size:.86rem; }
  #accountEmail { font-weight:600; color:var(--text); }
</style>
</head>
<body>
<main>
  <h1>修改密码</h1>
  <p>当前账号：<span id="accountEmail">--</span><br>首次登录必须先更换初始密码。</p>
  <form id="passwordForm">
    <label for="currentPassword">当前密码</label>
    <input id="currentPassword" type="password" autocomplete="current-password"
           maxlength="4096" required autofocus>
    <label for="newPassword">新密码</label>
    <input id="newPassword" type="password" autocomplete="new-password"
           minlength="12" maxlength="4096" required>
    <label for="confirmPassword">确认新密码</label>
    <input id="confirmPassword" type="password" autocomplete="new-password"
           minlength="12" maxlength="4096" required>
    <button id="submitButton" type="submit">保存新密码</button>
    <div id="message" role="alert" aria-live="polite"></div>
  </form>
</main>
<script>
try { sessionStorage.removeItem('ea_api_key'); } catch (_) {}
{ const legacyUrl = new URL(window.location.href);
  if (legacyUrl.searchParams.has('api_key')) {
    legacyUrl.searchParams.delete('api_key');
    history.replaceState(null, '', legacyUrl.pathname + legacyUrl.search + legacyUrl.hash);
  }
}
const DEFAULT_PASSWORD_NEXT = '/ui-v2/overview';
const PASSWORD_NEXT_PATHS = new Set([
  '/', '/batch', '/fleet', '/dashboard',
  '/ui-v2', '/ui-v2/', '/ui-v2/overview', '/ui-v2/accounts',
  '/ui-v2/accounts/new', '/ui-v2/jobs/new', '/ui-v2/jobs/batch',
  '/ui-v2/jobs', '/ui-v2/results', '/ui-v2/fleet',
]);
const PASSWORD_JOB_DETAIL = /^[/]ui-v2[/]jobs[/][A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
let csrfToken = '';
function safeNextPath() {
  const candidate = new URLSearchParams(window.location.search).get('next') ||
    DEFAULT_PASSWORD_NEXT;
  return PASSWORD_NEXT_PATHS.has(candidate) || PASSWORD_JOB_DETAIL.test(candidate)
    ? candidate : DEFAULT_PASSWORD_NEXT;
}
function loginRedirect() {
  window.location.assign('/login?next=' + encodeURIComponent('/change-password'));
}
async function initializeAuthentication() {
  const response = await fetch('/api/auth/me', {
    credentials:'same-origin', headers:{'Accept':'application/json'},
  });
  if (response.status === 401) {
    loginRedirect();
    throw new Error('登录已失效');
  }
  if (!response.ok) throw new Error(`${response.status}: ${await response.text()}`);
  const session = await response.json();
  csrfToken = String(session.csrf_token || '');
  document.getElementById('accountEmail').textContent = session.email || '';
  return session;
}
const authenticationReady = initializeAuthentication();
async function authenticatedFetch(input, init={}) {
  await authenticationReady;
  const options = {...init, credentials:'same-origin'};
  const method = String(options.method || 'GET').toUpperCase();
  const requestHeaders = new Headers(options.headers || {});
  if (!['GET', 'HEAD', 'OPTIONS'].includes(method)) {
    requestHeaders.set('X-CSRF-Token', csrfToken);
  }
  options.headers = requestHeaders;
  const response = await fetch(input, options);
  if (response.status === 401) loginRedirect();
  return response;
}
function errorDetail(payload, fallback) {
  return typeof payload?.detail === 'string' ? payload.detail : fallback;
}
document.getElementById('passwordForm').addEventListener('submit', async event => {
  event.preventDefault();
  const button = document.getElementById('submitButton');
  const message = document.getElementById('message');
  const current = document.getElementById('currentPassword');
  const next = document.getElementById('newPassword');
  const confirm = document.getElementById('confirmPassword');
  message.textContent = '';
  if (next.value !== confirm.value) {
    message.textContent = '两次输入的新密码不一致';
    return;
  }
  if (next.value === current.value) {
    message.textContent = '新密码不能与当前密码相同';
    return;
  }
  button.disabled = true;
  try {
    const response = await authenticatedFetch('/api/auth/password', {
      method:'POST', headers:{'Content-Type':'application/json', 'Accept':'application/json'},
      body:JSON.stringify({current_password:current.value, new_password:next.value}),
    });
    let payload = {};
    try { payload = await response.json(); } catch (_) {}
    current.value = ''; next.value = ''; confirm.value = '';
    if (!response.ok) {
      throw new Error(errorDetail(payload, '修改密码失败'));
    }
    window.location.assign(safeNextPath());
  } catch (error) {
    current.value = ''; next.value = ''; confirm.value = '';
    message.textContent = error.message || '修改密码失败，请重试';
  } finally {
    button.disabled = false;
  }
});
</script>
</body>
</html>
"""


_SAFE_UI_NEXT_PATHS = frozenset(
    {
        "/",
        "/batch",
        "/fleet",
        "/dashboard",
        "/change-password",
        "/ui-v2",
        "/ui-v2/",
        "/ui-v2/overview",
        "/ui-v2/accounts",
        "/ui-v2/accounts/new",
        "/ui-v2/jobs/new",
        "/ui-v2/jobs/batch",
        "/ui-v2/jobs",
        "/ui-v2/results",
        "/ui-v2/fleet",
    }
)
_SAFE_UI_V2_JOB_DETAIL = re.compile(
    r"/ui-v2/jobs/[A-Za-z0-9][A-Za-z0-9._-]{0,127}"
)
_LOGIN_ERROR_MESSAGES = {
    "invalid_credentials": "邮箱或密码错误",
    "invalid_request": "请填写有效的邮箱和密码",
    "rate_limited": "登录尝试过多，请稍后重试",
    "unavailable": "管理员认证暂不可用，请稍后重试",
}
# A 4,096-character password can expand to roughly 48 KiB when non-ASCII UTF-8
# bytes are percent-encoded by application/x-www-form-urlencoded.
_MAX_LOGIN_FORM_BYTES = 64 * 1024
_DEFAULT_UI_NEXT = "/ui-v2/overview"


def _safe_ui_next(
    raw_next: str | None,
    *,
    default: str = _DEFAULT_UI_NEXT,
) -> str:
    """Return only a known local UI path so redirects cannot leave this origin."""
    if raw_next in _SAFE_UI_NEXT_PATHS:
        return raw_next
    if (
        isinstance(raw_next, str)
        and _SAFE_UI_V2_JOB_DETAIL.fullmatch(raw_next) is not None
    ):
        return raw_next
    return default


def _principal_requires_password_change(principal: object) -> bool:
    if isinstance(principal, dict):
        return principal.get("must_change_password") is True
    return getattr(principal, "must_change_password", False) is True


def _render_login_html(next_path: str, error_code: str | None = None) -> str:
    """Render only allowlisted values into the otherwise static login page."""

    safe_next = _safe_ui_next(next_path)
    message = _LOGIN_ERROR_MESSAGES.get(error_code or "", "")
    return _LOGIN_HTML.replace("__LOGIN_NEXT__", escape(safe_next, quote=True)).replace(
        "__LOGIN_MESSAGE__",
        escape(message),
    )


async def _parse_login_form(request: Request) -> tuple[LoginRequest, str]:
    content_type = (
        request.headers.get("content-type", "").partition(";")[0].strip().lower()
    )
    if content_type != "application/x-www-form-urlencoded":
        raise HTTPException(
            status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            "application/x-www-form-urlencoded required",
        )

    declared_size = request.headers.get("content-length")
    if declared_size:
        try:
            parsed_size = int(declared_size)
            if parsed_size < 0 or parsed_size > _MAX_LOGIN_FORM_BYTES:
                raise ValueError("login form is too large")
        except ValueError as exc:
            raise ValueError("invalid login form size") from exc

    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > _MAX_LOGIN_FORM_BYTES:
            raise ValueError("login form is too large")
        body.extend(chunk)
    try:
        fields = parse_qs(
            body.decode("utf-8"),
            keep_blank_values=True,
            strict_parsing=True,
            max_num_fields=3,
        )
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError("invalid login form") from exc
    if set(fields) - {"email", "password", "next"} or any(
        len(values) != 1 for values in fields.values()
    ):
        raise ValueError("invalid login form fields")

    next_path = _safe_ui_next((fields.get("next") or [None])[0])
    incoming = LoginRequest.model_validate(
        {
            "email": (fields.get("email") or [""])[0],
            "password": (fields.get("password") or [""])[0],
        }
    )
    return incoming, next_path


def _redirect(
    path: str,
    *,
    next_path: str | None = None,
    error_code: str | None = None,
) -> RedirectResponse:
    location = path
    query: dict[str, str] = {}
    if next_path is not None:
        query["next"] = _safe_ui_next(next_path)
    if error_code is not None:
        query["error"] = error_code
    if query:
        location += "?" + urlencode(query)
    return RedirectResponse(location, status_code=303, headers=_UI_SECURITY_HEADERS)


def _html(content: str) -> HTMLResponse:
    return HTMLResponse(content=content, headers=_UI_SECURITY_HEADERS)


def _login_html(content: str) -> HTMLResponse:
    # Chromium serializes a native form's Origin as ``null`` when its source
    # page uses ``no-referrer``.  ``same-origin`` keeps cross-site referrers
    # suppressed while preserving the exact Origin required for login CSRF.
    headers = {**_UI_SECURITY_HEADERS, "Referrer-Policy": "same-origin"}
    return HTMLResponse(content=content, headers=headers)


async def _authenticated_ui_redirect(
    request: Request,
) -> RedirectResponse | None:
    """Redirect anonymous and first-login sessions before rendering app HTML."""
    next_path = (
        _DEFAULT_UI_NEXT if request.url.path == "/" else request.url.path
    )
    principal = await get_session_principal(request)
    if principal is None:
        return _redirect("/login", next_path=next_path)
    if _principal_requires_password_change(principal):
        return _redirect("/change-password", next_path=next_path)
    return None


@router.get("/login", response_class=HTMLResponse, include_in_schema=False)
async def login_page(request: Request):
    """Render the public account login form, or leave if already signed in."""
    next_path = _safe_ui_next(request.query_params.get("next"))
    principal = await get_session_principal(request)
    if principal is not None:
        if _principal_requires_password_change(principal):
            destination = (
                _DEFAULT_UI_NEXT
                if next_path == "/change-password"
                else next_path
            )
            return _redirect("/change-password", next_path=destination)
        return _redirect(next_path)
    return _login_html(
        _render_login_html(next_path, request.query_params.get("error"))
    )


@router.post("/login", include_in_schema=False)
async def browser_login(request: Request):
    """Authenticate through native browser navigation and a server-side 303."""

    require_same_origin(request)
    try:
        incoming, next_path = await _parse_login_form(request)
    except (ValidationError, ValueError):
        return _redirect(
            "/login",
            next_path=_DEFAULT_UI_NEXT,
            error_code="invalid_request",
        )

    response = _redirect(_DEFAULT_UI_NEXT)
    try:
        principal = await create_browser_session(incoming, request, response)
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            raise
        if exc.status_code == status.HTTP_401_UNAUTHORIZED:
            error_code = "invalid_credentials"
        elif exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
            error_code = "rate_limited"
        else:
            error_code = "unavailable"
        failed = _redirect(
            "/login",
            next_path=next_path,
            error_code=error_code,
        )
        if exc.headers and "Retry-After" in exc.headers:
            failed.headers["Retry-After"] = exc.headers["Retry-After"]
        return failed

    if _principal_requires_password_change(principal):
        destination = (
            _DEFAULT_UI_NEXT if next_path == "/change-password" else next_path
        )
        response.headers["Location"] = "/change-password?" + urlencode(
            {"next": destination}
        )
    else:
        response.headers["Location"] = next_path
    return response


@router.get("/change-password", response_class=HTMLResponse, include_in_schema=False)
async def change_password_page(request: Request):
    """Render the password-change form for an authenticated account."""
    principal = await get_session_principal(request)
    if principal is None:
        return _redirect("/login", next_path="/change-password")
    return _html(_CHANGE_PASSWORD_HTML)


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root_batch(request: Request):
    """Serve the authenticated legacy Batch Console rollback surface."""
    if redirect := await _authenticated_ui_redirect(request):
        return redirect
    return _html(_BATCH_HTML)


@router.get("/batch", response_class=HTMLResponse, include_in_schema=False)
async def batch_console(request: Request):
    """Alias for the authenticated legacy Batch Console."""
    if redirect := await _authenticated_ui_redirect(request):
        return redirect
    return _html(_BATCH_HTML)


@router.get("/fleet", response_class=HTMLResponse, include_in_schema=False)
async def fleet_dashboard(request: Request):
    """Serve the authenticated legacy Fleet Dashboard."""
    if redirect := await _authenticated_ui_redirect(request):
        return redirect
    return _html(_DASHBOARD_HTML)


@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_alt(request: Request):
    """Alias for the Fleet Dashboard."""
    if redirect := await _authenticated_ui_redirect(request):
        return redirect
    return _html(_DASHBOARD_HTML)
