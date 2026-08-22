import { el, clear, reconcileList, formatTime } from '../core/dom.js';
import { get, post, del, createGeneration } from '../core/api.js';
import { createPoller } from '../core/poller.js';
import { pageCache, getState } from '../core/store.js';
import { nodeStateBadge, boolBadge } from '../components/status-badge.js';
import { confirmDialog, showDialog } from '../components/dialog.js';
import { toastError, toastSuccess } from '../components/toast.js';
import { describeError } from '../core/errors.js';

const PAGE_SIZE = 100;
const STATUSES = ['', 'pending', 'running', 'draining', 'terminated', 'error'];

export function createPage({ container }) {
  const cache = pageCache('fleet');
  if (!cache.filter) cache.filter = { status: '', offset: 0 };
  const generation = createGeneration();
  const nodes = {};
  const busy = new Set();
  let poller = null;

  function mount() {
    container.appendChild(buildLayout(nodes, cache, resetToFirstPage, refresh));
    poller = createPoller({
      name: 'fleet',
      interval: 5000,
      task: (signal) => load(signal),
    });
    poller.start();
  }

  /** Re-fetch the page currently in view. */
  function refresh() {
    if (poller) poller.refresh();
  }

  /** A filter change invalidates the offset — restart from the first page. */
  function resetToFirstPage() {
    cache.filter.offset = 0;
    refresh();
  }

  const reload = refresh;

  async function load(signal) {
    const token = generation.next();
    const query = { limit: PAGE_SIZE, offset: cache.filter.offset };
    if (cache.filter.status) query.status = cache.filter.status;
    let data;
    try {
      data = await get('/nodes', { query, signal });
    } catch (error) {
      if (!token.valid) return;
      nodes.status.textContent = describeError(error);
      throw error;
    }
    if (!token.valid) return;
    cache.data = data;
    render(data);
  }

  function render(data) {
    const list = Array.isArray(data.nodes) ? data.nodes : [];
    // Global counts come from the summary API — the current page is not a
    // representative sample once the registry exceeds one page.
    const summary = getState().summary;
    const total = (summary && summary.workers && summary.workers.total);
    nodes.status.textContent = total === null || total === undefined
      ? `本页 ${list.length} 台（第 ${cache.filter.offset / PAGE_SIZE + 1} 页）`
      : `本页 ${list.length} 台 · 全局 ${total} 台（第 ${cache.filter.offset / PAGE_SIZE + 1} 页）`;

    nodes.empty.hidden = list.length > 0;
    reconcileList(nodes.tbody, list, (n) => n.node_id, (n) => buildRow(n), (row, n) => updateRow(row, n));

    nodes.prev.disabled = cache.filter.offset <= 0;
    nodes.next.disabled = list.length < PAGE_SIZE;
  }

  function buildRow(node) {
    const row = el('tr');
    for (let i = 0; i < 6; i += 1) row.appendChild(el('td'));
    row.children[5].className = 'actions';
    updateRow(row, node);
    return row;
  }

  function updateRow(row, node) {
    const [idCell, statusCell, netCell, wsCell, seenCell, actionCell] = row.children;

    clear(idCell);
    idCell.appendChild(el('div', { class: 'mono', text: node.node_id }));
    idCell.appendChild(el('div', { class: 'mono muted', text: node.instance_id || '—' }));

    clear(statusCell);
    statusCell.appendChild(nodeStateBadge(node.status));
    if (node.platform) statusCell.appendChild(el('div', { class: 'small muted', text: node.platform }));

    clear(netCell);
    netCell.appendChild(el('div', { class: 'mono small', text: node.private_ip || '—' }));
    netCell.appendChild(el('div', { class: 'mono small muted', text: node.public_ip || '—' }));

    clear(wsCell);
    wsCell.appendChild(boolBadge(node.ws_connected, { onLabel: '已连接', offLabel: '未连接' }));

    seenCell.textContent = formatTime(node.last_heartbeat || node.created_at);

    clear(actionCell);
    const released = node.status === 'terminated';
    actionCell.appendChild(actionButton('日志', () => showLogs(node), busy, node.node_id));
    if (!released) {
      actionCell.appendChild(actionButton('drain', async () => {
        if (!await confirmDialog({ title: '排空 Worker', message: `确认排空 ${node.node_id}？不再接受新任务。` })) return;
        await post(`/nodes/${encodeURIComponent(node.node_id)}/drain`);
        toastSuccess('已请求排空。');
        reload();
      }, busy, node.node_id));
      actionCell.appendChild(actionButton('销毁', async () => {
        if (!await confirmDialog({
          title: '销毁 Worker',
          message: `确认销毁 ${node.node_id}？该实例会被终止，运行中的任务将中断。`,
          confirmLabel: '销毁',
          danger: true,
        })) return;
        await post('/scale-in', { node_ids: [node.node_id], force: true });
        toastSuccess('已请求销毁。');
        reload();
      }, busy, node.node_id, 'btn-danger'));
    } else {
      // Historical rows keep the log entry (until the record is removed) but
      // must not offer terminate on an already-released resource.
      actionCell.appendChild(actionButton('移除记录', async () => {
        if (!await confirmDialog({ title: '移除节点记录', message: `从注册表移除 ${node.node_id}？` })) return;
        await del(`/nodes/${encodeURIComponent(node.node_id)}`);
        toastSuccess('记录已移除。');
        reload();
      }, busy, node.node_id));
    }
  }

  function dispose() {
    if (poller) poller.stop();
  }

  return { mount, dispose };
}

function actionButton(label, handler, busy, key, extraClass = '') {
  const button = el('button', { type: 'button', class: `btn btn-sm ${extraClass}`.trim(), text: label });
  button.addEventListener('click', async () => {
    const lock = `${key}:${label}`;
    if (busy.has(lock)) return;
    busy.add(lock);
    button.disabled = true;
    try {
      await handler();
    } catch (error) {
      toastError(describeError(error));
    } finally {
      busy.delete(lock);
      button.disabled = false;
    }
  });
  return button;
}

async function showLogs(node) {
  const view = el('pre', { class: 'log-view', text: '加载中…' });
  const dialogPromise = showDialog({
    title: `Worker 日志 · ${node.node_id}`,
    body: [el('p', { class: 'muted small', text: 'ea-runtime systemd journal（只读，SSH 读取）。' }), view],
    actions: [{ label: '关闭', value: null, kind: 'primary' }],
  });
  try {
    const data = await get(`/nodes/${encodeURIComponent(node.node_id)}/logs`, { query: { lines: 400 } });
    view.textContent = data.logs || '（无输出）';
  } catch (error) {
    const status = Number(error && error.status) || 0;
    view.textContent = status === 404 || status === 409
      ? 'Worker 资源已释放或尚未就绪，无法读取 systemd 日志；请查看 Job 归档日志。'
      : describeError(error);
  }
  await dialogPromise;
}

function buildLayout(nodes, cache, resetToFirstPage, refresh) {
  const root = document.createDocumentFragment();
  root.appendChild(el('div', { class: 'page-head' }, [
    el('div', {}, [
      el('h1', { text: 'Workers' }),
      el('p', { class: 'page-sub', text: '每页最多 100 台；全局计数来自 Manager summary，而不是当前页。' }),
    ]),
  ]));

  const statusSelect = el('select', { id: 'fleetStatus', 'aria-label': '按状态筛选' });
  for (const value of STATUSES) {
    statusSelect.appendChild(el('option', { value, text: value || '全部状态', selected: value === cache.filter.status }));
  }
  statusSelect.addEventListener('change', () => {
    cache.filter.status = statusSelect.value;
    resetToFirstPage();
  });

  const card = el('section', { class: 'card' });
  card.appendChild(el('div', { class: 'filters' }, [
    el('div', { class: 'field' }, [el('label', { for: 'fleetStatus', text: '状态' }), statusSelect]),
  ]));

  nodes.status = el('p', { class: 'small muted', role: 'status' });
  card.appendChild(nodes.status);

  nodes.tbody = el('tbody');
  const table = el('table', {}, [
    el('thead', {}, [el('tr', {}, [
      el('th', { text: 'Worker / 实例' }),
      el('th', { text: '状态' }),
      el('th', { text: '内网 / 公网 IP' }),
      el('th', { text: 'WebSocket' }),
      el('th', { text: '最近心跳' }),
      el('th', { text: '操作' }),
    ])]),
    nodes.tbody,
  ]);
  card.appendChild(el('div', { class: 'table-wrap' }, [table]));
  nodes.empty = el('p', { class: 'empty', text: '当前筛选下没有 Worker。' });
  card.appendChild(nodes.empty);

  nodes.prev = el('button', { type: 'button', class: 'btn btn-sm', text: '上一页' });
  nodes.next = el('button', { type: 'button', class: 'btn btn-sm', text: '下一页' });
  nodes.prev.addEventListener('click', () => {
    cache.filter.offset = Math.max(0, cache.filter.offset - PAGE_SIZE);
    refresh();
  });
  nodes.next.addEventListener('click', () => {
    cache.filter.offset += PAGE_SIZE;
    refresh();
  });
  card.appendChild(el('div', { class: 'pager' }, [nodes.prev, nodes.next]));

  root.appendChild(card);
  return root;
}
