/**
 * Jobs list.
 *
 * Capability-detects server-side pagination (``next_cursor``); on the legacy
 * full response it filters/pages client-side over the bounded snapshot the
 * Manager already returns. Never fires per-Job result requests from the list.
 */

import { el, clear, reconcileList, formatTime } from '../core/dom.js';
import { get, post, createGeneration } from '../core/api.js';
import { createPoller } from '../core/poller.js';
import { pageCache } from '../core/store.js';
import { jobStateBadge } from '../components/status-badge.js';
import { confirmDialog } from '../components/dialog.js';
import { toastError, toastSuccess } from '../components/toast.js';
import { describeError } from '../core/errors.js';

const PAGE_SIZE = 50;
const STATES = ['', 'prepared', 'launching', 'running', 'succeeded', 'failed', 'cancelled', 'interrupted', 'recovered'];

export function createPage({ router, container }) {
  const cache = pageCache('jobs');
  if (!cache.filter) cache.filter = { state: '', query: '', page: 0 };
  const generation = createGeneration();
  const nodes = {};
  const busy = new Set();
  let poller = null;

  function mount() {
    container.appendChild(buildLayout(router, nodes, cache, resetPage, refresh, render));
    poller = createPoller({ name: 'jobs', interval: 5000, task: (signal) => load(signal) });
    poller.start();
  }

  function refresh() {
    if (poller) poller.refresh();
  }

  function resetPage() {
    cache.filter.page = 0;
    refresh();
  }

  async function load(signal) {
    const token = generation.next();
    let data;
    try {
      // Paginated form first (Phase 3 backend); the legacy Manager ignores
      // unknown query params and returns the bounded full snapshot.
      data = await get('/jobs', {
        query: {
          limit: PAGE_SIZE,
          state: cache.filter.state || undefined,
          query: cache.filter.query || undefined,
        },
        signal,
      });
    } catch (error) {
      if (!token.valid) return;
      nodes.status.textContent = describeError(error);
      throw error;
    }
    if (!token.valid) return;
    cache.jobs = Array.isArray(data.jobs) ? data.jobs : [];
    cache.truncated = Boolean(data.truncated);
    render();
    adjustCadence();
  }

  function adjustCadence() {
    // Pure-terminal pages poll slowly; anything active keeps 5s.
    const anyActive = (cache.jobs || []).some((job) => !job.done);
    poller.setInterval(anyActive ? 5000 : 45000);
  }

  function visibleJobs() {
    let rows = cache.jobs || [];
    const { state, query } = cache.filter;
    if (state) rows = rows.filter((job) => job.state === state);
    const q = (query || '').trim().toLowerCase();
    if (q) {
      rows = rows.filter((job) =>
        String(job.job_id).toLowerCase().includes(q)
        || String(job.name || '').toLowerCase().includes(q));
    }
    return rows;
  }

  function render() {
    const rows = visibleJobs();
    const start = cache.filter.page * PAGE_SIZE;
    if (start >= rows.length && cache.filter.page > 0) {
      cache.filter.page = 0;
      return render();
    }
    const page = rows.slice(start, start + PAGE_SIZE);
    nodes.status.textContent = `${rows.length} 个 Job${cache.truncated ? '（历史已截断）' : ''} · 第 ${cache.filter.page + 1} 页`;
    nodes.empty.hidden = page.length > 0;
    reconcileList(nodes.tbody, page, (j) => j.job_id, (j) => buildRow(j), (row, j) => updateRow(row, j));
    nodes.prev.disabled = cache.filter.page <= 0;
    nodes.next.disabled = start + PAGE_SIZE >= rows.length;
  }

  function buildRow(job) {
    const row = el('tr');
    for (let i = 0; i < 5; i += 1) row.appendChild(el('td'));
    row.children[4].className = 'actions';
    updateRow(row, job);
    return row;
  }

  function updateRow(row, job) {
    const [nameCell, stateCell, workerCell, timeCell, actionCell] = row.children;

    clear(nameCell);
    nameCell.appendChild(el('a', {
      href: router.href(`/jobs/${encodeURIComponent(job.job_id)}`),
      text: job.name || job.job_id,
    }));
    nameCell.appendChild(el('div', { class: 'mono muted small', text: job.job_id }));

    clear(stateCell);
    stateCell.appendChild(jobStateBadge(job.state));
    if (Number(job.cleanup_pending) > 0) {
      stateCell.appendChild(el('div', { class: 'small', text: `待清理 ${job.cleanup_pending}` }));
    }
    if (job.error) stateCell.appendChild(el('div', { class: 'small err', text: String(job.error).slice(0, 140) }));

    clear(workerCell);
    workerCell.appendChild(el('div', { text: `${job.workers ?? '—'} workers` }));
    const phases = job.phases && typeof job.phases === 'object'
      ? Object.entries(job.phases).map(([phase, count]) => `${phase} ${count}`).join(' · ')
      : '';
    if (phases) workerCell.appendChild(el('div', { class: 'small muted', text: phases }));

    clear(timeCell);
    timeCell.appendChild(el('div', { class: 'small', text: formatTime(job.created_at) }));
    if (job.completed_at) timeCell.appendChild(el('div', { class: 'small muted', text: `结束 ${formatTime(job.completed_at)}` }));

    clear(actionCell);
    actionCell.appendChild(el('a', {
      class: 'btn btn-sm',
      href: router.href(`/jobs/${encodeURIComponent(job.job_id)}`),
      text: '详情',
    }));
    if (!job.done && !job.cancel_requested) {
      actionCell.appendChild(actionButton('取消', async () => {
        if (!await confirmDialog({
          title: '取消 Job',
          message: `确认取消 ${job.name || job.job_id}？运行中的命令会被终止，临时 Worker 将被销毁。`,
          confirmLabel: '取消 Job',
          danger: true,
        })) return;
        await post(`/jobs/${encodeURIComponent(job.job_id)}/cancel`);
        toastSuccess('已请求取消。');
        refresh();
      }, busy, job.job_id, 'btn-danger'));
    }
    if (job.done) {
      actionCell.appendChild(actionButton('重跑', async () => {
        if (!await confirmDialog({
          title: '重新提交 Job',
          message: `按已持久化的配置重新提交 ${job.name || job.job_id}？会创建一个新的 Job。`,
          confirmLabel: '重跑',
        })) return;
        const created = await post(`/jobs/${encodeURIComponent(job.job_id)}/resubmit`);
        toastSuccess(`已重新提交：${created.job_id}`);
        router.navigate(`/jobs/${encodeURIComponent(created.job_id)}`);
      }, busy, job.job_id));
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

function buildLayout(router, nodes, cache, resetPage, refresh, rerender) {
  const root = document.createDocumentFragment();
  root.appendChild(el('div', { class: 'page-head' }, [
    el('div', {}, [
      el('h1', { text: 'Jobs' }),
      el('p', { class: 'page-sub', text: '活跃 Job 5 秒刷新；纯终态页面降频。列表不请求单 Job 结果。' }),
    ]),
    el('div', { class: 'page-actions' }, [
      el('a', { class: 'btn btn-primary', href: router.href('/jobs/new'), text: '提交 Job' }),
    ]),
  ]));

  const card = el('section', { class: 'card' });

  const stateSelect = el('select', { id: 'jobsState' });
  for (const value of STATES) {
    stateSelect.appendChild(el('option', { value, text: value || '全部状态', selected: value === cache.filter.state }));
  }
  stateSelect.addEventListener('change', () => {
    cache.filter.state = stateSelect.value;
    resetPage();
  });
  const search = el('input', { type: 'search', id: 'jobsQuery', placeholder: '名称或 Job ID', value: cache.filter.query || '' });
  search.addEventListener('input', () => {
    cache.filter.query = search.value;
    cache.filter.page = 0;
    // Text filtering happens over the cached snapshot — no network round-trip
    // per keystroke.
    rerender();
  });
  card.appendChild(el('div', { class: 'filters' }, [
    el('div', { class: 'field' }, [el('label', { for: 'jobsState', text: '状态' }), stateSelect]),
    el('div', { class: 'field' }, [el('label', { for: 'jobsQuery', text: '搜索' }), search]),
  ]));

  nodes.status = el('p', { class: 'small muted', role: 'status' });
  card.appendChild(nodes.status);

  nodes.tbody = el('tbody');
  card.appendChild(el('div', { class: 'table-wrap' }, [
    el('table', {}, [
      el('thead', {}, [el('tr', {}, [
        el('th', { text: 'Job' }),
        el('th', { text: '状态' }),
        el('th', { text: 'Workers' }),
        el('th', { text: '时间' }),
        el('th', { text: '操作' }),
      ])]),
      nodes.tbody,
    ]),
  ]));
  nodes.empty = el('p', { class: 'empty', text: '没有匹配的 Job。' });
  card.appendChild(nodes.empty);

  nodes.prev = el('button', { type: 'button', class: 'btn btn-sm', text: '上一页' });
  nodes.next = el('button', { type: 'button', class: 'btn btn-sm', text: '下一页' });
  nodes.prev.addEventListener('click', () => { cache.filter.page -= 1; refresh(); });
  nodes.next.addEventListener('click', () => { cache.filter.page += 1; refresh(); });
  card.appendChild(el('div', { class: 'pager' }, [nodes.prev, nodes.next]));

  root.appendChild(card);
  return root;
}
