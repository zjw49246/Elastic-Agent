import { el, clear, formatDuration } from '../core/dom.js';
import { get } from '../core/api.js';
import { getState, subscribe } from '../core/store.js';
import { createPoller } from '../core/poller.js';
import { renderOtpList } from '../components/otp-center.js';
import { jobStateBadge } from '../components/status-badge.js';
import { describeError } from '../core/errors.js';

export function createPage({ router, container }) {
  let unsubscribe = null;
  let poller = null;
  const nodes = {};

  function mount() {
    container.appendChild(buildLayout(router, nodes));
    unsubscribe = subscribe(() => renderSummary(nodes));
    renderSummary(nodes);

    // The overview must never pull the full Job list; it reads a bounded page
    // of recent Jobs only to surface failures needing attention.
    poller = createPoller({
      name: 'overview-recent',
      interval: 15000,
      task: async (signal) => {
        try {
          const data = await get('/jobs', { signal });
          renderRecent(nodes, data.jobs || [], router);
        } catch (error) {
          renderRecentError(nodes, describeError(error));
        }
      },
    });
    poller.start();
  }

  function dispose() {
    if (poller) poller.stop();
    if (unsubscribe) unsubscribe();
  }

  return { mount, dispose };
}

function buildLayout(router, nodes) {
  const root = document.createDocumentFragment();
  root.appendChild(el('div', { class: 'page-head' }, [
    el('div', {}, [
      el('h1', { text: '总览' }),
      el('p', { class: 'page-sub', text: 'Manager 健康、容量与待处理事项；详细数据请进入对应页面。' }),
    ]),
    el('div', { class: 'page-actions' }, [
      el('a', { class: 'btn btn-primary', href: router.href('/jobs/new'), text: '提交 Job' }),
      el('a', { class: 'btn', href: router.href('/accounts/new'), text: '添加账号' }),
      el('a', { class: 'btn', href: router.href('/jobs'), text: '查看 Jobs' }),
    ]),
  ]));

  nodes.stats = el('div', { class: 'grid grid-cards' });
  root.appendChild(nodes.stats);

  const otpCard = el('section', { class: 'card', id: 'otpSection' }, [
    el('h2', { text: '待处理验证码' }),
  ]);
  nodes.otpEmpty = el('p', { class: 'muted small', text: '当前没有需要人工输入的验证码。' });
  nodes.otpList = el('div', {});
  otpCard.appendChild(nodes.otpEmpty);
  otpCard.appendChild(nodes.otpList);
  root.appendChild(otpCard);

  const recent = el('section', { class: 'card' }, [el('h2', { text: '最近异常' })]);
  nodes.recent = el('div', { class: 'stack' }, [el('p', { class: 'muted small', text: '加载中…' })]);
  recent.appendChild(nodes.recent);
  root.appendChild(recent);

  return root;
}

function stat(label, value, hint) {
  return el('div', { class: 'stat' }, [
    el('div', { class: 'stat-label', text: label }),
    el('div', { class: 'stat-value', text: value === null || value === undefined ? '—' : String(value) }),
    hint ? el('div', { class: 'stat-hint', text: hint }) : null,
  ]);
}

function renderSummary(nodes) {
  const { summary, health, loginAttempts } = getState();
  clear(nodes.stats);

  const manager = (summary && summary.manager) || {};
  const jobs = (summary && summary.jobs) || {};
  const workers = (summary && summary.workers) || {};
  const accounts = (summary && summary.accounts) || {};

  nodes.stats.appendChild(stat(
    'Manager',
    (health && health.status) || manager.status || '—',
    manager.uptime_seconds ? `已运行 ${formatDuration(manager.uptime_seconds)}` : '',
  ));
  nodes.stats.appendChild(stat(
    '活跃 Job',
    jobs.active,
    jobs.terminal_total !== null && jobs.terminal_total !== undefined ? `历史 ${jobs.terminal_total}` : '',
  ));
  nodes.stats.appendChild(stat(
    'Workers',
    workers.total,
    workers.connected !== undefined && workers.connected !== null ? `在线 ${workers.connected}` : '',
  ));
  nodes.stats.appendChild(stat(
    '账号',
    accounts.total,
    accounts.allocated !== null && accounts.allocated !== undefined ? `已分配 ${accounts.allocated}` : '',
  ));
  nodes.stats.appendChild(stat('待清理', jobs.cleanup_pending, jobs.cleanup_pending ? '需要关注' : ''));

  if (summary && summary.degraded) {
    nodes.stats.appendChild(stat('统计来源', '降级', 'Manager 尚未提供 /api/ui/summary'));
  }

  const attempts = Array.isArray(loginAttempts) ? loginAttempts : [];
  nodes.otpEmpty.hidden = attempts.length > 0;
  renderOtpList(nodes.otpList, attempts);
}

function renderRecent(nodes, jobs, router) {
  const interesting = jobs
    .filter((job) => job.state === 'failed' || job.state === 'interrupted' || Number(job.cleanup_pending) > 0)
    .slice(0, 10);
  clear(nodes.recent);
  if (interesting.length === 0) {
    nodes.recent.appendChild(el('p', { class: 'muted small', text: '没有失败或待清理的 Job。' }));
    return;
  }
  for (const job of interesting) {
    nodes.recent.appendChild(el('div', { class: 'spread' }, [
      el('div', {}, [
        el('a', { href: router.href(`/jobs/${encodeURIComponent(job.job_id)}`), text: job.name || job.job_id }),
        el('div', { class: 'mono muted', text: job.job_id }),
        job.error ? el('div', { class: 'small', text: job.error }) : null,
      ]),
      el('div', { class: 'row' }, [
        jobStateBadge(job.state),
        Number(job.cleanup_pending) > 0
          ? el('span', { class: 'badge badge-warn', text: `待清理 ${job.cleanup_pending}` })
          : null,
      ]),
    ]));
  }
}

function renderRecentError(nodes, message) {
  clear(nodes.recent);
  nodes.recent.appendChild(el('p', { class: 'muted small', text: message }));
}
