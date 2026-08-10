/**
 * Job detail: timeline, redacted spec, worker table, logs, OTP, results.
 *
 * Logs poll only while their panel is open; a scope switch aborts the previous
 * request and late responses are dropped via request generations.
 */

import { el, clear, formatTime, formatBytes, reconcileList } from '../core/dom.js';
import { get, post, createGeneration } from '../core/api.js';
import { createPoller } from '../core/poller.js';
import { getState, subscribe } from '../core/store.js';
import { jobStateBadge, workerPhaseBadge, boolBadge } from '../components/status-badge.js';
import { renderOtpList } from '../components/otp-center.js';
import { confirmDialog } from '../components/dialog.js';
import { toastError, toastSuccess } from '../components/toast.js';
import { describeError } from '../core/errors.js';
import { downloadJobResults, isDownloading, cancelDownload, supportsStreamingSave } from '../core/downloads.js';

const TIMELINE = ['prepared', 'launching', 'running', 'terminal', 'cleanup'];

export function createPage({ router, params, container }) {
  const jobId = params.jobId;
  const nodes = {};
  const generation = createGeneration();
  const logGeneration = createGeneration();
  const busy = new Set();
  let poller = null;
  let logPoller = null;
  let logScope = null; // {worker_id?} — logs poll only while open
  let unsubscribe = null;
  let resultLoaded = false;
  let job = null;

  function mount() {
    container.appendChild(buildLayout());
    poller = createPoller({ name: `job-${jobId}`, interval: 5000, task: (signal) => load(signal) });
    poller.start();
    unsubscribe = subscribe(() => renderOtp());
    renderOtp();
  }

  function dispose() {
    if (poller) poller.stop();
    stopLogs();
    if (unsubscribe) unsubscribe();
  }

  async function load(signal) {
    const token = generation.next();
    let data;
    try {
      data = await get(`/jobs/${encodeURIComponent(jobId)}`, { signal });
    } catch (error) {
      if (!token.valid) return;
      if (Number(error && error.status) === 404) {
        clear(nodes.header);
        nodes.header.appendChild(el('p', { class: 'err', text: `Job ${jobId} 不存在。` }));
        poller.stop();
        return;
      }
      throw error;
    }
    if (!token.valid) return;
    job = data;
    render();
    if (job.done) poller.setInterval(30000);
    if (job.done && !resultLoaded) {
      resultLoaded = true;
      void loadResults();
    }
  }

  // ---------------------------------------------------------------- render

  function render() {
    renderHeader();
    renderTimeline();
    renderSpec();
    renderWorkers();
  }

  function renderHeader() {
    clear(nodes.header);
    nodes.header.appendChild(el('div', { class: 'spread' }, [
      el('div', {}, [
        el('h1', { text: job.name || job.job_id }),
        el('p', { class: 'mono muted small', text: job.job_id }),
      ]),
      el('div', { class: 'row' }, [jobStateBadge(job.state)]),
    ]));
    const meta = el('p', { class: 'small muted' });
    const bits = [`创建 ${formatTime(job.created_at)}`];
    if (job.started_at) bits.push(`启动 ${formatTime(job.started_at)}`);
    if (job.completed_at) bits.push(`结束 ${formatTime(job.completed_at)}`);
    if (Number(job.cleanup_pending) > 0) bits.push(`待清理 ${job.cleanup_pending}`);
    meta.textContent = bits.join(' · ');
    nodes.header.appendChild(meta);
    if (job.error) nodes.header.appendChild(el('p', { class: 'err small', text: String(job.error) }));
    if (job.cancel_requested) {
      nodes.header.appendChild(el('p', { class: 'small', text: `已请求取消${job.cancel_reason ? `：${job.cancel_reason}` : ''}` }));
    }

    clear(nodes.actions);
    if (!job.done && !job.cancel_requested) {
      nodes.actions.appendChild(actionButton('取消 Job', async () => {
        if (!await confirmDialog({
          title: '取消 Job',
          message: '确认取消？运行中的命令会被终止，临时 Worker 将被销毁。',
          confirmLabel: '取消 Job',
          danger: true,
        })) return;
        await post(`/jobs/${encodeURIComponent(jobId)}/cancel`);
        toastSuccess('已请求取消。');
        poller.refresh();
      }, 'cancel', 'btn-danger'));
    }
    if (job.done) {
      nodes.actions.appendChild(actionButton('重跑', async () => {
        if (!await confirmDialog({ title: '重新提交', message: '按持久化配置创建新 Job？', confirmLabel: '重跑' })) return;
        const created = await post(`/jobs/${encodeURIComponent(jobId)}/resubmit`);
        toastSuccess(`已重新提交：${created.job_id}`);
        router.navigate(`/jobs/${encodeURIComponent(created.job_id)}`);
      }, 'resubmit'));
    }
    nodes.actions.appendChild(el('a', { class: 'btn btn-sm', href: router.href('/results'), text: '打开结果页' }));
  }

  function renderTimeline() {
    clear(nodes.timeline);
    const reached = timelineProgress(job);
    TIMELINE.forEach((stage, index) => {
      nodes.timeline.appendChild(el('li', { dataset: { done: String(index <= reached) } }, [
        el('span', { text: index <= reached ? '●' : '○', 'aria-hidden': 'true' }),
        el('span', { text: stageLabel(stage) }),
      ]));
    });
  }

  function renderSpec() {
    if (!job.spec) return;
    // Server already redacts env values and secret references; render as text.
    nodes.spec.textContent = JSON.stringify(job.spec, null, 2);
  }

  function renderWorkers() {
    const workers = Array.isArray(job.workers_detail) ? job.workers_detail : [];
    nodes.workersEmpty.hidden = workers.length > 0;
    reconcileList(
      nodes.workersBody,
      workers,
      (w) => w.worker_id || `shard-${w.shard_index}`,
      (w) => buildWorkerRow(w),
      (row, w) => updateWorkerRow(row, w),
    );
  }

  function buildWorkerRow(worker) {
    const row = el('tr');
    for (let i = 0; i < 6; i += 1) row.appendChild(el('td'));
    row.children[5].className = 'actions';
    updateWorkerRow(row, worker);
    return row;
  }

  function updateWorkerRow(row, worker) {
    const [shardCell, phaseCell, acctCell, eipCell, statusCell, actionCell] = row.children;

    clear(shardCell);
    shardCell.appendChild(el('div', { text: `shard ${worker.shard_index ?? '—'}` }));
    if (worker.worker_id) shardCell.appendChild(el('div', { class: 'mono muted small', text: worker.worker_id }));

    clear(phaseCell);
    phaseCell.appendChild(workerPhaseBadge(worker.phase));
    if (Number(worker.rotations) > 0) phaseCell.appendChild(el('div', { class: 'small muted', text: `换号 ${worker.rotations} 次` }));
    if (worker.error) phaseCell.appendChild(el('div', { class: 'small err', text: String(worker.error).slice(0, 160) }));

    clear(acctCell);
    if (worker.account_email || worker.account_id) {
      acctCell.appendChild(el('div', { class: 'small', text: worker.account_email || worker.account_id }));
    }
    const accounts = Array.isArray(worker.accounts) ? worker.accounts : [];
    if (accounts.length > 1) {
      acctCell.appendChild(el('div', { class: 'small muted', text: `${accounts.length} 个槽位 · 活跃 ${worker.active_slot ?? 0}` }));
    }

    clear(eipCell);
    eipCell.appendChild(el('div', { class: 'mono small', text: worker.eip || '—' }));

    clear(statusCell);
    statusCell.appendChild(boolBadge(worker.final_collected, { onLabel: '已收集', offLabel: '未收集' }));
    statusCell.appendChild(boolBadge(worker.cleaned_up, { onLabel: '已清理', offLabel: '未清理' }));
    if (worker.collection_error) statusCell.appendChild(el('div', { class: 'small err', text: String(worker.collection_error).slice(0, 120) }));
    if (worker.cleanup_error) statusCell.appendChild(el('div', { class: 'small err', text: String(worker.cleanup_error).slice(0, 120) }));

    clear(actionCell);
    const logBtn = el('button', { type: 'button', class: 'btn btn-sm', text: '日志' });
    logBtn.addEventListener('click', () => openLogs(worker.worker_id || null));
    actionCell.appendChild(logBtn);
  }

  function renderOtp() {
    const attempts = (getState().loginAttempts || []).filter((c) => c.job_id === jobId);
    nodes.otpCard.hidden = attempts.length === 0;
    renderOtpList(nodes.otpList, attempts);
  }

  // ----------------------------------------------------------------- logs

  function openLogs(workerId) {
    logScope = { worker_id: workerId };
    nodes.logsCard.hidden = false;
    nodes.logsTitle.textContent = workerId ? `Job 日志 · ${workerId}` : 'Job 日志（全部 Worker）';
    nodes.logsView.textContent = '加载中…';
    stopLogs(false);
    logPoller = createPoller({
      name: `job-logs-${jobId}`,
      interval: 3000,
      task: (signal) => loadLogs(signal),
    });
    logPoller.start();
    nodes.logsCard.scrollIntoView({ block: 'nearest' });
  }

  function stopLogs(hide = true) {
    if (logPoller) {
      logPoller.stop();
      logPoller = null;
    }
    if (hide && nodes.logsCard) nodes.logsCard.hidden = true;
  }

  async function loadLogs(signal) {
    const scope = logScope;
    const token = logGeneration.next();
    const query = { lines: 1000 };
    if (scope && scope.worker_id) query.worker_id = scope.worker_id;
    const data = await get(`/jobs/${encodeURIComponent(jobId)}/logs`, { query, signal });
    // A late response for an earlier scope must not overwrite the new one.
    if (!token.valid || scope !== logScope) return;
    renderLogs(data);
    if (data.status === 'archived' || data.status === 'unavailable') {
      // Terminal logs do not change; stop polling but keep the panel visible.
      if (logPoller) { logPoller.stop(); logPoller = null; }
    }
  }

  function renderLogs(data) {
    clear(nodes.logsView);
    if (data.message && !(data.entries || []).length) {
      nodes.logsView.appendChild(el('div', { class: 'muted', text: data.message }));
    }
    for (const entry of data.entries || []) {
      const line = el('div', {
        class: entry.stream === 'stderr' ? 'log-line-stderr' : '',
        text: entry.data,
      });
      nodes.logsView.appendChild(line);
    }
    nodes.logsMeta.textContent = [
      `状态 ${data.status || '—'}`,
      data.truncated ? '已截断' : '',
      `${data.returned ?? 0}/${data.total ?? 0} 行`,
    ].filter(Boolean).join(' · ');
    if (nodes.logsFollow.checked) nodes.logsView.scrollTop = nodes.logsView.scrollHeight;
  }

  // -------------------------------------------------------------- results

  async function loadResults() {
    clear(nodes.results);
    nodes.results.appendChild(el('p', { class: 'muted small', text: '检查结果中…' }));
    try {
      const data = await get(`/jobs/${encodeURIComponent(jobId)}/results`);
      clear(nodes.results);
      nodes.results.appendChild(el('p', { class: 'small', text: `文件 ${data.file_count ?? 0} 个` }));
      if (data.s3_uri) nodes.results.appendChild(el('p', { class: 'mono small', text: data.s3_uri }));
      if (Array.isArray(data.scores) && data.scores.length) {
        const list = el('ul', { class: 'small' });
        for (const score of data.scores.slice(0, 20)) {
          list.appendChild(el('li', { text: `${score.task_id || ''} ${score.status || ''} ${score.final_score ?? ''}`.trim() }));
        }
        nodes.results.appendChild(list);
      }
      nodes.results.appendChild(buildDownloadControls());
    } catch (error) {
      clear(nodes.results);
      const status = Number(error && error.status) || 0;
      nodes.results.appendChild(el('p', {
        class: 'muted small',
        text: status === 404 ? '该 Job 暂无已收集结果。' : `结果暂不可用：${describeError(error)}`,
      }));
      if (status !== 404) {
        const retry = el('button', { type: 'button', class: 'btn btn-sm', text: '重试' });
        retry.addEventListener('click', () => loadResults());
        nodes.results.appendChild(retry);
      }
    }
  }

  function buildDownloadControls() {
    const wrap = el('div', { class: 'row' });
    const progress = el('span', { class: 'small muted', role: 'status' });
    const download = el('button', { type: 'button', class: 'btn btn-sm btn-primary', text: '下载全部（tar.gz）' });
    const cancel = el('button', { type: 'button', class: 'btn btn-sm', text: '取消', hidden: true });
    download.addEventListener('click', async () => {
      if (isDownloading(jobId)) return;
      download.disabled = true;
      cancel.hidden = false;
      if (!supportsStreamingSave()) {
        progress.textContent = '当前浏览器不支持流式落盘，大文件会占用内存。';
      }
      try {
        await downloadJobResults(jobId, {
          onProgress: ({ received, expected }) => {
            progress.textContent = expected
              ? `已接收 ${formatBytes(received)} / 源 ${formatBytes(expected)}`
              : `已接收 ${formatBytes(received)}`;
          },
        });
        progress.textContent = '下载完成。';
      } catch (error) {
        progress.textContent = error && error.aborted ? '下载已取消。' : describeError(error);
      } finally {
        download.disabled = false;
        cancel.hidden = true;
      }
    });
    cancel.addEventListener('click', () => cancelDownload(jobId));
    wrap.appendChild(download);
    wrap.appendChild(cancel);
    wrap.appendChild(progress);
    return wrap;
  }

  // -------------------------------------------------------------- layout

  function actionButton(label, handler, key, extraClass = '') {
    const button = el('button', { type: 'button', class: `btn ${extraClass}`.trim(), text: label });
    button.addEventListener('click', async () => {
      if (busy.has(key)) return;
      busy.add(key);
      button.disabled = true;
      try {
        await handler();
      } catch (error) {
        toastError(describeError(error));
      } finally {
        busy.delete(key);
        button.disabled = false;
      }
    });
    return button;
  }

  function buildLayout() {
    const root = document.createDocumentFragment();

    nodes.header = el('div', {});
    nodes.actions = el('div', { class: 'row', style: { 'margin-top': '8px' } });
    root.appendChild(el('section', { class: 'card' }, [nodes.header, nodes.actions]));

    nodes.timeline = el('ul', { class: 'timeline' });
    root.appendChild(el('section', { class: 'card' }, [el('h2', { text: '状态时间线' }), nodes.timeline]));

    nodes.otpList = el('div', {});
    nodes.otpCard = el('section', { class: 'card', hidden: true }, [
      el('h2', { text: '需要验证码' }),
      nodes.otpList,
    ]);
    root.appendChild(nodes.otpCard);

    nodes.workersBody = el('tbody');
    nodes.workersEmpty = el('p', { class: 'empty', text: '尚无 Worker 信息。' });
    root.appendChild(el('section', { class: 'card' }, [
      el('div', { class: 'spread' }, [
        el('h2', { text: 'Workers' }),
        (() => {
          const all = el('button', { type: 'button', class: 'btn btn-sm', text: '查看全部日志' });
          all.addEventListener('click', () => openLogs(null));
          return all;
        })(),
      ]),
      el('div', { class: 'table-wrap' }, [
        el('table', {}, [
          el('thead', {}, [el('tr', {}, [
            el('th', { text: 'Shard / Worker' }),
            el('th', { text: '阶段' }),
            el('th', { text: '账号' }),
            el('th', { text: 'EIP' }),
            el('th', { text: '收集 / 清理' }),
            el('th', { text: '操作' }),
          ])]),
          nodes.workersBody,
        ]),
      ]),
      nodes.workersEmpty,
    ]));

    nodes.logsTitle = el('h2', { text: 'Job 日志' });
    nodes.logsMeta = el('span', { class: 'small muted' });
    nodes.logsFollow = el('input', { type: 'checkbox', id: 'logFollow', checked: true });
    nodes.logsView = el('div', { class: 'log-view', role: 'log' });
    const closeLogs = el('button', { type: 'button', class: 'btn btn-sm', text: '关闭' });
    closeLogs.addEventListener('click', () => stopLogs(true));
    nodes.logsCard = el('section', { class: 'card', hidden: true }, [
      el('div', { class: 'spread' }, [
        nodes.logsTitle,
        el('div', { class: 'row' }, [
          el('label', { class: 'small', for: 'logFollow' }, [nodes.logsFollow, ' 跟随']),
          nodes.logsMeta,
          closeLogs,
        ]),
      ]),
      nodes.logsView,
    ]);
    root.appendChild(nodes.logsCard);

    nodes.results = el('div', {});
    root.appendChild(el('section', { class: 'card' }, [el('h2', { text: '结果' }), nodes.results]));
    nodes.results.appendChild(el('p', { class: 'muted small', text: 'Job 终态后自动检查一次；也可打开结果页查看全部。' }));

    nodes.spec = el('pre', { class: 'code', text: '加载中…' });
    const specDetails = el('details', {}, [
      el('summary', { text: '脱敏后的 JobSpec' }),
      nodes.spec,
    ]);
    root.appendChild(el('section', { class: 'card' }, [specDetails]));

    return root;
  }

  return { mount, dispose };
}

function timelineProgress(job) {
  if (Number(job.cleanup_pending) > 0) return 3;
  switch (job.state) {
    case 'prepared': return 0;
    case 'launching': return 1;
    case 'running': return 2;
    case 'succeeded':
    case 'failed':
    case 'cancelled':
    case 'interrupted':
    case 'recovered':
      return 4;
    default: return 0;
  }
}

function stageLabel(stage) {
  switch (stage) {
    case 'prepared': return '已准备';
    case 'launching': return '启动中';
    case 'running': return '运行中';
    case 'terminal': return '终态';
    case 'cleanup': return '清理完成';
    default: return stage;
  }
}
