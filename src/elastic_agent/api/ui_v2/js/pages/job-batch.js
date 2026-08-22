/** Strict JSON JobBatch planning, durable submission and status tracking. */

import { el, clear } from '../core/dom.js';
import { get, postJsonText } from '../core/api.js';
import { describeError, isAbort } from '../core/errors.js';
import { createPoller } from '../core/poller.js';
import {
  JOB_BATCH_DEFAULT_MAX_ITEMS,
  JOB_BATCH_MAX_BYTES,
  JOB_BATCH_SCHEMA_MAX_ITEMS,
  claimBatchSubmissionIntent,
  clearBatchSubmissionIntent,
  batchReceiptIsTerminal,
  parseBatchSource,
} from '../core/job-batch.js';
import { confirmDialog } from '../components/dialog.js';
import { toastError, toastSuccess } from '../components/toast.js';

export function createPage({ router, container }) {
  const nodes = {};
  let generation = 0;
  let disposed = false;
  let planning = false;
  let submitting = false;
  let requestController = null;
  let receiptPoller = null;
  let planned = null; // exact {source, manifest, plan, submissionIntent}
  let receipt = null;

  function mount() {
    container.appendChild(buildLayout());
  }

  function dispose() {
    disposed = true;
    generation += 1;
    if (requestController) requestController.abort();
    if (receiptPoller) receiptPoller.stop();
  }

  function stopReceiptPolling() {
    if (receiptPoller) receiptPoller.stop();
    receiptPoller = null;
  }

  function invalidateSource({ clearOutput = true } = {}) {
    generation += 1;
    planned = null;
    receipt = null;
    stopReceiptPolling();
    nodes.submitBtn.disabled = true;
    nodes.submitBtn.textContent = '确认并提交';
    if (clearOutput) clear(nodes.output);
  }

  function setBusy(value) {
    nodes.input.disabled = value;
    nodes.file.disabled = value;
    nodes.exampleBtn.disabled = value;
    nodes.planBtn.disabled = value;
    nodes.submitBtn.disabled = value || !planned || planned.plan.valid !== true || Boolean(receipt);
  }

  async function plan() {
    if (planning || submitting) return;
    const source = nodes.input.value;
    let manifest;
    try {
      manifest = parseBatchSource(source);
    } catch (error) {
      renderError(error.message);
      toastError(error.message);
      nodes.input.focus();
      return;
    }

    const token = generation;
    planning = true;
    setBusy(true);
    nodes.planBtn.textContent = '校验中…';
    requestController = new AbortController();
    try {
      const result = await postJsonText('/job-batches/plan', source, {
        signal: requestController.signal,
      });
      if (disposed || token !== generation || source !== nodes.input.value) return;
      planned = { source, manifest, plan: result, submissionIntent: null };
      receipt = null;
      renderPlan(result);
      if (result.valid === true) {
        toastSuccess(`批量计划校验通过：${result.items?.length || 0} 个 Job。`);
      } else {
        toastError('批量计划校验不通过，请查看逐项详情。');
      }
    } catch (error) {
      if (!isAbort(error) && token === generation) {
        renderError(describeError(error));
        toastError(describeError(error));
      }
    } finally {
      if (token === generation && !disposed) {
        planning = false;
        requestController = null;
        nodes.planBtn.textContent = '校验计划';
        setBusy(false);
      }
    }
  }

  async function submit() {
    if (submitting || planning || !planned || planned.plan.valid !== true) return;
    if (nodes.input.value !== planned.source) {
      invalidateSource();
      renderError('JSON 已在预检后发生变化，请重新校验。');
      return;
    }

    submitting = true;
    setBusy(true);
    const summary = planned.plan.summary || {};
    const confirmed = await confirmDialog({
      title: '确认启动批量 Jobs',
      message: `将接受 ${summary.job_count ?? planned.manifest.jobs.length} 个 Job、`
        + `${summary.total_workers ?? '—'} 个 Workers，最大并发 `
        + `${summary.max_active_jobs ?? planned.manifest.policy?.max_active_jobs ?? 3} 个 Job。`
        + '本次确认会创建一组新的运行和真实资源；仅网络中断重试会复用本次运行身份。',
      confirmLabel: '确认提交',
    });
    if (!confirmed || !planned || nodes.input.value !== planned.source) {
      submitting = false;
      setBusy(false);
      return;
    }

    const token = generation;
    const intent = planned;
    nodes.submitBtn.textContent = '提交中…';
    requestController = new AbortController();
    try {
      if (!intent.submissionIntent) {
        intent.submissionIntent = await claimBatchSubmissionIntent(intent.source);
      }
      const result = await postJsonText('/job-batches', intent.source, {
        headers: { 'Idempotency-Key': intent.submissionIntent.idempotencyKey },
        signal: requestController.signal,
      });
      if (disposed || token !== generation || intent !== planned) return;
      if (!result || typeof result.job_batch_id !== 'string' || !result.job_batch_id) {
        throw new Error('服务器回执缺少 job_batch_id；稳定幂等键已保留，可安全重试。');
      }
      clearBatchSubmissionIntent(intent.submissionIntent);
      receipt = result;
      renderReceipt(result);
      toastSuccess(`批次已接收：${result.job_batch_id}`);
      if (!batchReceiptIsTerminal(result)) startReceiptPolling(result.job_batch_id, token);
    } catch (error) {
      if (!isAbort(error) && token === generation) {
        const message = Number(error && error.status) === 409
          ? '本次待恢复提交的幂等键与另一份 manifest 冲突；请重新预检后再提交。'
          : describeError(error);
        renderError(message, { preservePlan: true });
        toastError(message);
      }
    } finally {
      if (token === generation && !disposed) {
        submitting = false;
        requestController = null;
        nodes.submitBtn.textContent = receipt ? '批次已提交' : '确认并提交';
        setBusy(false);
      }
    }
  }

  function startReceiptPolling(jobBatchId, token) {
    stopReceiptPolling();
    receiptPoller = createPoller({
      name: `job-batch-${jobBatchId}`,
      interval: 3000,
      maxBackoff: 30000,
      immediate: false,
      task: async (signal) => {
        const result = await get(`/job-batches/${encodeURIComponent(jobBatchId)}`, { signal });
        if (disposed || token !== generation || !receipt || receipt.job_batch_id !== jobBatchId) return;
        receipt = result;
        renderReceipt(result);
        if (batchReceiptIsTerminal(result)) {
          stopReceiptPolling();
          toastSuccess(`批次已进入终态：${jobBatchId}`);
        }
      },
    });
    receiptPoller.start();
  }

  function renderPlan(result) {
    clear(nodes.output);
    const items = Array.isArray(result.items) ? result.items : [];
    const valid = result.valid === true;
    const summary = result.summary || {};
    nodes.output.appendChild(el('section', { class: 'card' }, [
      el('h3', { text: valid ? '✓ 批量计划校验通过' : '✗ 批量计划校验不通过' }),
      el('p', { class: 'small mono', text: `batch_id: ${result.batch_id || '—'}` }),
      el('p', {
        class: 'small',
        text: `${summary.job_count ?? items.length} Jobs · ${summary.total_workers ?? '—'} Workers · `
          + `${formatNumber(summary.total_worker_hours)} Worker-hours · 最大并发 ${summary.max_active_jobs ?? '—'}`,
      }),
      renderMessages(result.errors, 'err'),
      renderMessages(result.warnings, 'muted'),
    ]));
    nodes.output.appendChild(renderPlanTable(items));
    nodes.submitBtn.disabled = !valid;
  }

  function renderPlanTable(items) {
    const tbody = el('tbody');
    for (const item of items) {
      tbody.appendChild(el('tr', {}, [
        el('td', { class: 'mono', text: item.client_id || '—' }),
        el('td', { text: item.name || '—' }),
        el('td', {}, [el('span', {
          class: `badge badge-${item.valid ? 'ok' : 'err'}`,
          text: item.valid ? '通过' : '失败',
        })]),
        el('td', { class: 'small' }, [
          renderMessages(item.warnings, 'muted'),
          renderMessages(item.errors, 'err'),
        ]),
      ]));
    }
    return el('div', { class: 'table-wrap' }, [el('table', {}, [
      el('thead', {}, [el('tr', {}, [
        el('th', { text: 'client_id' }),
        el('th', { text: '名称' }),
        el('th', { text: '状态' }),
        el('th', { text: '详情' }),
      ])]),
      tbody,
    ])]);
  }

  function renderReceipt(result) {
    clear(nodes.output);
    const items = Array.isArray(result.items) ? result.items : [];
    const state = String(result.state || 'queued');
    const stateClass = receiptStateClass(state, items);
    nodes.output.appendChild(el('section', { class: 'card' }, [
      el('div', { class: 'spread' }, [
        el('div', {}, [
          el('h3', { text: '批次提交回执' }),
          el('p', { class: 'small mono', text: `batch_id: ${result.batch_id || '—'}` }),
          el('p', { class: 'small mono', text: `job_batch_id: ${result.job_batch_id || '—'}` }),
        ]),
        el('span', { class: `badge badge-${stateClass}`, text: state }),
      ]),
      result.idempotent_replay
        ? el('p', { class: 'small muted', text: '幂等重放：服务端返回了此前已接受的同一批次。' })
        : null,
      !batchReceiptIsTerminal(result)
        ? el('p', { class: 'small muted', text: '正在自动刷新 queued / accepted / terminal / error 状态。' })
        : null,
    ]));

    const tbody = el('tbody');
    for (const item of items) {
      const jobState = item.state === 'terminal' && item.job_state ? ` · ${item.job_state}` : '';
      tbody.appendChild(el('tr', {}, [
        el('td', { class: 'mono', text: item.client_id || '—' }),
        el('td', { text: item.name || '—' }),
        el('td', {}, [item.job_id
          ? el('a', { href: router.href(`/jobs/${encodeURIComponent(item.job_id)}`), class: 'mono', text: item.job_id })
          : el('span', { class: 'mono muted', text: '等待接受' })]),
        el('td', {}, [el('span', {
          class: `badge badge-${itemStateClass(item)}`,
          text: `${item.state || 'queued'}${jobState}`,
        })]),
        el('td', { class: 'small err', text: item.error || '' }),
      ]));
    }
    nodes.output.appendChild(el('div', { class: 'table-wrap' }, [el('table', {}, [
      el('thead', {}, [el('tr', {}, [
        el('th', { text: 'client_id' }),
        el('th', { text: '名称' }),
        el('th', { text: 'Job ID' }),
        el('th', { text: '状态' }),
        el('th', { text: '错误' }),
      ])]),
      tbody,
    ])]));
  }

  function renderMessages(messages, className) {
    const values = Array.isArray(messages) ? messages : [];
    return el('div', { class: className }, values.map((message) => el('div', { text: String(message) })));
  }

  function renderError(message, { preservePlan = false } = {}) {
    if (!preservePlan) clear(nodes.output);
    nodes.output.prepend(el('div', { class: 'card' }, [el('p', { class: 'err', text: message })]));
  }

  async function handleFile(event) {
    const file = event.target.files && event.target.files[0];
    if (!file) return;
    const token = ++generation;
    planned = null;
    receipt = null;
    stopReceiptPolling();
    clear(nodes.output);
    if (file.size === 0 || file.size > JOB_BATCH_MAX_BYTES) {
      const message = file.size === 0 ? 'JSON 文件不能为空。' : '文件不能超过 2 MiB。';
      renderError(message);
      toastError(message);
      event.target.value = '';
      return;
    }
    try {
      const bytes = await file.arrayBuffer();
      if (token !== generation || disposed) return;
      const source = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
      nodes.input.value = source;
      invalidateSource();
      nodes.fileMeta.textContent = `${file.name} · ${bytes.byteLength} bytes · 已载入内存，尚未预检`;
    } catch (_) {
      if (token !== generation) return;
      renderError('文件必须是有效 UTF-8 JSON。');
      toastError('文件必须是有效 UTF-8 JSON。');
    }
  }

  function loadExample() {
    nodes.input.value = JSON.stringify({
      schema_version: 1,
      batch_id: `batch-${Date.now().toString(36)}`,
      policy: { max_active_jobs: 3, on_job_failure: 'continue' },
      jobs: [
        {
          client_id: 'task-1',
          spec: {
            name: 'example-job-1',
            run: { command: 'echo hello from task 1' },
            account: { mode: 'none' },
            fanout: { workers: 1 },
            collect: { paths: ['results'] },
          },
        },
        {
          client_id: 'task-2',
          spec: {
            name: 'example-job-2',
            run: { command: 'echo hello from task 2' },
            account: { mode: 'none' },
            fanout: { workers: 1 },
            collect: { paths: ['results'] },
          },
        },
      ],
    }, null, 2);
    nodes.file.value = '';
    nodes.fileMeta.textContent = '示例已载入内存，尚未预检。';
    invalidateSource();
  }

  function buildLayout() {
    const root = document.createDocumentFragment();
    root.appendChild(el('div', { class: 'page-head' }, [
      el('div', {}, [
        el('h1', { text: '批量提交 Job' }),
        el('p', {
          class: 'page-sub',
          text: '同一份 JSON 可重复运行；每次确认创建新运行，网络中断重试不会重复创建。',
        }),
      ]),
      el('div', { class: 'page-actions' }, [
        el('a', { class: 'btn', href: router.href('/jobs/new'), text: '单个提交' }),
      ]),
    ]));

    const card = el('section', { class: 'card' });
    nodes.file = el('input', {
      type: 'file', accept: '.json,application/json', id: 'batchFile', hidden: true,
    });
    nodes.file.addEventListener('change', handleFile);
    nodes.exampleBtn = el('button', { type: 'button', class: 'btn btn-sm', text: '加载示例' });
    nodes.exampleBtn.addEventListener('click', loadExample);
    nodes.fileMeta = el('span', { class: 'small muted', text: '文件仅保存在当前页面内存中。' });
    card.appendChild(el('div', { class: 'row', style: { 'margin-bottom': '10px' } }, [
      el('label', { class: 'btn btn-sm', for: 'batchFile', text: '上传 JSON 文件' }),
      nodes.exampleBtn,
      nodes.file,
      nodes.fileMeta,
    ]));

    nodes.input = el('textarea', {
      id: 'batchInput',
      placeholder: '粘贴 JobBatchManifest JSON…',
      style: { 'min-height': '360px', 'font-size': '0.82rem' },
      'aria-label': 'JSON manifest',
      spellcheck: 'false',
    });
    nodes.input.addEventListener('input', () => {
      nodes.file.value = '';
      nodes.fileMeta.textContent = '内容已修改，必须重新预检。';
      invalidateSource();
    });
    card.appendChild(el('div', { class: 'field' }, [
      el('label', { for: 'batchInput', text: 'JSON Manifest' }),
      nodes.input,
      el('span', {
        class: 'help',
        text: `请求不超过 2 MiB；schema 硬限 ${JOB_BATCH_SCHEMA_MAX_ITEMS} 个 Job，`
          + `部署默认 ${JOB_BATCH_DEFAULT_MAX_ITEMS} 个，实际限制以服务端预检为准。`,
      }),
    ]));

    nodes.planBtn = el('button', { type: 'button', class: 'btn', text: '校验计划' });
    nodes.planBtn.addEventListener('click', plan);
    nodes.submitBtn = el('button', {
      type: 'button', class: 'btn btn-primary', text: '确认并提交', disabled: true,
    });
    nodes.submitBtn.addEventListener('click', submit);
    card.appendChild(el('div', { class: 'row' }, [nodes.planBtn, nodes.submitBtn]));
    root.appendChild(card);

    nodes.output = el('div', { role: 'status', 'aria-live': 'polite' });
    root.appendChild(nodes.output);
    return root;
  }

  return { mount, dispose };
}

function formatNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '—';
}

function itemStateClass(item) {
  const state = String(item && item.state || '').toLowerCase();
  const jobState = String(item && item.job_state || '').toLowerCase();
  if (state === 'error' || (state === 'terminal' && jobState === 'failed')) return 'err';
  if (state === 'terminal' && jobState === 'cancelled') return 'warn';
  if (state === 'terminal' || state === 'accepted') return 'ok';
  return 'idle';
}

function receiptStateClass(state, items) {
  if (state !== 'terminal') return state === 'running' ? 'ok' : 'idle';
  return items.some((item) => itemStateClass(item) === 'err') ? 'err' : 'ok';
}
