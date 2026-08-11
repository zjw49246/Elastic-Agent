/**
 * Batch submit — paste or upload a JSON manifest to create multiple Jobs.
 *
 * Uses ``POST /api/job-batches/plan`` for preflight and
 * ``POST /api/job-batches`` with an ``Idempotency-Key`` header for submission.
 *
 * Manifest schema (server-side ``JobBatchManifest``):
 * {
 *   "schema_version": 1,
 *   "batch_id": "my-batch-20260810",
 *   "policy": { "max_active_jobs": 3, "on_job_failure": "continue" },
 *   "jobs": [
 *     { "client_id": "task-1", "spec": { <JobSpec> } },
 *     ...
 *   ]
 * }
 */

import { el, clear } from '../core/dom.js';
import { post } from '../core/api.js';
import { describeError } from '../core/errors.js';
import { toastError, toastSuccess } from '../components/toast.js';

export function createPage({ router, container }) {
  const nodes = {};
  let submitting = false;

  function mount() {
    container.appendChild(buildLayout());
  }

  function dispose() {}

  async function plan() {
    const manifest = parseInput();
    if (!manifest) return;
    nodes.planBtn.disabled = true;
    nodes.planBtn.textContent = '校验中…';
    try {
      const result = await post('/job-batches/plan', manifest);
      renderPlan(result);
      if (result.valid) toastSuccess(`批量计划校验通过：${result.items?.length || 0} 个 Job。`);
      else toastError('批量计划校验不通过，请查看下方详情。');
    } catch (error) {
      renderError(describeError(error));
      toastError(describeError(error));
    } finally {
      nodes.planBtn.disabled = false;
      nodes.planBtn.textContent = '校验计划';
    }
  }

  async function submit() {
    if (submitting) return;
    const manifest = parseInput();
    if (!manifest) return;

    // Generate idempotency key
    const key = (crypto.randomUUID && crypto.randomUUID())
      || `${Date.now()}-${Math.random().toString(36).slice(2)}`;

    submitting = true;
    nodes.submitBtn.disabled = true;
    nodes.submitBtn.textContent = '提交中…';
    try {
      const result = await post('/job-batches', manifest, {
        headers: { 'Idempotency-Key': key },
      });
      renderResult(result);
      toastSuccess(`批量 Job 已提交：${result.batch_id || 'batch'}`);
    } catch (error) {
      renderError(describeError(error));
      toastError(describeError(error));
    } finally {
      submitting = false;
      nodes.submitBtn.disabled = false;
      nodes.submitBtn.textContent = '校验并提交';
    }
  }

  function parseInput() {
    const raw = nodes.input.value.trim();
    if (!raw) {
      toastError('请粘贴或上传 JSON manifest。');
      return null;
    }
    try {
      const parsed = JSON.parse(raw);
      if (typeof parsed !== 'object' || Array.isArray(parsed)) {
        toastError('JSON 必须是一个对象（不是数组）。');
        return null;
      }
      return parsed;
    } catch (error) {
      toastError(`JSON 解析失败：${error.message}`);
      nodes.input.focus();
      return null;
    }
  }

  function renderPlan(plan) {
    clear(nodes.output);
    const items = plan.items || [];
    const valid = plan.valid;

    const summary = el('div', { class: 'card' }, [
      el('h3', { text: valid ? '✓ 批量计划校验通过' : '✗ 批量计划校验不通过' }),
      el('p', { class: 'small', text: `batch_id: ${plan.batch_id || '—'} · ${items.length} 个 Job` }),
    ]);

    if (plan.errors && plan.errors.length) {
      summary.appendChild(el('div', { class: 'err small' }, [
        el('strong', { text: '全局错误：' }),
        ...plan.errors.map((e) => el('div', { text: String(e) })),
      ]));
    }

    const table = el('tbody');
    for (const item of items) {
      const row = el('tr', {}, [
        el('td', { class: 'mono', text: item.client_id }),
        el('td', { text: item.name || '—' }),
        el('td', {}, [
          el('span', {
            class: `badge badge-${item.valid ? 'ok' : 'err'}`,
            text: item.valid ? '通过' : '失败',
          }),
        ]),
        el('td', { class: 'small' }, [
          ...(item.warnings || []).map((w) => el('div', { class: 'muted', text: w })),
          ...(item.errors || []).map((e) => el('div', { class: 'err', text: e })),
        ]),
      ]);
      table.appendChild(row);
    }

    nodes.output.appendChild(summary);
    nodes.output.appendChild(el('div', { class: 'table-wrap' }, [
      el('table', {}, [
        el('thead', {}, [el('tr', {}, [
          el('th', { text: 'client_id' }),
          el('th', { text: '名称' }),
          el('th', { text: '状态' }),
          el('th', { text: '详情' }),
        ])]),
        table,
      ]),
    ]));

    if (plan.summary) {
      nodes.output.appendChild(el('details', {}, [
        el('summary', { text: '计划摘要' }),
        el('pre', { class: 'code', text: JSON.stringify(plan.summary, null, 2) }),
      ]));
    }
  }

  function renderResult(result) {
    clear(nodes.output);
    const items = result.items || [];
    nodes.output.appendChild(el('div', { class: 'card' }, [
      el('h3', { text: '批量 Job 已提交' }),
      el('p', { class: 'small mono', text: `batch_id: ${result.batch_id || '—'}` }),
      result.idempotent_replay
        ? el('p', { class: 'small muted', text: '（幂等重放：此批次之前已提交过）' })
        : null,
    ]));

    if (items.length) {
      const table = el('tbody');
      for (const item of items) {
        const jobId = item.job_id || '—';
        table.appendChild(el('tr', {}, [
          el('td', { class: 'mono', text: item.client_id }),
          el('td', {}, [
            jobId !== '—'
              ? el('a', { href: router.href(`/jobs/${encodeURIComponent(jobId)}`), class: 'mono', text: jobId })
              : el('span', { class: 'mono muted', text: '—' }),
          ]),
          el('td', {}, [
            el('span', {
              class: `badge badge-${item.state === 'error' ? 'err' : item.state === 'accepted' ? 'ok' : 'idle'}`,
              text: item.state || '—',
            }),
          ]),
          el('td', { class: 'small err', text: item.error || '' }),
        ]));
      }
      nodes.output.appendChild(el('div', { class: 'table-wrap' }, [
        el('table', {}, [
          el('thead', {}, [el('tr', {}, [
            el('th', { text: 'client_id' }),
            el('th', { text: 'Job ID' }),
            el('th', { text: '状态' }),
            el('th', { text: '错误' }),
          ])]),
          table,
        ]),
      ]));
    }
  }

  function renderError(message) {
    clear(nodes.output);
    nodes.output.appendChild(el('div', { class: 'card' }, [
      el('p', { class: 'err', text: message }),
    ]));
  }

  function handleFile(event) {
    const file = event.target.files && event.target.files[0];
    if (!file) return;
    if (file.size > 2 * 1024 * 1024) {
      toastError('文件不能超过 2 MiB。');
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      nodes.input.value = reader.result;
      // Try to pretty-print
      try {
        const parsed = JSON.parse(reader.result);
        nodes.input.value = JSON.stringify(parsed, null, 2);
      } catch (_) { /* keep as-is */ }
    };
    reader.readAsText(file);
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
  }

  function buildLayout() {
    const root = document.createDocumentFragment();
    root.appendChild(el('div', { class: 'page-head' }, [
      el('div', {}, [
        el('h1', { text: '批量提交 Job' }),
        el('p', { class: 'page-sub', text: '粘贴或上传 JSON manifest，一次提交多个 Job。每个 Job 独立校验和接受。' }),
      ]),
      el('div', { class: 'page-actions' }, [
        el('a', { class: 'btn', href: router.href('/jobs/new'), text: '单个提交' }),
      ]),
    ]));

    const card = el('section', { class: 'card' });

    // File upload
    const fileInput = el('input', { type: 'file', accept: '.json,application/json', id: 'batchFile' });
    fileInput.addEventListener('change', handleFile);
    const exampleBtn = el('button', { type: 'button', class: 'btn btn-sm', text: '加载示例' });
    exampleBtn.addEventListener('click', loadExample);
    card.appendChild(el('div', { class: 'row', style: { 'margin-bottom': '10px' } }, [
      el('label', { class: 'btn btn-sm', for: 'batchFile', text: '上传 JSON 文件' }),
      fileInput,
      exampleBtn,
    ]));

    // JSON editor
    nodes.input = el('textarea', {
      id: 'batchInput',
      placeholder: '粘贴 JobBatchManifest JSON…\n\n{\n  "schema_version": 1,\n  "batch_id": "my-batch",\n  "jobs": [\n    { "client_id": "task-1", "spec": { ... } }\n  ]\n}',
      style: { 'min-height': '300px', 'font-family': 'ui-monospace, monospace', 'font-size': '0.82rem' },
      'aria-label': 'JSON manifest',
    });
    card.appendChild(el('div', { class: 'field' }, [
      el('label', { for: 'batchInput', text: 'JSON Manifest' }),
      nodes.input,
      el('span', { class: 'help', text: '最多 100 个 Job，总大小不超过 2 MiB。schema_version 必须为 1。' }),
    ]));

    // Buttons
    nodes.planBtn = el('button', { type: 'button', class: 'btn', text: '校验计划' });
    nodes.planBtn.addEventListener('click', plan);
    nodes.submitBtn = el('button', { type: 'button', class: 'btn btn-primary', text: '校验并提交' });
    nodes.submitBtn.addEventListener('click', submit);
    card.appendChild(el('div', { class: 'row' }, [nodes.planBtn, nodes.submitBtn]));

    root.appendChild(card);

    // Output area
    nodes.output = el('div', { role: 'status' });
    root.appendChild(nodes.output);

    return root;
  }

  return { mount, dispose };
}
