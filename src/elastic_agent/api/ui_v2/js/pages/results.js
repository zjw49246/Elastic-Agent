/**
 * Results — independent of the Jobs page.
 *
 * The legacy ``/api/results`` scans S3 prefixes, so it is only called on page
 * entry or explicit manual refresh — never on a global timer. Per-Job file
 * details load lazily on expand; downloads stream with cancel support.
 */

import { el, clear, formatBytes, reconcileList } from '../core/dom.js';
import { get, createGeneration } from '../core/api.js';
import { pageCache } from '../core/store.js';
import { describeError } from '../core/errors.js';
import { toastError } from '../components/toast.js';
import { downloadJobResults, isDownloading, cancelDownload, supportsStreamingSave } from '../core/downloads.js';

const PAGE_SIZE = 50;

export function createPage({ router, container }) {
  const cache = pageCache('results');
  if (!cache.filter) cache.filter = { query: '', page: 0 };
  const generation = createGeneration();
  const detailGenerations = new Map();
  const nodes = {};
  let loading = false;

  function mount() {
    container.appendChild(buildLayout());
    if (cache.data) render();
    else void load();
  }

  function dispose() {
    // No poller to stop — results are load-on-demand by design.
  }

  async function load() {
    if (loading) return;
    loading = true;
    const token = generation.next();
    nodes.status.textContent = '加载结果目录…';
    nodes.refreshBtn.disabled = true;
    try {
      const data = await get('/results');
      if (!token.valid) return;
      cache.data = data;
      cache.loadedAt = new Date();
      render();
    } catch (error) {
      if (!token.valid) return;
      nodes.status.textContent = describeError(error);
    } finally {
      loading = false;
      nodes.refreshBtn.disabled = false;
    }
  }

  function visibleRows() {
    let rows = (cache.data && cache.data.jobs) || [];
    const q = (cache.filter.query || '').trim().toLowerCase();
    if (q) rows = rows.filter((row) => String(row.job_id).toLowerCase().includes(q));
    return rows;
  }

  function render() {
    const rows = visibleRows();
    const start = cache.filter.page * PAGE_SIZE;
    if (start >= rows.length && cache.filter.page > 0) {
      cache.filter.page = 0;
      return render();
    }
    const page = rows.slice(start, start + PAGE_SIZE);
    nodes.status.textContent = `${rows.length} 个有结果的 Job · 第 ${cache.filter.page + 1} 页`
      + (cache.loadedAt ? ` · 更新于 ${cache.loadedAt.toLocaleTimeString()}` : '');
    nodes.empty.hidden = page.length > 0;
    reconcileList(nodes.list, page, (r) => r.job_id, (r) => buildCard(r), (node, r) => updateCard(node, r));
    nodes.prev.disabled = cache.filter.page <= 0;
    nodes.next.disabled = start + PAGE_SIZE >= rows.length;
  }

  function buildCard(row) {
    const card = el('details', { class: 'card', style: { 'margin-bottom': '10px' } });
    const summary = el('summary', { class: 'spread' });
    card.appendChild(summary);
    const body = el('div', { class: 'stack', style: { 'margin-top': '10px' } });
    card.appendChild(body);
    card._summary = summary;
    card._body = body;
    card._loaded = false;
    card.addEventListener('toggle', () => {
      if (card.open && !card._loaded) {
        card._loaded = true;
        void loadDetail(card, card._row.job_id);
      }
    });
    updateCard(card, row);
    return card;
  }

  function updateCard(card, row) {
    card._row = row;
    const summary = card._summary;
    clear(summary);
    summary.appendChild(el('span', {}, [
      el('strong', { text: row.job_id }),
      el('span', { class: 'muted small', text: ` · 文件 ${row.file_count ?? '—'}` }),
    ]));
    if (row.s3_uri) summary.appendChild(el('span', { class: 'mono small muted', text: row.s3_uri }));
  }

  async function loadDetail(card, jobId) {
    const body = card._body;
    clear(body);
    body.appendChild(el('p', { class: 'muted small', text: '加载文件明细…' }));
    const gen = createGeneration();
    detailGenerations.set(jobId, gen);
    const token = gen.next();
    try {
      const data = await get(`/jobs/${encodeURIComponent(jobId)}/results`);
      if (!token.valid) return;
      clear(body);

      const head = el('div', { class: 'row' });
      head.appendChild(el('a', { class: 'btn btn-sm', href: router.href(`/jobs/${encodeURIComponent(jobId)}`), text: 'Job 详情' }));
      if (data.s3_uri) {
        const copy = el('button', { type: 'button', class: 'btn btn-sm', text: '复制 S3 URI' });
        copy.addEventListener('click', async () => {
          try { await navigator.clipboard.writeText(data.s3_uri); } catch (_) { toastError('复制失败。'); }
        });
        head.appendChild(copy);
      }
      head.appendChild(buildDownload(jobId));
      body.appendChild(head);

      if (Array.isArray(data.scores) && data.scores.length) {
        const list = el('ul', { class: 'small' });
        for (const score of data.scores.slice(0, 30)) {
          list.appendChild(el('li', { text: `${score.task_id || ''} ${score.status || ''} ${score.final_score ?? ''}`.trim() }));
        }
        body.appendChild(el('div', {}, [el('h3', { text: '分数' }), list]));
      }

      if (Array.isArray(data.files) && data.files.length) {
        const tbody = el('tbody');
        for (const file of data.files) {
          tbody.appendChild(el('tr', {}, [
            el('td', {}, [el('span', { class: 'mono small', text: file.path })]),
            el('td', { text: formatBytes(file.size) }),
          ]));
        }
        body.appendChild(el('div', { class: 'table-wrap' }, [
          el('table', {}, [
            el('thead', {}, [el('tr', {}, [el('th', { text: '文件' }), el('th', { text: '大小' })])]),
            tbody,
          ]),
        ]));
        if (data.file_count > data.files.length) {
          body.appendChild(el('p', { class: 'muted small', text: `仅显示前 ${data.files.length} / ${data.file_count} 个文件。` }));
        }
      }
    } catch (error) {
      if (!token.valid) return;
      clear(body);
      body.appendChild(el('p', { class: 'muted small', text: describeError(error) }));
      card._loaded = false;
    }
  }

  function buildDownload(jobId) {
    const wrap = el('span', { class: 'row' });
    const progress = el('span', { class: 'small muted', role: 'status' });
    const download = el('button', { type: 'button', class: 'btn btn-sm btn-primary', text: '下载全部' });
    const cancel = el('button', { type: 'button', class: 'btn btn-sm', text: '取消', hidden: true });
    download.addEventListener('click', async () => {
      if (isDownloading(jobId)) return;
      download.disabled = true;
      cancel.hidden = false;
      if (!supportsStreamingSave()) progress.textContent = '当前浏览器不支持流式落盘，大文件会占用内存。';
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

  function buildLayout() {
    const root = document.createDocumentFragment();
    nodes.refreshBtn = el('button', { type: 'button', class: 'btn', text: '刷新' });
    nodes.refreshBtn.addEventListener('click', () => load());
    root.appendChild(el('div', { class: 'page-head' }, [
      el('div', {}, [
        el('h1', { text: '结果' }),
        el('p', { class: 'page-sub', text: '覆盖 S3 与 Manager 本地的全部已收集结果；仅在进入页面或手动刷新时加载，不做后台轮询。' }),
      ]),
      el('div', { class: 'page-actions' }, [nodes.refreshBtn]),
    ]));

    const card = el('section', { class: 'card' });
    const search = el('input', { type: 'search', id: 'resultsQuery', placeholder: 'Job ID', value: cache.filter.query || '' });
    search.addEventListener('input', () => {
      cache.filter.query = search.value;
      cache.filter.page = 0;
      render();
    });
    card.appendChild(el('div', { class: 'filters' }, [
      el('div', { class: 'field' }, [el('label', { for: 'resultsQuery', text: '搜索' }), search]),
    ]));
    nodes.status = el('p', { class: 'small muted', role: 'status' });
    card.appendChild(nodes.status);
    root.appendChild(card);

    nodes.list = el('div', {});
    root.appendChild(nodes.list);
    nodes.empty = el('p', { class: 'empty', text: '暂无已收集结果。' });
    root.appendChild(nodes.empty);

    nodes.prev = el('button', { type: 'button', class: 'btn btn-sm', text: '上一页' });
    nodes.next = el('button', { type: 'button', class: 'btn btn-sm', text: '下一页' });
    nodes.prev.addEventListener('click', () => { cache.filter.page -= 1; render(); });
    nodes.next.addEventListener('click', () => { cache.filter.page += 1; render(); });
    root.appendChild(el('div', { class: 'pager' }, [nodes.prev, nodes.next]));
    return root;
  }

  return { mount, dispose };
}
