/**
 * Authenticated, cancellable result downloads.
 *
 * The Bearer header is mandatory, so a plain ``<a href>`` is not an option —
 * that would put the API Key in a URL. One download per Job is allowed at a
 * time; cancelling aborts the fetch and cancels the reader so the Manager stops
 * streaming from S3.
 */

import { rawFetch } from './api.js';
import { ApiError, isAbort } from './errors.js';

const active = new Map();

export function isDownloading(jobId) {
  return active.has(jobId);
}

export function activeDownloadCount() {
  return active.size;
}

export function cancelDownload(jobId) {
  const entry = active.get(jobId);
  if (!entry) return false;
  entry.cancelled = true;
  entry.controller.abort();
  return true;
}

export function cancelAllDownloads() {
  for (const jobId of Array.from(active.keys())) cancelDownload(jobId);
}

/**
 * Stream ``/api/jobs/{id}/results/download/stream`` to disk.
 *
 * Uses the File System Access API when available so large archives never have
 * to be buffered whole. Without it we fall back to an in-memory blob and warn
 * the caller through ``onProgress`` metadata.
 */
export async function downloadJobResults(jobId, { onProgress, onDone } = {}) {
  if (active.has(jobId)) {
    throw new Error('该 Job 已有下载进行中。');
  }
  const controller = new AbortController();
  const entry = { controller, cancelled: false };
  active.set(jobId, entry);
  const filename = `${safeName(jobId)}-results.tar.gz`;
  let writable = null;
  let reader = null;

  try {
    const response = await rawFetch(
      `/jobs/${encodeURIComponent(jobId)}/results/download/stream`,
      { signal: controller.signal },
    );
    if (!response.ok) {
      throw new ApiError({
        status: response.status,
        method: 'GET',
        path: `/jobs/${jobId}/results/download/stream`,
        message: response.status === 404
          ? '该 Job 暂无可下载结果。'
          : `下载失败（HTTP ${response.status}）。`,
      });
    }

    const expected = Number(response.headers.get('X-Elastic-Agent-Source-Bytes')) || 0;
    const objects = Number(response.headers.get('X-Elastic-Agent-Object-Count')) || 0;
    if (onProgress) onProgress({ received: 0, expected, objects, streaming: true });

    const handle = await pickSaveHandle(filename);
    if (handle) writable = await handle.createWritable();

    const chunks = [];
    let received = 0;
    reader = response.body.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      received += value.byteLength;
      if (writable) await writable.write(value);
      else chunks.push(value);
      if (onProgress) onProgress({ received, expected, objects, streaming: Boolean(writable) });
    }

    if (writable) {
      await writable.close();
      writable = null;
    } else {
      saveBlob(new Blob(chunks, { type: 'application/gzip' }), filename);
    }
    if (onDone) onDone({ received, filename });
    return { received, filename };
  } catch (error) {
    if (writable) {
      try { await writable.abort(); } catch (_) { /* already closed */ }
    }
    if (reader) {
      try { await reader.cancel(); } catch (_) { /* stream already ended */ }
    }
    if (isAbort(error) || entry.cancelled) {
      const cancelled = new Error('下载已取消。');
      cancelled.aborted = true;
      throw cancelled;
    }
    throw error;
  } finally {
    active.delete(jobId);
  }
}

async function pickSaveHandle(filename) {
  if (typeof window.showSaveFilePicker !== 'function') return null;
  try {
    return await window.showSaveFilePicker({
      suggestedName: filename,
      types: [{ description: 'gzip archive', accept: { 'application/gzip': ['.tar.gz'] } }],
    });
  } catch (_) {
    // User dismissed the picker, or the browser refused: fall back to a blob.
    return null;
  }
}

function saveBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  setTimeout(() => URL.revokeObjectURL(url), 30000);
}

function safeName(jobId) {
  return String(jobId).replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 120) || 'job';
}

export function supportsStreamingSave() {
  return typeof window.showSaveFilePicker === 'function';
}
