import test from 'node:test';
import assert from 'node:assert/strict';

import {
  JOB_BATCH_MAX_BYTES,
  claimBatchSubmissionIntent,
  clearBatchSubmissionIntent,
  parseBatchSource,
  utf8ByteLength,
} from '../../src/elastic_agent/api/ui_v2/js/core/job-batch.js';
import { postJsonText } from '../../src/elastic_agent/api/ui_v2/js/core/api.js';

const manifest = (overrides = {}) => ({
  schema_version: 1,
  batch_id: 'batch-contract',
  policy: { max_active_jobs: 2, on_job_failure: 'continue' },
  jobs: [{
    client_id: 'item-1',
    spec: { name: 'one', run: { command: 'true' } },
  }],
  ...overrides,
});

test('strict parser accepts a valid manifest and rejects null', () => {
  assert.equal(parseBatchSource(JSON.stringify(manifest())).batch_id, 'batch-contract');
  assert.throws(() => parseBatchSource('null'), /顶层必须是 object/);
});

test('strict parser accepts 500 active jobs and rejects 501', () => {
  const atLimit = manifest({
    policy: { max_active_jobs: 500, on_job_failure: 'continue' },
  });
  assert.equal(parseBatchSource(JSON.stringify(atLimit)).policy.max_active_jobs, 500);
  const aboveLimit = manifest({
    policy: { max_active_jobs: 501, on_job_failure: 'continue' },
  });
  assert.throws(() => parseBatchSource(JSON.stringify(aboveLimit)), /1–500/);
});

test('strict parser rejects semantic duplicate keys before transport', () => {
  const source = '{"schema_version":1,"batch_id":"batch-contract",'
    + '"jobs":[{"client_id":"item-1","spec":{"run":{"command":"true"}}}],'
    + '"a":1,"\\u0061":2}';
  assert.throws(() => parseBatchSource(source), /重复 object key/);
});

test('schema hard limit and UTF-8 byte limit are enforced locally', () => {
  const jobs = Array.from({ length: 101 }, (_, index) => ({
    client_id: `item-${index}`,
    spec: { run: { command: 'true' } },
  }));
  assert.throws(() => parseBatchSource(JSON.stringify(manifest({ jobs }))), /最多允许 100/);
  assert.equal(utf8ByteLength('批'), 3);
  assert.throws(() => parseBatchSource(`"${'a'.repeat(JOB_BATCH_MAX_BYTES)}"`), /2 MiB/);
});

test('one pending run retries safely, then the same JSON gets a new execution key', async () => {
  const values = new Map();
  const storage = {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
    removeItem: (key) => values.delete(key),
  };
  let nextByte = 1;
  const cryptoApi = {
    subtle: globalThis.crypto.subtle,
    getRandomValues: (bytes) => {
      bytes.fill(nextByte);
      nextByte += 1;
      return bytes;
    },
  };
  const source = JSON.stringify(manifest());
  const first = await claimBatchSubmissionIntent(source, { cryptoApi, storage });
  const retry = await claimBatchSubmissionIntent(source, { cryptoApi, storage });
  assert.equal(retry.idempotencyKey, first.idempotencyKey);
  assert.equal(retry.recovered, true);
  assert.match(first.idempotencyKey, /^batch-json-v2-[0-9a-f]{64}$/);

  clearBatchSubmissionIntent(first, { storage });
  const rerun = await claimBatchSubmissionIntent(source, { cryptoApi, storage });
  assert.notEqual(rerun.idempotencyKey, first.idempotencyKey);
  assert.equal(rerun.recovered, false);
});

test('raw JSON transport preserves exact source bytes', async () => {
  const previousWindow = globalThis.window;
  const previousFetch = globalThis.fetch;
  const source = '{\n  "schema_version": 1,\n  "batch_id": "exact",\n  "jobs": []\n}\n';
  let captured;
  globalThis.window = { location: { origin: 'https://manager.test' } };
  globalThis.fetch = async (url, init) => {
    captured = { url, init };
    return new Response('{"valid":true}', {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };
  try {
    await postJsonText('/job-batches/plan', source);
    assert.equal(captured.url, '/api/job-batches/plan');
    assert.equal(captured.init.body, source);
    assert.equal(captured.init.headers['Content-Type'], 'application/json');
  } finally {
    globalThis.window = previousWindow;
    globalThis.fetch = previousFetch;
  }
});

test('structured JobBatch 422 details remain actionable', async () => {
  const previousWindow = globalThis.window;
  const previousFetch = globalThis.fetch;
  globalThis.window = { location: { origin: 'https://manager.test' } };
  globalThis.fetch = async () => new Response(JSON.stringify({
    detail: {
      message: 'Job batch preflight failed; no batch was accepted',
      errors: ['job_count exceeds configured maximum 20'],
      items: [{ client_id: 'item-7', errors: ['account capacity is insufficient'] }],
    },
  }), {
    status: 422,
    headers: { 'content-type': 'application/json' },
  });
  try {
    await assert.rejects(
      postJsonText('/job-batches', JSON.stringify(manifest())),
      /item-7: account capacity is insufficient/,
    );
  } finally {
    globalThis.window = previousWindow;
    globalThis.fetch = previousFetch;
  }
});
