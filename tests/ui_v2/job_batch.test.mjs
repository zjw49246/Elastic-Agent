import test from 'node:test';
import assert from 'node:assert/strict';

import {
  JOB_BATCH_MAX_BYTES,
  batchIdempotencyKey,
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

test('batch_id derives a stable intent key and changed identity derives a new key', async () => {
  const first = await batchIdempotencyKey('batch-contract');
  const replay = await batchIdempotencyKey('batch-contract');
  const changed = await batchIdempotencyKey('batch-other');
  assert.equal(first, replay);
  assert.notEqual(first, changed);
  assert.match(first, /^batch-json-v1-[0-9a-f]{64}$/);
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
