import test from 'node:test';
import assert from 'node:assert/strict';

const storageCalls = [];
globalThis.window = {
  location: {
    href: 'https://manager.example/ui-v2/jobs/batch?api_key=retired',
    origin: 'https://manager.example',
  },
  history: {
    state: null,
    replaceState(_state, _title, path) {
      this.path = path;
    },
  },
  sessionStorage: {
    removeItem(key) { storageCalls.push(['remove', key]); },
    getItem(key) { storageCalls.push(['get', key]); return null; },
    setItem(key, value) { storageCalls.push(['set', key, value]); },
  },
};

const auth = await import('../../src/elastic_agent/api/ui_v2/js/core/auth.js');
const api = await import('../../src/elastic_agent/api/ui_v2/js/core/api.js');

test('administrator session keeps only CSRF in memory and removes retired key', () => {
  auth.initAuth();
  assert.deepEqual(storageCalls, [['remove', 'ea_api_key']]);
  assert.equal(window.history.path, '/ui-v2/jobs/batch');
  assert.equal(auth.hasSession(), false);

  const principal = auth.setSession({
    email: 'owner@example.test',
    role: 'admin',
    csrf_token: 'c'.repeat(43),
  });
  assert.deepEqual(principal, { email: 'owner@example.test', role: 'admin' });
  assert.deepEqual(auth.csrfHeader('GET'), {});
  assert.deepEqual(auth.csrfHeader('POST'), { 'X-CSRF-Token': 'c'.repeat(43) });
  assert.equal(auth.hasSession(), true);
  assert.equal(storageCalls.some(([operation]) => operation === 'set'), false);
  assert.equal(storageCalls.some(([operation]) => operation === 'get'), false);

  auth.clearSession();
  assert.equal(auth.hasSession(), false);
  assert.deepEqual(auth.csrfHeader('DELETE'), {});
});

test('malformed session payload is rejected', () => {
  assert.throws(
    () => auth.setSession({ email: 'owner@example.test', role: 'admin', csrf_token: 'short' }),
    /invalid administrator session/,
  );
});

test('API client sends same-origin cookie mode and CSRF only on mutations', async () => {
  auth.setSession({
    email: 'owner@example.test',
    role: 'admin',
    csrf_token: 'z'.repeat(43),
  });
  const observed = [];
  globalThis.fetch = async (url, init) => {
    observed.push({ url, init });
    return new Response(JSON.stringify({ ok: true }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  };

  await api.get('/health');
  await api.post('/jobs/plan', { name: 'bounded-test' });

  assert.equal(observed.length, 2);
  assert.equal(observed[0].init.credentials, 'same-origin');
  assert.equal(observed[0].init.headers['X-CSRF-Token'], undefined);
  assert.equal(observed[1].init.credentials, 'same-origin');
  assert.equal(observed[1].init.headers['X-CSRF-Token'], 'z'.repeat(43));
  assert.equal(observed[1].init.headers.Authorization, undefined);
});
