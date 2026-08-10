/**
 * Immutable global store plus per-page caches.
 *
 * Secrets are structurally excluded: ``set()`` rejects any key listed in
 * NEVER_STORE so a password, mailbox token, Agent API Key, OTP or secret env
 * value cannot be parked in application state.
 */

export const NEVER_STORE = Object.freeze([
  'password',
  'api_key',
  'apiKey',
  'email_token',
  'emailToken',
  'otp',
  'code',
  'secret_env',
  'secretEnv',
]);

function assertNoSecrets(patch, path = '') {
  if (!patch || typeof patch !== 'object') return;
  for (const [key, value] of Object.entries(patch)) {
    if (NEVER_STORE.includes(key)) {
      throw new Error(`拒绝把敏感字段写入 store：${path}${key}`);
    }
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      assertNoSecrets(value, `${path}${key}.`);
    }
  }
}

const initialState = Object.freeze({
  route: null,
  authed: false,
  health: null,
  summary: null,
  loginAttempts: [],
  theme: 'light',
});

let state = initialState;
const subscribers = new Set();

export function getState() {
  return state;
}

export function setState(patch) {
  assertNoSecrets(patch);
  const next = Object.freeze({ ...state, ...patch });
  if (next === state) return state;
  state = next;
  for (const listener of Array.from(subscribers)) {
    try { listener(state); } catch (_) { /* a bad subscriber must not stall others */ }
  }
  return state;
}

export function subscribe(listener) {
  subscribers.add(listener);
  return () => subscribers.delete(listener);
}

/** Page-scoped cache: survives navigation within the session, never persisted. */
const pageCaches = new Map();

export function pageCache(name) {
  if (!pageCaches.has(name)) pageCaches.set(name, {});
  return pageCaches.get(name);
}

export function clearPageCaches() {
  pageCaches.clear();
}

/**
 * In-memory Job form draft.
 *
 * Kept out of session/local storage on purpose: ``run.env`` and Harness code
 * routinely contain material we must not persist in the browser.
 */
let jobDraft = null;

export function getJobDraft() {
  return jobDraft;
}

export function setJobDraft(draft) {
  jobDraft = draft ? { ...draft } : null;
}

export function clearJobDraft() {
  jobDraft = null;
}

export function resetStore() {
  state = initialState;
  clearPageCaches();
  clearJobDraft();
}
