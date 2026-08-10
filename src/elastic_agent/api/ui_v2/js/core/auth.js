/**
 * API Key custody.
 *
 * The key lives in a module-private variable plus ``sessionStorage`` only.
 * It is never written to localStorage, a cookie, the DOM, a query string or a
 * download URL; ``api.js`` is the only consumer and it only sends it as an
 * ``Authorization: Bearer`` header.
 */

const STORAGE_KEY = 'ea_api_key';

let currentKey = '';
const listeners = new Set();

/**
 * Strip any ``api_key`` present in the URL before it can be read as a
 * credential. The value is discarded, not adopted: URLs leak through history,
 * referrers and proxy logs, so a key that arrived that way is already burnt.
 */
export function scrubUrlCredentials() {
  let removed = false;
  const url = new URL(window.location.href);
  for (const name of ['api_key', 'apikey', 'token', 'key']) {
    if (url.searchParams.has(name)) {
      url.searchParams.delete(name);
      removed = true;
    }
  }
  if (url.hash && /(?:^|[#&?])(api_key|apikey|token)=/i.test(url.hash)) {
    url.hash = '';
    removed = true;
  }
  if (removed) {
    window.history.replaceState(window.history.state, '', url.pathname + url.search + url.hash);
  }
  return removed;
}

export function initAuth() {
  scrubUrlCredentials();
  // Server-injected token (no manual input needed when behind Cloudflare Access).
  const meta = document.querySelector('meta[name="ea-auth-token"]');
  const injected = meta ? (meta.getAttribute('content') || '').trim() : '';
  if (injected) {
    currentKey = injected;
    return true;
  }
  let stored = '';
  try {
    stored = window.sessionStorage.getItem(STORAGE_KEY) || '';
  } catch (_) {
    stored = '';
  }
  currentKey = typeof stored === 'string' ? stored.trim() : '';
  return Boolean(currentKey);
}

export function hasKey() {
  return Boolean(currentKey);
}

/** Only ``api.js`` should call this. */
export function authHeader() {
  return currentKey ? { Authorization: `Bearer ${currentKey}` } : {};
}

export function setKey(value) {
  currentKey = typeof value === 'string' ? value.trim() : '';
  try {
    if (currentKey) window.sessionStorage.setItem(STORAGE_KEY, currentKey);
    else window.sessionStorage.removeItem(STORAGE_KEY);
  } catch (_) {
    /* private mode: memory-only key is still usable for this tab */
  }
  emit();
  return currentKey;
}

export function forgetKey() {
  setKey('');
}

export function onAuthChange(listener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function emit() {
  for (const listener of Array.from(listeners)) {
    try { listener(hasKey()); } catch (_) { /* never break the caller */ }
  }
}
