/**
 * Browser administrator-session custody.
 *
 * The opaque session token is held only by the browser's Secure, HttpOnly
 * cookie.  This module keeps the non-secret principal and CSRF token in memory
 * for the current page lifetime; neither value is persisted in web storage.
 */

let currentPrincipal = null;
let currentCsrfToken = '';

/** Remove retired URL credentials and the legacy sessionStorage API-key slot. */
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
  try { window.sessionStorage.removeItem('ea_api_key'); } catch (_) { /* best effort */ }
  return removed;
}

export function initAuth() {
  scrubUrlCredentials();
  clearSession();
}

export function setSession(payload) {
  if (!payload || typeof payload !== 'object') throw new TypeError('invalid administrator session');
  const email = typeof payload.email === 'string' ? payload.email.trim() : '';
  const role = typeof payload.role === 'string' ? payload.role : '';
  const csrfToken = typeof payload.csrf_token === 'string' ? payload.csrf_token : '';
  if (!email || role !== 'admin' || csrfToken.length < 32) {
    throw new TypeError('invalid administrator session');
  }
  currentPrincipal = Object.freeze({ email, role });
  currentCsrfToken = csrfToken;
  return currentPrincipal;
}

export function clearSession() {
  currentPrincipal = null;
  currentCsrfToken = '';
}

export function hasSession() {
  return currentPrincipal !== null && currentCsrfToken.length >= 32;
}

export function sessionPrincipal() {
  return currentPrincipal;
}

/** Only api.js should call this for same-origin state-changing requests. */
export function csrfHeader(method) {
  const normalized = String(method || 'GET').toUpperCase();
  if (['GET', 'HEAD', 'OPTIONS'].includes(normalized)) return {};
  return currentCsrfToken ? { 'X-CSRF-Token': currentCsrfToken } : {};
}
