/**
 * Manager REST client.
 *
 * Adds the Bearer header, decodes JSON, and converts failures into ApiError
 * objects that carry status/method/path but never the request body.
 */

import { authHeader } from './auth.js';
import { ApiError, isAbort } from './errors.js';

const API_ROOT = '/api';

let unauthorizedHandler = null;
let unauthorizedNotified = false;

export function onUnauthorized(handler) {
  unauthorizedHandler = handler;
}

/** Called after a successful re-auth so the next 401 fires the handler again. */
export function resetUnauthorizedLatch() {
  unauthorizedNotified = false;
}

function buildUrl(path, query) {
  const base = path.startsWith('/') ? path : `/${path}`;
  const url = new URL(API_ROOT + base, window.location.origin);
  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (value === undefined || value === null || value === '') continue;
      url.searchParams.set(key, String(value));
    }
  }
  return url.pathname + url.search;
}

async function safeDetail(response) {
  const type = response.headers.get('content-type') || '';
  try {
    if (type.includes('application/json')) {
      const body = await response.json();
      const detail = body && body.detail;
      if (typeof detail === 'string') return detail;
      if (Array.isArray(detail)) {
        return detail
          .map((item) => {
            const loc = Array.isArray(item.loc) ? item.loc.filter((p) => p !== 'body').join('.') : '';
            return loc ? `${loc}: ${item.msg}` : String(item.msg || '');
          })
          .filter(Boolean)
          .join('；');
      }
      return '';
    }
    const raw = await response.text();
    return raw.slice(0, 300);
  } catch (_) {
    return '';
  }
}

/**
 * Perform an authenticated request.
 *
 * @param {string} method
 * @param {string} path Path relative to /api
 * @param {object} [options] {body, query, signal, headers}
 */
export async function request(method, path, options = {}) {
  const { body, query, signal, headers = {} } = options;
  const url = buildUrl(path, query);
  const init = {
    method,
    headers: { Accept: 'application/json', ...authHeader(), ...headers },
    signal,
    credentials: 'omit',
    cache: 'no-store',
    referrerPolicy: 'no-referrer',
  };
  if (body !== undefined) {
    init.headers['Content-Type'] = 'application/json';
    init.body = JSON.stringify(body);
  }

  let response;
  try {
    response = await fetch(url, init);
  } catch (error) {
    if (isAbort(error)) throw error;
    throw new ApiError({ status: 0, method, path, message: '网络请求失败，请检查连接。' });
  }

  if (response.status === 401) {
    if (!unauthorizedNotified && unauthorizedHandler) {
      unauthorizedNotified = true;
      try { unauthorizedHandler(); } catch (_) { /* ignore */ }
    }
    throw new ApiError({ status: 401, method, path, message: '认证失败：API Key 无效或已更换。' });
  }

  if (!response.ok) {
    const detail = await safeDetail(response);
    throw new ApiError({
      status: response.status,
      method,
      path,
      detail,
      message: detail || `请求失败（HTTP ${response.status}）`,
    });
  }

  if (response.status === 204) return null;
  const type = response.headers.get('content-type') || '';
  if (!type.includes('application/json')) return await response.text();
  return await response.json();
}

export const get = (path, options) => request('GET', path, options);
export const post = (path, body, options = {}) => request('POST', path, { ...options, body });
export const put = (path, body, options = {}) => request('PUT', path, { ...options, body });
export const del = (path, options) => request('DELETE', path, options);

/** Raw authenticated fetch — used by streaming downloads. */
export function rawFetch(path, options = {}) {
  const url = buildUrl(path, options.query);
  return fetch(url, {
    method: options.method || 'GET',
    headers: { ...authHeader(), ...(options.headers || {}) },
    signal: options.signal,
    credentials: 'omit',
    cache: 'no-store',
    referrerPolicy: 'no-referrer',
  });
}

/**
 * Guard against late responses overwriting newer state.
 *
 * Each call to ``next()`` invalidates every earlier token, so a slow request
 * for page 1 cannot clobber the rendered page 2.
 */
export function createGeneration() {
  let current = 0;
  return {
    next() {
      current += 1;
      const mine = current;
      return { get valid() { return mine === current; }, id: mine };
    },
    get value() { return current; },
  };
}

export function encodeId(value) {
  return encodeURIComponent(String(value));
}
