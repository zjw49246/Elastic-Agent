/**
 * Safe error objects for the UI.
 *
 * ``ApiError`` deliberately keeps only status/method/path/message. It never
 * carries the request body, so a rejected password, mailbox token, Agent API
 * Key, OTP or ``run.secret_env`` value cannot resurface in a toast, a console
 * trace or an unhandled-rejection handler.
 */

export class ApiError extends Error {
  constructor({ status, message, method, path, detail }) {
    super(message || `请求失败（HTTP ${status}）`);
    this.name = 'ApiError';
    this.status = Number(status) || 0;
    this.method = method || 'GET';
    this.path = path || '';
    this.detail = typeof detail === 'string' ? detail : '';
  }

  get isAuth() {
    return this.status === 401;
  }

  get isUnconfigured() {
    return this.status === 503;
  }

  get isRetryable() {
    return this.status === 0 || this.status === 429 || this.status >= 500;
  }
}

export class AbortedError extends Error {
  constructor(message = 'aborted') {
    super(message);
    this.name = 'AbortedError';
    this.aborted = true;
  }
}

export function isAbort(error) {
  return Boolean(error) && (error.name === 'AbortError' || error.aborted === true);
}

/** Human-readable, secret-free message for any thrown value. */
export function describeError(error) {
  if (!error) return '未知错误';
  if (error instanceof ApiError) {
    if (error.status === 401) return '管理员登录已失效，请重新登录。';
    if (error.status === 403) return '该操作已被服务端禁用。';
    if (error.status === 404) return error.message || '资源不存在。';
    if (error.status === 409) return error.message || '状态冲突，操作未执行。';
    if (error.status === 410) return error.message || '该请求已过期。';
    if (error.status === 413) return error.message || '响应超出大小限制。';
    if (error.status === 503) return error.message || '服务暂时不可用。';
    return error.message;
  }
  if (error instanceof Error) return error.message || String(error.name);
  return '未知错误';
}

/** Install global handlers that never print secrets. */
export function installGlobalErrorHandlers(report) {
  const handle = (error) => {
    if (isAbort(error)) return;
    try {
      report(describeError(error));
    } catch (_) {
      /* reporting must never itself throw */
    }
  };
  window.addEventListener('error', (event) => handle(event.error || new Error(event.message)));
  window.addEventListener('unhandledrejection', (event) => {
    event.preventDefault();
    handle(event.reason);
  });
}
