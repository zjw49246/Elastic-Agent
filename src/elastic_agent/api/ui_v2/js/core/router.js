/**
 * History API router with a configurable base path.
 *
 * The canary mounts the app under ``/ui-v2``; the base is derived from the
 * document location so the same bundle also works from a clean root. Only UI
 * routes are handled — ``/api/*`` and ``/ws/*`` are never claimed.
 */

export class Router {
  constructor({ base, routes, onNavigate }) {
    this.base = normalizeBase(base);
    this.routes = routes.map(({ pattern, ...rest }) => ({ ...rest, ...compile(pattern) }));
    this.onNavigate = onNavigate;
    this.current = null;
    this._onPop = () => this.resolve();
    this._onClick = (event) => this._interceptLink(event);
  }

  start() {
    window.addEventListener('popstate', this._onPop);
    document.addEventListener('click', this._onClick);
    this.resolve();
    return this;
  }

  stop() {
    window.removeEventListener('popstate', this._onPop);
    document.removeEventListener('click', this._onClick);
  }

  /** Absolute href for a logical path, e.g. ``/jobs/abc`` → ``/ui-v2/jobs/abc``. */
  href(path) {
    const clean = path.startsWith('/') ? path : `/${path}`;
    return `${this.base}${clean}` || '/';
  }

  navigate(path, { replace = false } = {}) {
    const url = this.href(path);
    if (replace) window.history.replaceState({}, '', url);
    else window.history.pushState({}, '', url);
    this.resolve();
  }

  /** Logical path (base stripped) for the current location. */
  logicalPath() {
    let path = window.location.pathname;
    if (this.base && path.startsWith(this.base)) path = path.slice(this.base.length);
    if (!path.startsWith('/')) path = `/${path}`;
    if (path.length > 1 && path.endsWith('/')) path = path.slice(0, -1);
    return path;
  }

  resolve() {
    const path = this.logicalPath();
    const query = new URLSearchParams(window.location.search);
    for (const route of this.routes) {
      const match = route.regex.exec(path);
      if (!match) continue;
      const params = {};
      route.keys.forEach((key, index) => {
        params[key] = decodeURIComponent(match[index + 1]);
      });
      const resolved = { name: route.name, path, params, query, load: route.load };
      this.current = resolved;
      this.onNavigate(resolved);
      return resolved;
    }
    const resolved = { name: 'not-found', path, params: {}, query, load: null };
    this.current = resolved;
    this.onNavigate(resolved);
    return resolved;
  }

  _interceptLink(event) {
    if (event.defaultPrevented || event.button !== 0) return;
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    const anchor = event.target.closest && event.target.closest('a[href]');
    if (!anchor) return;
    if (anchor.target && anchor.target !== '_self') return;
    if (anchor.hasAttribute('download')) return;
    const url = new URL(anchor.href, window.location.href);
    if (url.origin !== window.location.origin) return;
    // API, WebSocket and download URLs must reach the network, never the SPA.
    if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/ws/')) return;
    if (this.base && !url.pathname.startsWith(this.base)) return;
    event.preventDefault();
    this.navigate(url.pathname.slice(this.base.length) + url.search);
  }
}

/** Detect the mount point from the current location (``/ui-v2`` or ``''``). */
export function detectBase() {
  const path = window.location.pathname;
  const match = /^\/ui-v2(?=\/|$)/.exec(path);
  return match ? match[0] : '';
}

function normalizeBase(base) {
  if (!base) return '';
  const trimmed = base.endsWith('/') ? base.slice(0, -1) : base;
  return trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
}

function compile(pattern) {
  const keys = [];
  const source = pattern
    .split('/')
    .map((segment) => {
      if (!segment) return '';
      if (segment.startsWith(':')) {
        keys.push(segment.slice(1));
        return '/([^/]+)';
      }
      return `/${segment.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`;
    })
    .join('');
  return { regex: new RegExp(`^${source || '/'}$`), keys };
}
