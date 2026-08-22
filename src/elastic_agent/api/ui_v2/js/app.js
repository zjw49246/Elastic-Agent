/**
 * Elastic-Agent UI v2 entry point.
 *
 * Boot order is fixed (see docs/ui-v2-implementation-plan.md §7.2):
 *   1. scrub retired URL/storage credentials
 *   2. validate the HttpOnly administrator session and retain CSRF in memory
 *   3. install safe global error handlers
 *   4. build router + shell
 *   5. restore the deep link
 *   6. redirect expired sessions to login while preserving the deep link
 *   7. start the global health/OTP pollers
 *   8. mount the page and start its own loaders
 */

import { initAuth, hasSession, setSession, clearSession } from './core/auth.js';
import { get, post, onUnauthorized } from './core/api.js';
import { installGlobalErrorHandlers, describeError, isAbort } from './core/errors.js';
import { Router, detectBase } from './core/router.js';
import { createPoller, stopAllPollers } from './core/poller.js';
import { setState, getState } from './core/store.js';
import { cancelAllDownloads } from './core/downloads.js';
import { el, clear } from './core/dom.js';
import { AppShell } from './components/app-shell.js';
import { toastError } from './components/toast.js';

const ROUTES = [
  { name: 'overview', pattern: '/overview', load: () => import('./pages/overview.js') },
  { name: 'accounts', pattern: '/accounts', load: () => import('./pages/accounts.js') },
  { name: 'account-new', pattern: '/accounts/new', load: () => import('./pages/account-new.js') },
  { name: 'job-new', pattern: '/jobs/new', load: () => import('./pages/job-new.js') },
  { name: 'job-batch', pattern: '/jobs/batch', load: () => import('./pages/job-batch.js') },
  { name: 'jobs', pattern: '/jobs', load: () => import('./pages/jobs.js') },
  { name: 'job-detail', pattern: '/jobs/:jobId', load: () => import('./pages/job-detail.js') },
  { name: 'results', pattern: '/results', load: () => import('./pages/results.js') },
  { name: 'fleet', pattern: '/fleet', load: () => import('./pages/fleet.js') },
  { name: 'root', pattern: '/', load: null },
];

const pageRoot = document.getElementById('pageRoot');
const mainEl = document.getElementById('ea-main');

let shell = null;
let router = null;
let currentPage = null;
let globalPollers = [];
let navigationToken = 0;

async function boot() {
  initAuth();
  installGlobalErrorHandlers((message) => toastError(message));
  onUnauthorized(() => handleUnauthorized());

  router = new Router({
    base: detectBase(),
    routes: ROUTES,
    onNavigate: (route) => { void handleNavigate(route); },
  });

  shell = new AppShell({ router, onAuthClick: () => { void logout(); } }).mount();
  setState({ authed: false, principal: null });

  let session;
  try {
    session = await get('/auth/me');
  } catch (error) {
    if (Number(error && error.status) === 401) return;
    renderPageError(error);
    return;
  }
  if (session.must_change_password === true) {
    redirectToPasswordChange();
    return;
  }
  const principal = setSession(session);
  setState({ authed: true, principal });

  document.getElementById('ea-app').dataset.state = 'ready';
  router.start();
  startGlobalPollers();
}

async function handleNavigate(route) {
  const token = ++navigationToken;
  setState({ route: { name: route.name, path: route.path } });

  if (route.name === 'root') {
    router.navigate('/overview', { replace: true });
    return;
  }

  if (currentPage && typeof currentPage.dispose === 'function') {
    try { currentPage.dispose(); } catch (_) { /* a broken page must not block navigation */ }
  }
  currentPage = null;
  clear(pageRoot);
  shell.setActiveRoute(route.name);

  if (!route.load) {
    renderNotFound(route.path);
    return;
  }

  try {
    const module = await route.load();
    if (token !== navigationToken) return;
    const page = module.createPage({
      router,
      params: route.params,
      query: route.query,
      container: pageRoot,
    });
    currentPage = page;
    page.mount();
    focusMain();
  } catch (error) {
    if (isAbort(error) || token !== navigationToken) return;
    renderPageError(error);
  }
}

function focusMain() {
  const heading = pageRoot.querySelector('h1');
  if (heading) {
    heading.setAttribute('tabindex', '-1');
    heading.focus({ preventScroll: false });
  } else {
    mainEl.focus();
  }
}

function renderNotFound(path) {
  pageRoot.appendChild(el('div', { class: 'card' }, [
    el('h1', { text: '页面不存在' }),
    el('p', { class: 'muted', text: `没有匹配的界面路由：${path}` }),
    el('p', {}, [el('a', { class: 'btn', href: router.href('/overview'), text: '返回总览' })]),
  ]));
  focusMain();
}

function renderPageError(error) {
  pageRoot.appendChild(el('div', { class: 'card' }, [
    el('h1', { text: '页面加载失败' }),
    el('p', { class: 'muted', text: describeError(error) }),
  ]));
  focusMain();
}

// ---------------------------------------------------------------- auth flow

function handleUnauthorized() {
  stopAllPollers();
  globalPollers = [];
  clearSession();
  setState({ authed: false, principal: null });
  redirectToLogin();
}

function safeUiPath() {
  const path = window.location.pathname;
  return path === '/ui-v2' || path.startsWith('/ui-v2/')
    ? path
    : '/ui-v2/overview';
}

function redirectToLogin() {
  window.location.replace(`/login?next=${encodeURIComponent(safeUiPath())}`);
}

function redirectToPasswordChange() {
  window.location.replace(`/change-password?next=${encodeURIComponent(safeUiPath())}`);
}

async function logout() {
  cancelAllDownloads();
  stopAllPollers();
  globalPollers = [];
  try {
    await post('/auth/logout', {});
  } catch (error) {
    if (Number(error && error.status) !== 401) toastError(describeError(error));
  } finally {
    clearSession();
    setState({
      authed: false,
      principal: null,
      health: null,
      summary: null,
      loginAttempts: [],
    });
    redirectToLogin();
  }
}

// --------------------------------------------------------- global pollers

function startGlobalPollers() {
  stopGlobalPollers();
  if (!hasSession()) return;

  const health = createPoller({
    name: 'health',
    interval: 30000,
    task: async (signal) => {
      const data = await get('/health', { signal });
      setState({ health: data });
    },
  });

  const summary = createPoller({
    name: 'summary',
    interval: 10000,
    task: async (signal) => {
      const data = await loadSummary(signal);
      setState({ summary: data });
    },
  });

  const otp = createPoller({
    name: 'otp',
    interval: 5000,
    task: async (signal) => {
      const data = await get('/accounts/login-attempts', { signal });
      setState({ loginAttempts: Array.isArray(data.attempts) ? data.attempts : [] });
    },
  });

  globalPollers = [health, summary, otp];
  for (const poller of globalPollers) poller.start();
}

function stopGlobalPollers() {
  for (const poller of globalPollers) poller.stop();
  globalPollers = [];
}

/**
 * Prefer the lightweight ``/api/ui/summary``; fall back to a bounded set of
 * legacy calls when the Manager predates it (capability detection, so the
 * static UI can be rolled out before the backend release).
 */
export async function loadSummary(signal) {
  try {
    return await get('/ui/summary', { signal });
  } catch (error) {
    if (Number(error && error.status) !== 404) throw error;
    return await legacySummary(signal);
  }
}

async function legacySummary(signal) {
  const [health, nodes] = await Promise.all([
    get('/health', { signal }),
    get('/nodes', { query: { limit: 1 }, signal }),
  ]);
  return {
    generated_at: new Date().toISOString(),
    degraded: true,
    manager: {
      status: health.status,
      provider: health.provider,
      region: '',
      uptime_seconds: health.uptime_seconds,
    },
    jobs: { active: null, by_state: {}, terminal_total: null, cleanup_pending: null },
    workers: { total: nodes.total ?? 0, connected: health.worker_count ?? 0, by_status: {} },
    accounts: { total: null, enabled: null, allocated: null },
    otp: { pending: (getState().loginAttempts || []).length },
  };
}

export { router, shell };

boot();
