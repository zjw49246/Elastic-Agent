/**
 * Elastic-Agent UI v2 entry point.
 *
 * Boot order is fixed (see docs/ui-v2-implementation-plan.md §7.2):
 *   1. scrub api_key from the URL
 *   2. init auth (key stays module-private)
 *   3. install safe global error handlers
 *   4. build router + shell
 *   5. restore the deep link
 *   6. prompt for a key when missing, then continue to the original route
 *   7. start the global health/OTP pollers
 *   8. mount the page and start its own loaders
 */

import { initAuth, hasKey, setKey, forgetKey, scrubUrlCredentials } from './core/auth.js';
import { get, onUnauthorized, resetUnauthorizedLatch } from './core/api.js';
import { installGlobalErrorHandlers, describeError, isAbort } from './core/errors.js';
import { Router, detectBase } from './core/router.js';
import { createPoller, stopAllPollers } from './core/poller.js';
import { setState, getState } from './core/store.js';
import { cancelAllDownloads } from './core/downloads.js';
import { el, clear } from './core/dom.js';
import { AppShell } from './components/app-shell.js';
import { showDialog } from './components/dialog.js';
import { toastError } from './components/toast.js';

const ROUTES = [
  { name: 'overview', pattern: '/overview', load: () => import('./pages/overview.js') },
  { name: 'accounts', pattern: '/accounts', load: () => import('./pages/accounts.js') },
  { name: 'account-new', pattern: '/accounts/new', load: () => import('./pages/account-new.js') },
  { name: 'job-new', pattern: '/jobs/new', load: () => import('./pages/job-new.js') },
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
let authPromise = null;
let navigationToken = 0;

async function boot() {
  scrubUrlCredentials();
  const authed = initAuth();
  installGlobalErrorHandlers((message) => toastError(message));
  onUnauthorized(() => handleUnauthorized());

  router = new Router({
    base: detectBase(),
    routes: ROUTES,
    onNavigate: (route) => { void handleNavigate(route); },
  });

  shell = new AppShell({ router, onAuthClick: () => promptForKey({ replace: false }) }).mount();
  setState({ authed });

  document.getElementById('ea-app').dataset.state = 'ready';
  router.start();

  if (!authed) {
    await promptForKey({ initial: true });
  } else {
    startGlobalPollers();
  }
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
  setState({ authed: false });
  void promptForKey({ reason: 'API Key 无效或已更换，请重新输入。' });
}

function promptForKey({ initial = false, reason = '' } = {}) {
  if (authPromise) return authPromise;
  authPromise = (async () => {
    const input = el('input', {
      type: 'password',
      autocomplete: 'off',
      'aria-label': '管理 API Key',
      placeholder: 'ELASTIC_AGENT_EXTERNAL_API_KEYS 中的一项',
    });
    const status = el('p', { class: 'err small' });
    const body = [
      el('p', { class: 'muted small', text: reason || '控制台使用管理员 API Key 访问 Manager REST API。Key 仅保存在本标签页的 sessionStorage 中，只通过 Authorization 请求头发送。' }),
      el('div', { class: 'field' }, [el('label', { for: 'authKeyInput', text: 'API Key' }), input]),
      status,
    ];
    input.id = 'authKeyInput';

    const actions = [
      {
        label: '保存并继续',
        kind: 'primary',
        value: 'saved',
        autofocus: true,
        onClick: async () => {
          const value = input.value.trim();
          if (!value) {
            status.textContent = '请输入 API Key。';
            return false;
          }
          setKey(value);
          input.value = '';
          try {
            await get('/health');
            resetUnauthorizedLatch();
            return true;
          } catch (error) {
            if (Number(error && error.status) === 401) {
              forgetKey();
              status.textContent = 'API Key 无效。';
              return false;
            }
            // Non-auth failures (503/network) should not discard a good key.
            return true;
          }
        },
      },
    ];
    if (!initial && hasKey()) {
      actions.unshift({
        label: '忘记当前 Key',
        kind: 'danger',
        value: 'forgotten',
        onClick: () => {
          cancelAllDownloads();
          stopAllPollers();
          globalPollers = [];
          forgetKey();
          setState({ authed: false, health: null, summary: null, loginAttempts: [] });
          return true;
        },
      });
    }

    const result = await showDialog({
      title: '需要 API Key',
      body,
      actions,
      dismissible: !initial,
    });
    input.value = '';
    if (result === 'saved' && hasKey()) {
      setState({ authed: true });
      startGlobalPollers();
    }
    return result;
  })().finally(() => { authPromise = null; });
  return authPromise;
}

// --------------------------------------------------------- global pollers

function startGlobalPollers() {
  stopGlobalPollers();
  if (!hasKey()) return;

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
