/**
 * Persistent application shell: navigation, status bar, theme and auth entry.
 */

import { el, clear } from '../core/dom.js';
import { getState, subscribe } from '../core/store.js';

export const NAV_ITEMS = [
  { name: 'overview', path: '/overview', label: '总览' },
  { name: 'accounts', path: '/accounts', label: '账号' },
  { name: 'job-new', path: '/jobs/new', label: '＋ 提交 Job', primary: true },
  { name: 'jobs', path: '/jobs', label: 'Jobs', count: 'jobs' },
  { name: 'results', path: '/results', label: '结果' },
  { name: 'fleet', path: '/fleet', label: 'Workers', count: 'workers' },
];

export class AppShell {
  constructor({ router, onAuthClick }) {
    this.router = router;
    this.onAuthClick = onAuthClick;
    this.root = document.getElementById('ea-app');
    this.navList = document.getElementById('navList');
    this.statusBar = document.getElementById('appStatus');
    this.otpChip = document.getElementById('otpJump');
    this.otpCount = document.getElementById('otpCount');
    this.navToggle = document.getElementById('navToggle');
    this.navScrim = document.getElementById('navScrim');
    this.navLinks = new Map();
    this._unsubscribe = null;
  }

  mount() {
    this._renderNav();
    this._wireChrome();
    this._applyTheme(readTheme());
    this._unsubscribe = subscribe(() => this.update());
    this.update();
    return this;
  }

  _renderNav() {
    clear(this.navList);
    for (const item of NAV_ITEMS) {
      const link = el('a', {
        class: `nav-link${item.primary ? ' nav-primary' : ''}`,
        href: this.router.href(item.path),
        dataset: { route: item.name },
      });
      link.appendChild(el('span', { class: 'nav-label', text: item.label }));
      if (item.count) {
        const count = el('span', { class: 'nav-count', text: '—' });
        link.appendChild(count);
        link._count = count;
      }
      link.addEventListener('click', () => this.closeNav());
      this.navLinks.set(item.name, link);
      this.navList.appendChild(el('li', {}, [link]));
    }
  }

  _wireChrome() {
    this.navToggle.addEventListener('click', () => {
      if (this.root.dataset.nav === 'open') this.closeNav();
      else this.openNav();
    });
    this.navScrim.addEventListener('click', () => this.closeNav());
    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape' && this.root.dataset.nav === 'open') {
        this.closeNav();
        this.navToggle.focus();
      }
    });
    document.getElementById('themeToggle').addEventListener('click', () => {
      const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
      this._applyTheme(next);
      try { window.sessionStorage.setItem('ea_theme', next); } catch (_) { /* private mode */ }
    });
    document.getElementById('authButton').addEventListener('click', () => this.onAuthClick());
    this.otpChip.addEventListener('click', () => {
      this.router.navigate('/overview');
      const target = document.getElementById('otpSection');
      if (target) target.scrollIntoView({ block: 'start' });
    });
  }

  _applyTheme(theme) {
    document.documentElement.dataset.theme = theme === 'dark' ? 'dark' : 'light';
  }

  openNav() {
    this.root.dataset.nav = 'open';
    this.navToggle.setAttribute('aria-expanded', 'true');
    this.navScrim.hidden = false;
    const first = this.navList.querySelector('a');
    if (first) first.focus();
  }

  closeNav() {
    delete this.root.dataset.nav;
    this.navToggle.setAttribute('aria-expanded', 'false');
    this.navScrim.hidden = true;
  }

  setActiveRoute(name) {
    for (const [route, link] of this.navLinks) {
      if (route === name) link.setAttribute('aria-current', 'page');
      else link.removeAttribute('aria-current');
    }
  }

  update() {
    const { health, summary, loginAttempts } = getState();
    clear(this.statusBar);

    if (health) {
      this.statusBar.appendChild(el('span', {
        class: `badge badge-${health.status === 'healthy' ? 'ok' : 'err'}`,
        text: health.status === 'healthy' ? 'Manager 正常' : 'Manager 异常',
      }));
      if (health.provider) {
        const region = summary && summary.manager && summary.manager.region;
        this.statusBar.appendChild(el('span', {
          text: region ? `${health.provider} · ${region}` : String(health.provider),
        }));
      }
    } else {
      this.statusBar.appendChild(el('span', { text: '正在获取 Manager 状态…' }));
    }

    if (summary) {
      const jobs = summary.jobs || {};
      const workers = summary.workers || {};
      this.statusBar.appendChild(el('span', { text: `活跃 Job ${jobs.active ?? 0}` }));
      this.statusBar.appendChild(el('span', { text: `Worker ${workers.connected ?? workers.total ?? 0}` }));
      this._setCount('jobs', jobs.active);
      this._setCount('workers', workers.total);
    }

    const pending = Array.isArray(loginAttempts) ? loginAttempts.length : 0;
    this.otpChip.hidden = pending === 0;
    this.otpCount.textContent = String(pending);
  }

  _setCount(kind, value) {
    for (const item of NAV_ITEMS) {
      if (item.count !== kind) continue;
      const link = this.navLinks.get(item.name);
      if (link && link._count) {
        link._count.textContent = value === undefined || value === null ? '—' : String(value);
      }
    }
  }

  dispose() {
    if (this._unsubscribe) this._unsubscribe();
  }
}

export function readTheme() {
  try {
    return window.sessionStorage.getItem('ea_theme') === 'dark' ? 'dark' : 'light';
  } catch (_) {
    return 'light';
  }
}
