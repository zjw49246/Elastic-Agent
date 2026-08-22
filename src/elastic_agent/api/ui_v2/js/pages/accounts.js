import { el, clear, reconcileList } from '../core/dom.js';
import { get, post, del, createGeneration } from '../core/api.js';
import { createPoller } from '../core/poller.js';
import { pageCache } from '../core/store.js';
import { badge, boolBadge } from '../components/status-badge.js';
import { confirmDialog, showDialog } from '../components/dialog.js';
import { toastError, toastSuccess } from '../components/toast.js';
import { describeError } from '../core/errors.js';

export function createPage({ router, container }) {
  const cache = pageCache('accounts');
  if (!cache.filter) {
    cache.filter = { kind: '', agent: '', provider: '', group: '', enabled: '', query: '' };
  }
  const generation = createGeneration();
  const nodes = {};
  const busy = new Set();
  let poller = null;

  function mount() {
    container.appendChild(buildLayout(router, nodes, cache, () => refresh()));
    poller = createPoller({ name: 'accounts', interval: 20000, task: (signal) => load(signal) });
    poller.start();
  }

  function refresh() {
    if (poller) poller.refresh();
  }

  async function load(signal) {
    const token = generation.next();
    const [accounts, allocations, bindings] = await Promise.all([
      get('/accounts', { signal }),
      get('/accounts/allocations', { signal }).catch(() => ({ allocations: {} })),
      get('/accounts/bindings', { signal }).catch(() => ({ bindings: [] })),
    ]);
    if (!token.valid) return;
    cache.accounts = accounts.accounts || [];
    cache.allocations = allocations.allocations || {};
    cache.bindings = new Map((bindings.bindings || []).map((b) => [b.account_id, b]));
    render();
  }

  function render() {
    const rows = filterAccounts(cache.accounts || [], cache.filter);
    nodes.status.textContent = `${rows.length} / ${(cache.accounts || []).length} 个账号`;
    nodes.empty.hidden = rows.length > 0;
    reconcileList(nodes.tbody, rows, (a) => a.id, (a) => buildRow(a), (row, a) => updateRow(row, a));
  }

  function buildRow(account) {
    const row = el('tr');
    for (let i = 0; i < 6; i += 1) row.appendChild(el('td'));
    row.children[5].className = 'actions';
    updateRow(row, account);
    return row;
  }

  function updateRow(row, account) {
    const [idCell, kindCell, secretCell, usageCell, bindCell, actionCell] = row.children;
    const isApi = account.auth_kind === 'agent_api';

    clear(idCell);
    idCell.appendChild(el('div', { class: 'mono', text: account.id }));
    if (account.email) idCell.appendChild(el('div', { class: 'small muted', text: account.email }));
    if (account.group) idCell.appendChild(el('div', { class: 'small muted', text: `组 ${account.group}` }));

    clear(kindCell);
    kindCell.appendChild(badge(isApi ? 'Agent API' : 'OAuth', isApi ? 'run' : 'idle'));
    if (isApi && account.api_provider) {
      kindCell.appendChild(el('div', { class: 'small', text: account.api_provider }));
    }
    const agents = account.supported_agent_types && account.supported_agent_types.length
      ? account.supported_agent_types
      : (account.agent_type ? [account.agent_type] : []);
    if (agents.length) kindCell.appendChild(el('div', { class: 'small muted', text: agents.join(' / ') }));
    const models = modelsFor(account);
    if (models.length) {
      kindCell.appendChild(el('div', { class: 'small muted', text: `模型 ${models.slice(0, 3).join(', ')}${models.length > 3 ? '…' : ''}` }));
    }

    // Secrets are write-only server-side; only presence flags are shown.
    clear(secretCell);
    if (isApi) {
      secretCell.appendChild(boolBadge(account.has_api_key, { onLabel: 'Key 已配置', offLabel: '无 Key' }));
    } else {
      secretCell.appendChild(boolBadge(account.has_password, { onLabel: '密码已配置', offLabel: '无密码' }));
      secretCell.appendChild(boolBadge(account.has_email_token, { onLabel: '取码 Token 已配置', offLabel: '无取码 Token' }));
    }
    secretCell.appendChild(boolBadge(account.enabled, { onLabel: '启用', offLabel: '停用' }));

    clear(usageCell);
    usageCell.appendChild(usageNode(account));

    clear(bindCell);
    const binding = cache.bindings && cache.bindings.get(account.id);
    if (binding) {
      bindCell.appendChild(el('div', { class: 'mono small', text: binding.eip || '—' }));
      bindCell.appendChild(el('div', { class: 'small muted', text: `${binding.region || ''} ${binding.state || ''}`.trim() }));
    } else {
      bindCell.appendChild(el('span', { class: 'small muted', text: '无 EIP' }));
    }
    const allocs = (cache.allocations && cache.allocations[account.id]) || [];
    const active = allocs.filter((a) => a.active);
    if (active.length) {
      for (const alloc of active.slice(0, 2)) {
        bindCell.appendChild(el('div', { class: 'small', text: `${alloc.job_name || alloc.job_id} · ${alloc.phase || ''}` }));
      }
    }

    clear(actionCell);
    if (isApi) {
      actionCell.appendChild(action('刷新额度', async () => {
        await post(`/agent-api/accounts/${encodeURIComponent(account.id)}/refresh`);
        toastSuccess('已刷新额度。');
        refresh();
      }, busy, account.id));
      // Backend rejects Agent API deletion outright; surface it as disabled.
      const disabled = el('button', {
        type: 'button',
        class: 'btn btn-sm',
        text: '删除',
        disabled: true,
        title: 'Agent API 账号删除当前已禁用；必须先终止全部受托 Worker。',
      });
      actionCell.appendChild(disabled);
    } else {
      actionCell.appendChild(action(account.enabled ? '停用' : '启用', async () => {
        await post('/accounts', { ...oauthPayload(account), enabled: !account.enabled });
        toastSuccess(account.enabled ? '账号已停用。' : '账号已启用。');
        refresh();
      }, busy, account.id));
      actionCell.appendChild(action('删除', () => deleteOauthAccount(account, binding, refresh), busy, account.id, 'btn-danger'));
    }
  }

  function dispose() {
    if (poller) poller.stop();
  }

  return { mount, dispose };
}

function oauthPayload(account) {
  // Re-posting an account preserves existing secrets: omitting the secret
  // fields (and the clear_* flags) is an explicit "keep as-is" on the server.
  return {
    id: account.id,
    email: account.email,
    agent_type: account.agent_type || 'claude',
    group: account.group || 'standard',
  };
}

function modelsFor(account) {
  const models = account.models;
  if (!models || typeof models !== 'object') return [];
  const flat = [];
  for (const list of Object.values(models)) {
    if (Array.isArray(list)) flat.push(...list);
  }
  return Array.from(new Set(flat));
}

function usageNode(account) {
  const usage = account.api_usage;
  if (!usage || typeof usage !== 'object') return el('span', { class: 'small muted', text: '—' });
  const wrap = el('div', { class: 'small' });
  if (usage.available === false) {
    wrap.appendChild(badge('不可用', 'err'));
    if (usage.reason) wrap.appendChild(el('div', { class: 'muted', text: String(usage.reason).slice(0, 120) }));
    return wrap;
  }
  if (usage.mode === 'unrestricted') {
    wrap.appendChild(badge('无消费上限', 'ok'));
    return wrap;
  }
  const parts = [];
  if (usage.remaining !== undefined && usage.remaining !== null) parts.push(`剩余 ${usage.remaining}`);
  if (usage.used !== undefined && usage.used !== null) parts.push(`已用 ${usage.used}`);
  if (usage.limit !== undefined && usage.limit !== null) parts.push(`上限 ${usage.limit}`);
  wrap.appendChild(el('span', { text: parts.join(' · ') || '—' }));
  return wrap;
}

/**
 * Deleting a bound OAuth account must decommission its EIP first.
 *
 * Order is fixed: read binding → irreversible warning → full account ID
 * confirmation → decommission with release_eip=true → delete identity. Any
 * failure (including 409) stops before the identity is removed.
 */
async function deleteOauthAccount(account, binding, refresh) {
  if (binding) {
    const input = el('input', { type: 'text', autocomplete: 'off', 'aria-label': '输入完整账号 ID 以确认' });
    const status = el('p', { class: 'err small' });
    const confirmed = await showDialog({
      title: '释放 EIP 并删除账号',
      body: [
        el('p', { text: `账号 ${account.id} 绑定了弹性公网 IP ${binding.eip || '(未知)'}。` }),
        el('p', { class: 'err', text: '释放后该 IP 不可恢复；再次分配会得到不同的公网出口地址，账号可能需要重新通过风控校验。' }),
        el('p', { class: 'small muted', text: '请输入完整账号 ID 以确认：' }),
        input,
        status,
      ],
      actions: [
        { label: '取消', value: false },
        {
          label: '释放 EIP 并删除',
          kind: 'danger',
          value: true,
          onClick: () => {
            if (input.value.trim() !== account.id) {
              status.textContent = '账号 ID 不匹配。';
              return false;
            }
            return true;
          },
        },
      ],
    });
    if (confirmed !== true) return;

    try {
      await post(`/accounts/${encodeURIComponent(account.id)}/binding/decommission`, {
        release_eip: true,
        confirm_account_id: account.id,
      });
    } catch (error) {
      toastError(`EIP 释放失败，账号未删除：${describeError(error)}`);
      refresh();
      return;
    }
  } else if (!await confirmDialog({
    title: '删除账号',
    message: `确认删除账号 ${account.id}？`,
    confirmLabel: '删除',
    danger: true,
  })) {
    return;
  }

  await del(`/accounts/${encodeURIComponent(account.id)}`);
  toastSuccess('账号已删除。');
  refresh();
}

function action(label, handler, busy, key, extraClass = '') {
  const button = el('button', { type: 'button', class: `btn btn-sm ${extraClass}`.trim(), text: label });
  button.addEventListener('click', async () => {
    const lock = `${key}:${label}`;
    if (busy.has(lock)) return;
    busy.add(lock);
    button.disabled = true;
    try {
      await handler();
    } catch (error) {
      toastError(describeError(error));
    } finally {
      busy.delete(lock);
      button.disabled = false;
    }
  });
  return button;
}

export function filterAccounts(accounts, filter) {
  const query = (filter.query || '').trim().toLowerCase();
  return accounts.filter((account) => {
    if (filter.kind && account.auth_kind !== filter.kind) return false;
    if (filter.provider && account.api_provider !== filter.provider) return false;
    if (filter.agent) {
      const agents = account.supported_agent_types && account.supported_agent_types.length
        ? account.supported_agent_types
        : (account.agent_type ? [account.agent_type] : []);
      if (!agents.includes(filter.agent)) return false;
    }
    if (filter.group && account.group !== filter.group) return false;
    if (filter.enabled === 'true' && !account.enabled) return false;
    if (filter.enabled === 'false' && account.enabled) return false;
    if (query) {
      const haystack = `${account.id} ${account.email || ''}`.toLowerCase();
      if (!haystack.includes(query)) return false;
    }
    return true;
  });
}

function buildLayout(router, nodes, cache, refresh) {
  const root = document.createDocumentFragment();
  root.appendChild(el('div', { class: 'page-head' }, [
    el('div', {}, [
      el('h1', { text: '账号' }),
      el('p', { class: 'page-sub', text: 'OAuth 与 Agent API 账号统一列表；秘密字段只显示是否已配置。' }),
    ]),
    el('div', { class: 'page-actions' }, [
      el('a', { class: 'btn btn-primary', href: router.href('/accounts/new'), text: '添加账号' }),
    ]),
  ]));

  const card = el('section', { class: 'card' });
  const filters = el('div', { class: 'filters' });
  filters.appendChild(selectField('acctKind', '类型', [
    ['', '全部'], ['oauth', 'OAuth'], ['agent_api', 'Agent API'],
  ], cache.filter.kind, (value) => { cache.filter.kind = value; refresh(); }));
  filters.appendChild(selectField('acctAgent', 'Agent', [
    ['', '全部'], ['claude', 'Claude'], ['codex', 'Codex'],
  ], cache.filter.agent, (value) => { cache.filter.agent = value; refresh(); }));
  filters.appendChild(selectField('acctProvider', 'Provider', [
    ['', '全部'], ['cloudrouter', 'CloudRouter'], ['apex', 'ApexRouter'],
  ], cache.filter.provider, (value) => { cache.filter.provider = value; refresh(); }));
  filters.appendChild(selectField('acctEnabled', '状态', [
    ['', '全部'], ['true', '启用'], ['false', '停用'],
  ], cache.filter.enabled, (value) => { cache.filter.enabled = value; refresh(); }));

  const search = el('input', { type: 'search', id: 'acctQuery', placeholder: '账号 ID 或邮箱', value: cache.filter.query || '' });
  search.addEventListener('input', () => {
    cache.filter.query = search.value;
    refresh();
  });
  filters.appendChild(el('div', { class: 'field' }, [el('label', { for: 'acctQuery', text: '关键词' }), search]));
  card.appendChild(filters);

  nodes.status = el('p', { class: 'small muted', role: 'status' });
  card.appendChild(nodes.status);

  nodes.tbody = el('tbody');
  card.appendChild(el('div', { class: 'table-wrap' }, [
    el('table', {}, [
      el('thead', {}, [el('tr', {}, [
        el('th', { text: '账号' }),
        el('th', { text: '类型 / Agent' }),
        el('th', { text: '凭据与状态' }),
        el('th', { text: '额度' }),
        el('th', { text: 'EIP / 分配' }),
        el('th', { text: '操作' }),
      ])]),
      nodes.tbody,
    ]),
  ]));
  nodes.empty = el('p', { class: 'empty', text: '没有匹配的账号。' });
  card.appendChild(nodes.empty);

  root.appendChild(card);
  return root;
}

function selectField(id, label, options, value, onChange) {
  const select = el('select', { id });
  for (const [optionValue, optionLabel] of options) {
    select.appendChild(el('option', { value: optionValue, text: optionLabel, selected: optionValue === value }));
  }
  select.addEventListener('change', () => onChange(select.value));
  return el('div', { class: 'field' }, [el('label', { for: id, text: label }), select]);
}
