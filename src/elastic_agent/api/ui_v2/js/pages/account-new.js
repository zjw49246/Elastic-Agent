/**
 * Add account.
 *
 * Secret controls (OpenAI password, mailbox query token, Agent API Key) are
 * cleared as soon as the request succeeds and are never copied into the store,
 * a draft, a toast or an error object.
 */

import { el, clear } from '../core/dom.js';
import { post } from '../core/api.js';
import { describeError } from '../core/errors.js';
import { toastError, toastSuccess } from '../components/toast.js';
import { confirmDialog } from '../components/dialog.js';

const TYPES = [
  { value: 'claude_oauth', label: 'Claude OAuth' },
  { value: 'codex_oauth', label: 'Codex OAuth' },
  { value: 'cloudrouter', label: 'CloudRouter（Agent API）' },
  { value: 'apex', label: 'ApexRouter（Agent API，按模型类型分配）' },
];

export function createPage({ router, container }) {
  const nodes = {};
  let beforeUnload = null;

  function mount() {
    container.appendChild(buildLayout(nodes, submit));
    setType(nodes, 'claude_oauth');
    beforeUnload = (event) => {
      if (hasSecretValue(nodes)) {
        event.preventDefault();
        event.returnValue = '';
      }
    };
    window.addEventListener('beforeunload', beforeUnload);
    // Intercept in-app navigation too: leaving with a secret still typed
    // should be a deliberate choice, and the value is discarded either way.
    nodes.form.addEventListener('click', () => {}, { once: true });
  }

  async function submit(event) {
    event.preventDefault();
    clearErrors(nodes);
    const type = nodes.type.value;
    const errors = validate(nodes, type);
    if (errors.length) {
      showErrors(nodes, errors);
      return;
    }
    nodes.submit.disabled = true;
    try {
      if (type === 'cloudrouter' || type === 'apex') {
        await post('/agent-api/accounts', {
          provider: type === 'apex' ? 'apex' : 'cloudrouter',
          name: nodes.apiName.value.trim(),
          api_key: nodes.apiKey.value,
          group: nodes.group.value.trim() || 'standard',
        });
      } else {
        const payload = {
          id: nodes.id.value.trim(),
          email: nodes.email.value.trim(),
          agent_type: type === 'codex_oauth' ? 'codex' : 'claude',
          group: nodes.group.value.trim() || 'standard',
          enabled: true,
        };
        if (nodes.password.value) payload.password = nodes.password.value;
        if (nodes.emailToken.value) payload.email_token = nodes.emailToken.value;
        await post('/accounts', payload);
      }
      wipeSecrets(nodes);
      toastSuccess('账号已添加。');
      router.navigate('/accounts');
    } catch (error) {
      // describeError only ever reads status/detail — never the request body.
      showErrors(nodes, [describeError(error)]);
      toastError(describeError(error));
    } finally {
      nodes.submit.disabled = false;
    }
  }

  function dispose() {
    wipeSecrets(nodes);
    if (beforeUnload) window.removeEventListener('beforeunload', beforeUnload);
  }

  return { mount, dispose };
}

export function validate(nodes, type) {
  const errors = [];
  if (type === 'cloudrouter' || type === 'apex') {
    if (!nodes.apiName.value.trim()) errors.push('请填写显示名称。');
    if (!nodes.apiKey.value) errors.push('请填写 API Key。');
  } else {
    if (!nodes.id.value.trim()) errors.push('请填写账号 ID。');
    if (!nodes.email.value.trim()) errors.push('请填写邮箱。');
    if (type === 'codex_oauth' && !nodes.password.value && !nodes.emailToken.value) {
      errors.push('Codex 账号至少需要 OpenAI 密码或邮箱取码 Token 之一。');
    }
  }
  return errors;
}

function hasSecretValue(nodes) {
  return Boolean(nodes.password.value || nodes.emailToken.value || nodes.apiKey.value);
}

function wipeSecrets(nodes) {
  for (const key of ['password', 'emailToken', 'apiKey']) {
    if (nodes[key]) nodes[key].value = '';
  }
}

function setType(nodes, type) {
  nodes.type.value = type;
  const isApi = type === 'cloudrouter' || type === 'apex';
  nodes.oauthFields.hidden = isApi;
  nodes.apiFields.hidden = !isApi;
  nodes.codexHint.hidden = type !== 'codex_oauth';
  nodes.apexHint.hidden = type !== 'apex';
  // Hidden inputs must not carry stale secrets into the next submission.
  wipeSecrets(nodes);
  for (const node of nodes.oauthFields.querySelectorAll('input')) node.disabled = isApi;
  for (const node of nodes.apiFields.querySelectorAll('input, select')) node.disabled = !isApi;
}

function clearErrors(nodes) {
  clear(nodes.errors);
  nodes.errors.hidden = true;
}

function showErrors(nodes, messages) {
  clear(nodes.errors);
  for (const message of messages) nodes.errors.appendChild(el('li', { text: message }));
  nodes.errors.hidden = false;
}

function buildLayout(nodes, submit) {
  const root = document.createDocumentFragment();
  root.appendChild(el('div', { class: 'page-head' }, [
    el('div', {}, [
      el('h1', { text: '添加账号' }),
      el('p', { class: 'page-sub', text: '秘密字段是 write-only：提交成功后立即清空，REST 永不回显。' }),
    ]),
  ]));

  nodes.form = el('form', { class: 'card', novalidate: true, autocomplete: 'off' });
  nodes.form.addEventListener('submit', submit);

  nodes.type = el('select', { id: 'newAcctType' });
  for (const type of TYPES) {
    nodes.type.appendChild(el('option', { value: type.value, text: type.label }));
  }
  nodes.type.addEventListener('change', () => setType(nodes, nodes.type.value));
  nodes.form.appendChild(el('div', { class: 'field' }, [
    el('label', { for: 'newAcctType', text: '账号类型' }),
    nodes.type,
  ]));

  nodes.group = el('input', { type: 'text', id: 'newAcctGroup', value: 'standard' });
  nodes.form.appendChild(el('div', { class: 'field' }, [
    el('label', { for: 'newAcctGroup', text: '账号组' }),
    nodes.group,
    el('span', { class: 'help', text: 'Job 通过 account.group 选号；留空按 standard 处理。' }),
  ]));

  // ---- OAuth fields
  nodes.id = el('input', { type: 'text', id: 'newAcctId', autocomplete: 'off' });
  nodes.email = el('input', { type: 'text', id: 'newAcctEmail', autocomplete: 'off' });
  nodes.password = el('input', { type: 'password', id: 'newAcctPassword', autocomplete: 'new-password' });
  nodes.emailToken = el('input', { type: 'password', id: 'newAcctEmailToken', autocomplete: 'new-password' });
  nodes.codexHint = el('p', { class: 'help', text: 'Codex 至少需要密码或邮箱取码 Token 之一。取码 Token 只是邮箱查询凭据，不是 OpenAI Token，也不是通用 IMAP 密码。' });

  nodes.oauthFields = el('fieldset', {}, [
    el('legend', { text: 'OAuth 账号' }),
    el('div', { class: 'form-grid' }, [
      el('div', { class: 'field' }, [el('label', { for: 'newAcctId', text: '账号 ID' }), nodes.id,
        el('span', { class: 'help', text: '稳定身份键；EIP 绑定与租约都按它记录。' })]),
      el('div', { class: 'field' }, [el('label', { for: 'newAcctEmail', text: '邮箱' }), nodes.email]),
      el('div', { class: 'field' }, [el('label', { for: 'newAcctPassword', text: '登录密码（write-only）' }), nodes.password]),
      el('div', { class: 'field' }, [el('label', { for: 'newAcctEmailToken', text: '邮箱取码 Token（write-only）' }), nodes.emailToken]),
    ]),
    nodes.codexHint,
  ]);
  nodes.form.appendChild(nodes.oauthFields);

  // ---- Agent API fields
  nodes.apiName = el('input', { type: 'text', id: 'newApiName', autocomplete: 'off' });
  nodes.apiKey = el('input', { type: 'password', id: 'newApiKey', autocomplete: 'new-password' });
  nodes.apexHint = el('p', { class: 'help', text: 'ApexRouter 的 Claude/Codex 能力以 /models 返回为准。' });
  nodes.apiFields = el('fieldset', {}, [
    el('legend', { text: 'Agent API 账号' }),
    el('div', { class: 'form-grid' }, [
      el('div', { class: 'field' }, [el('label', { for: 'newApiName', text: '显示名称' }), nodes.apiName]),
      el('div', { class: 'field' }, [el('label', { for: 'newApiKey', text: 'API Key（write-only）' }), nodes.apiKey,
        el('span', { class: 'help', text: '提交前会向 provider 校验；成功后表单立即清空，Key 只在 Manager 私有 store 中。' })]),
    ]),
    nodes.apexHint,
  ]);
  nodes.form.appendChild(nodes.apiFields);

  nodes.errors = el('ul', { class: 'err small', hidden: true, role: 'alert' });
  nodes.form.appendChild(nodes.errors);

  nodes.submit = el('button', { type: 'submit', class: 'btn btn-primary', text: '添加账号' });
  const cancel = el('button', { type: 'button', class: 'btn', text: '取消' });
  cancel.addEventListener('click', async () => {
    if (hasSecretValue(nodes) && !await confirmDialog({
      title: '离开将清空',
      message: '当前表单仍有未提交的秘密字段，离开后会被清空。',
      confirmLabel: '离开',
    })) return;
    wipeSecrets(nodes);
    window.history.back();
  });
  nodes.form.appendChild(el('div', { class: 'row' }, [nodes.submit, cancel]));

  root.appendChild(nodes.form);
  return root;
}
