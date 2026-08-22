/**
 * OTP challenge cards.
 *
 * The client key is ``(login_request_id, challenge_id)``; each card carries the
 * account, Job, shard and Worker it belongs to so two workers waiting at once
 * can never be confused. The code lives in the input element only — it is never
 * written to the store, a draft, a toast or a log line.
 */

import { el, reconcileList } from '../core/dom.js';
import { post } from '../core/api.js';
import { describeError } from '../core/errors.js';
import { toastError, toastSuccess } from './toast.js';

const inFlight = new Set();

export function challengeKey(challenge) {
  return `${challenge.login_request_id}::${challenge.challenge_id}`;
}

export function renderOtpList(container, challenges, { onSubmitted } = {}) {
  reconcileList(
    container,
    challenges,
    challengeKey,
    (challenge) => buildCard(challenge, onSubmitted),
    (node, challenge) => updateCard(node, challenge),
  );
  return container;
}

function metaLine(challenge) {
  const parts = [];
  if (challenge.account_email) parts.push(`账号 ${challenge.account_email}`);
  if (challenge.account_id) parts.push(`ID ${challenge.account_id}`);
  if (challenge.job_name || challenge.job_id) {
    parts.push(`Job ${challenge.job_name || challenge.job_id}`);
  }
  if (challenge.shard_index !== null && challenge.shard_index !== undefined) {
    parts.push(`shard ${challenge.shard_index}`);
  }
  if (challenge.worker_id) parts.push(`Worker ${challenge.worker_id}`);
  return parts.join(' · ');
}

function buildCard(challenge, onSubmitted) {
  const card = el('div', { class: 'otp-card', dataset: { challenge: challenge.challenge_id } });
  card.appendChild(el('h3', { text: '需要输入 6 位验证码' }));
  card.appendChild(el('p', { class: 'otp-meta', text: metaLine(challenge) }));

  const input = el('input', {
    type: 'text',
    inputmode: 'numeric',
    autocomplete: 'one-time-code',
    maxlength: '6',
    pattern: '[0-9]{6}',
    'aria-label': `验证码（${challenge.account_email || challenge.account_id}）`,
    placeholder: '000000',
  });
  const submit = el('button', { type: 'submit', class: 'btn btn-primary btn-sm', text: '提交' });
  const status = el('span', { class: 'small muted', role: 'status' });

  const form = el('form');
  form.appendChild(input);
  form.appendChild(submit);
  form.appendChild(status);
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const key = challengeKey(card._challenge);
    if (inFlight.has(key)) return;
    const code = input.value.trim();
    if (!/^\d{6}$/.test(code)) {
      status.textContent = '验证码必须是 6 位数字。';
      input.setAttribute('aria-invalid', 'true');
      input.focus();
      return;
    }
    input.removeAttribute('aria-invalid');
    inFlight.add(key);
    submit.disabled = true;
    status.textContent = '提交中…';
    try {
      await post(
        `/accounts/login-attempts/${encodeURIComponent(card._challenge.login_request_id)}/otp`,
        { challenge_id: card._challenge.challenge_id, code },
      );
      input.value = '';
      status.textContent = '已提交。';
      toastSuccess('验证码已提交。');
      if (onSubmitted) onSubmitted(card._challenge);
    } catch (error) {
      const httpStatus = Number(error && error.status) || 0;
      if (httpStatus === 404) status.textContent = '该登录请求已不存在。';
      else if (httpStatus === 409) status.textContent = '冲突或已处理，请勿重复提交旧验证码。';
      else if (httpStatus === 410) status.textContent = '该验证码请求已过期，请等待新的挑战。';
      else status.textContent = describeError(error);
      toastError(status.textContent);
    } finally {
      inFlight.delete(key);
      submit.disabled = false;
    }
  });

  card.appendChild(form);
  card._challenge = challenge;
  card._meta = card.querySelector('.otp-meta');
  return card;
}

function updateCard(card, challenge) {
  // Replace only the metadata text; the live input node (and therefore focus,
  // selection and any partially typed code) is preserved across polls.
  card._challenge = challenge;
  const meta = metaLine(challenge);
  if (card._meta && card._meta.textContent !== meta) card._meta.textContent = meta;
}
