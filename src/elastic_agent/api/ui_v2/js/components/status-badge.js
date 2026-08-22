import { el } from '../core/dom.js';

const JOB_STATE_STYLE = {
  prepared: 'idle',
  launching: 'run',
  running: 'run',
  succeeded: 'ok',
  failed: 'err',
  cancelled: 'warn',
  interrupted: 'warn',
  recovered: 'warn',
  unknown: 'idle',
};

const NODE_STATE_STYLE = {
  pending: 'warn',
  running: 'ok',
  draining: 'warn',
  terminated: 'idle',
  error: 'err',
};

const WORKER_PHASE_STYLE = {
  bootstrapping: 'warn',
  provisioning: 'warn',
  logging_in: 'warn',
  rotating: 'warn',
  running: 'run',
  collecting: 'run',
  done: 'ok',
  failed: 'err',
};

function badge(label, kind) {
  return el('span', { class: `badge badge-${kind || 'idle'}`, text: label });
}

export function jobStateBadge(state) {
  const key = String(state || 'unknown');
  return badge(key, JOB_STATE_STYLE[key] || 'idle');
}

export function nodeStateBadge(status) {
  const key = String(status || 'unknown');
  return badge(key, NODE_STATE_STYLE[key] || 'idle');
}

export function workerPhaseBadge(phase) {
  const key = String(phase || 'unknown');
  return badge(key, WORKER_PHASE_STYLE[key] || 'idle');
}

export function boolBadge(value, { onLabel = '是', offLabel = '否', invert = false } = {}) {
  const truthy = Boolean(value);
  const good = invert ? !truthy : truthy;
  return badge(truthy ? onLabel : offLabel, good ? 'ok' : 'idle');
}

export { badge };
