import { el, clear } from '../core/dom.js';

let region = null;

function ensureRegion() {
  if (!region) region = document.getElementById('toastRegion');
  return region;
}

export function toast(message, kind = 'info', timeout = 5200) {
  const host = ensureRegion();
  if (!host) return null;
  const node = el('div', { class: `toast toast-${kind}`, text: message });
  host.appendChild(node);
  if (timeout > 0) {
    setTimeout(() => {
      if (node.parentNode === host) host.removeChild(node);
    }, timeout);
  }
  return node;
}

export const toastError = (message) => toast(message, 'error', 8000);
export const toastSuccess = (message) => toast(message, 'success');

export function clearToasts() {
  const host = ensureRegion();
  if (host) clear(host);
}
