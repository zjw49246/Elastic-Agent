/**
 * Modal dialog with focus trap, Esc close and focus restoration.
 */

import { el, clear, appendAll } from '../core/dom.js';

const FOCUSABLE = 'a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])';

let openDialog = null;

export function dialogIsOpen() {
  return openDialog !== null;
}

/**
 * @param {object} options
 * @param {string} options.title
 * @param {Node|Node[]} options.body
 * @param {Array<{label:string,kind?:string,value?:any,autofocus?:boolean,onClick?:Function}>} options.actions
 * @param {boolean} [options.dismissible=true]
 * @returns {Promise<any>} resolves with the chosen action value (or null)
 */
export function showDialog({ title, body, actions = [], dismissible = true }) {
  const root = document.getElementById('dialogRoot');
  if (!root) return Promise.resolve(null);
  if (openDialog) openDialog.close(null);

  const previousFocus = document.activeElement;
  const titleId = `dlg-title-${Math.random().toString(36).slice(2, 9)}`;

  return new Promise((resolve) => {
    const panel = el('div', {
      class: 'dialog',
      role: 'dialog',
      'aria-modal': 'true',
      'aria-labelledby': titleId,
    });
    panel.appendChild(el('h2', { id: titleId, text: title }));
    const content = el('div', { class: 'dialog-body' });
    appendAll(content, body);
    panel.appendChild(content);

    const bar = el('div', { class: 'dialog-actions' });
    const buttons = [];
    for (const action of actions) {
      const button = el('button', {
        type: 'button',
        class: `btn ${action.kind === 'primary' ? 'btn-primary' : action.kind === 'danger' ? 'btn-danger' : ''}`.trim(),
        text: action.label,
      });
      button.addEventListener('click', async () => {
        if (action.onClick) {
          button.disabled = true;
          try {
            const result = await action.onClick();
            if (result === false) { button.disabled = false; return; }
          } catch (error) {
            button.disabled = false;
            throw error;
          }
        }
        close(action.value === undefined ? action.label : action.value);
      });
      if (action.autofocus) button.dataset.autofocus = 'true';
      buttons.push(button);
      bar.appendChild(button);
    }
    panel.appendChild(bar);

    const backdrop = el('div', { class: 'dialog-backdrop' });
    backdrop.appendChild(panel);
    if (dismissible) {
      backdrop.addEventListener('mousedown', (event) => {
        if (event.target === backdrop) close(null);
      });
    }

    function onKeydown(event) {
      if (event.key === 'Escape' && dismissible) {
        event.preventDefault();
        close(null);
        return;
      }
      if (event.key !== 'Tab') return;
      const items = Array.from(panel.querySelectorAll(FOCUSABLE)).filter((n) => n.offsetParent !== null || n === document.activeElement);
      if (items.length === 0) return;
      const first = items[0];
      const last = items[items.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    }

    function close(value) {
      document.removeEventListener('keydown', onKeydown, true);
      if (backdrop.parentNode === root) root.removeChild(backdrop);
      openDialog = null;
      if (previousFocus && typeof previousFocus.focus === 'function') {
        try { previousFocus.focus(); } catch (_) { /* node may be gone */ }
      }
      resolve(value);
    }

    document.addEventListener('keydown', onKeydown, true);
    clear(root);
    root.appendChild(backdrop);
    openDialog = { close };

    const autofocus = panel.querySelector('[data-autofocus="true"]')
      || panel.querySelector(FOCUSABLE);
    if (autofocus) autofocus.focus();
  });
}

export async function confirmDialog({ title, message, confirmLabel = '确认', danger = false }) {
  const result = await showDialog({
    title,
    body: el('p', { text: message }),
    actions: [
      { label: '取消', value: false },
      { label: confirmLabel, kind: danger ? 'danger' : 'primary', value: true, autofocus: true },
    ],
  });
  return result === true;
}
