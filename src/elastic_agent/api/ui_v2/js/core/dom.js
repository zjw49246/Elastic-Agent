/**
 * Safe DOM helpers.
 *
 * Every server-supplied string reaches the document through ``textContent`` or
 * an attribute assignment; this module intentionally exposes no innerHTML path
 * so a hostile Job name or account error cannot inject markup.
 */

export function el(tag, props = {}, children = []) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (value === null || value === undefined || value === false) continue;
    if (key === 'class') {
      node.className = value;
    } else if (key === 'text') {
      node.textContent = String(value);
    } else if (key === 'dataset') {
      for (const [dk, dv] of Object.entries(value)) {
        if (dv === null || dv === undefined) continue;
        node.dataset[dk] = String(dv);
      }
    } else if (key === 'style') {
      for (const [sk, sv] of Object.entries(value)) node.style.setProperty(sk, sv);
    } else if (key.startsWith('on') && typeof value === 'function') {
      node.addEventListener(key.slice(2).toLowerCase(), value);
    } else if (value === true) {
      node.setAttribute(key, '');
    } else {
      node.setAttribute(key, String(value));
    }
  }
  appendAll(node, children);
  return node;
}

export function appendAll(parent, children) {
  const list = Array.isArray(children) ? children : [children];
  for (const child of list) {
    if (child === null || child === undefined || child === false) continue;
    parent.appendChild(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return parent;
}

export function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
  return node;
}

export function text(value) {
  return document.createTextNode(value === null || value === undefined ? '' : String(value));
}

export function fragment(children) {
  return appendAll(document.createDocumentFragment(), children);
}

/** Reconcile a keyed list in-place so focus, scroll and expansion survive polls. */
export function reconcileList(container, items, keyOf, create, update) {
  const existing = new Map();
  for (const child of Array.from(container.children)) {
    const key = child.dataset.key;
    if (key !== undefined) existing.set(key, child);
    else container.removeChild(child);
  }
  const seen = new Set();
  let cursor = null;
  for (const item of items) {
    const key = String(keyOf(item));
    seen.add(key);
    let node = existing.get(key);
    if (!node) {
      node = create(item);
      node.dataset.key = key;
    } else if (update) {
      update(node, item);
    }
    const next = cursor ? cursor.nextSibling : container.firstChild;
    if (next !== node) container.insertBefore(node, next);
    cursor = node;
  }
  for (const [key, node] of existing) {
    if (!seen.has(key)) container.removeChild(node);
  }
  return container;
}

export function formatBytes(bytes) {
  const value = Number(bytes);
  if (!Number.isFinite(value) || value < 0) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let n = value;
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i += 1; }
  return `${i === 0 ? n : n.toFixed(1)} ${units[i]}`;
}

export function formatTime(value) {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString();
}

export function formatDuration(seconds) {
  const total = Number(seconds);
  if (!Number.isFinite(total) || total < 0) return '—';
  const s = Math.floor(total % 60);
  const m = Math.floor((total / 60) % 60);
  const h = Math.floor(total / 3600);
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}
