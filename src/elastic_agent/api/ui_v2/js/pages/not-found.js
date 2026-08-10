import { el } from '../core/dom.js';

export function createPage({ router, container }) {
  function mount() {
    container.appendChild(el('div', { class: 'card' }, [
      el('h1', { text: '页面不存在' }),
      el('p', { class: 'muted', text: '没有匹配的界面路由。' }),
      el('p', {}, [el('a', { class: 'btn', href: router.href('/overview'), text: '返回总览' })]),
    ]));
  }
  return { mount, dispose() {} };
}
