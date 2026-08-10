/**
 * Single-flight poller with visibility pausing and exponential backoff.
 *
 * Every page owns its pollers and must ``stop()`` them in ``dispose()``. A live
 * registry is exported so leak tests can assert the count returns to baseline
 * after navigation.
 */

import { isAbort } from './errors.js';

const live = new Set();

export function activePollerCount() {
  return live.size;
}

export function stopAllPollers() {
  for (const poller of Array.from(live)) poller.stop();
}

export class Poller {
  /**
   * @param {object} options
   * @param {string} options.name
   * @param {number} options.interval Base period in ms.
   * @param {(signal: AbortSignal) => Promise<void>} options.task
   * @param {boolean} [options.immediate=true] Run once as soon as started.
   * @param {number} [options.maxBackoff=120000]
   */
  constructor({ name, interval, task, immediate = true, maxBackoff = 120000 }) {
    this.name = name;
    this.interval = interval;
    this.task = task;
    this.immediate = immediate;
    this.maxBackoff = maxBackoff;
    this.timer = null;
    this.controller = null;
    this.inFlight = false;
    this.failures = 0;
    this.paused = false;
    this.stopped = true;
    this._onVisibility = () => this._handleVisibility();
  }

  start() {
    if (!this.stopped) return this;
    this.stopped = false;
    this.failures = 0;
    live.add(this);
    document.addEventListener('visibilitychange', this._onVisibility);
    if (document.hidden) {
      this.paused = true;
      return this;
    }
    if (this.immediate) this._run();
    else this._schedule(this.interval);
    return this;
  }

  stop() {
    this.stopped = true;
    this.paused = false;
    live.delete(this);
    document.removeEventListener('visibilitychange', this._onVisibility);
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.controller) {
      this.controller.abort();
      this.controller = null;
    }
    this.inFlight = false;
  }

  /** Run once immediately, resetting the schedule. */
  refresh() {
    if (this.stopped || this.inFlight) return;
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this._run();
  }

  setInterval(interval) {
    this.interval = interval;
  }

  _handleVisibility() {
    if (this.stopped) return;
    if (document.hidden) {
      this.paused = true;
      if (this.timer !== null) {
        clearTimeout(this.timer);
        this.timer = null;
      }
      if (this.controller) {
        this.controller.abort();
        this.controller = null;
      }
      this.inFlight = false;
    } else if (this.paused) {
      this.paused = false;
      this._run();
    }
  }

  _schedule(delay) {
    if (this.stopped || this.paused) return;
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.timer = null;
      this._run();
    }, delay);
  }

  async _run() {
    if (this.stopped || this.paused || this.inFlight) return;
    this.inFlight = true;
    this.controller = new AbortController();
    const signal = this.controller.signal;
    let delay = this.interval;
    try {
      await this.task(signal);
      this.failures = 0;
    } catch (error) {
      if (isAbort(error) || this.stopped) {
        this.inFlight = false;
        this.controller = null;
        return;
      }
      const status = Number(error && error.status) || 0;
      if (status === 401) {
        // A dead key must not turn every page into a retry storm; the shell
        // restarts pollers once the operator supplies a new one.
        this.inFlight = false;
        this.controller = null;
        this.stop();
        return;
      }
      if (status >= 400 && status < 500 && status !== 429) {
        this.inFlight = false;
        this.controller = null;
        this.stop();
        return;
      }
      this.failures += 1;
      const backoff = Math.min(this.interval * 2 ** this.failures, this.maxBackoff);
      delay = backoff * (0.75 + Math.random() * 0.5);
    }
    this.inFlight = false;
    this.controller = null;
    this._schedule(delay);
  }
}

export function createPoller(options) {
  return new Poller(options);
}
