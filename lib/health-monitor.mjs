// lib/health-monitor.mjs — Health state monitor for btc-receiver v3.00

import { BufferedWriter } from './buffered-writer.mjs';

/**
 * Simplified health monitor for Phase 1.
 * Tracks connector states and writes health.jsonl.
 */
export class HealthMonitor {
  /**
   * @param {string} outputPath
   * @param {Object} [options]
   * @param {number} [options.intervalMs=1000]
   */
  constructor(outputPath, options = {}) {
    this._outputPath = outputPath;
    this._intervalMs = options.intervalMs ?? 1000;
    /** @type {Map<string, import('./events.mjs').ConnectorStats>} */
    this._connectorStats = new Map();
    this._writer = new BufferedWriter(outputPath, {
      flushIntervalMs: 1000,
      maxBufferLines: 100,
      maxLossMs: 30000,
    });
    this._timer = null;
    this._closed = false;
    this._startTime = Date.now();
  }

  /** Register or update connector stats. */
  updateConnector(market, stats) {
    this._connectorStats.set(market, { ...stats });
  }

  /** Start periodic writing. */
  start() {
    if (this._timer) return;
    this._timer = setInterval(() => this._tick(), this._intervalMs);
    if (this._timer.unref) this._timer.unref();
  }

  stop() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }

  async close() {
    this.stop();
    this._closed = true;
    await this._writer.close();
  }

  /** @returns {Object} */
  getHealthSummary() {
    const markets = {};
    for (const [market, stats] of this._connectorStats) {
      markets[market] = {
        state: stats.state,
        connectedAt: stats.connectedAt,
        lastDepthMsgAt: stats.lastDepthMsgAt,
        lastTradeMsgAt: stats.lastTradeMsgAt,
        depthMsgCount: stats.depthMsgCount,
        tradeMsgCount: stats.tradeMsgCount,
        reconnectCount: stats.reconnectCount,
        resyncCount: stats.resyncCount,
        lastSeq: stats.lastSeq,
      };
    }

    const states = Object.values(markets).map(m => m.state);
    let overallState = 'normal';
    if (states.some(s => s === 'error' || s === 'reconnecting')) {
      overallState = 'critical';
    } else if (states.some(s => s !== 'running')) {
      overallState = 'warning';
    }

    return {
      ts: Date.now(),
      state: overallState,
      markets,
    };
  }

  _tick() {
    if (this._closed) return;
    const report = this.getHealthSummary();
    this._writer.write(report);
  }
}
