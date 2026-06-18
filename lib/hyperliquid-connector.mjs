// lib/hyperliquid-connector.mjs — Hyperliquid perpetual connector for btc-receiver v3.00
// NOTE: Hyperliquid does not provide a public liquidation/forceOrder stream.
//       Liquidation tracking for Hyperliquid is not implemented.
//       No stub is left — the `liquidation` event is never emitted.

import { BaseConnector } from './base-connector.mjs';
import { FullBook } from './full-book.mjs';

const WS_SNAPSHOT_TIMEOUT_MS = 15000;

const toPairs = (rows = []) => rows.map(l => [String(l.px), String(l.sz)]);

export class HyperliquidConnector extends BaseConnector {
  constructor(config) {
    super(config, {
      market: 'hyperliquid_perp',
      wsUrl: config.wsUrl || 'wss://api.hyperliquid.xyz/ws',
      restUrl: '', // No REST snapshot needed; l2Book is full replace via WS
    });
    /** @type {FullBook} */
    this.book = new FullBook('hyperliquid_perp', { maxLevels: config.depthLimit ?? 100 });
  }

  subscribe() {
    this._ws.send(JSON.stringify({
      method: 'subscribe',
      subscription: { type: 'trades', coin: 'BTC' },
    }));
    this._ws.send(JSON.stringify({
      method: 'subscribe',
      subscription: { type: 'l2Book', coin: 'BTC' },
    }));
  }

  _onMessage(data) {
    if (data.channel === 'l2Book') {
      this._handleDepth(data);
    } else if (data.channel === 'trades') {
      this._handleTrade(data);
    }
    // subscription ack: { type: 'subscriptionResponse', ... } — ignore
  }

  _handleDepth(data) {
    // Hyperliquid l2Book is always a full snapshot (replace, not diff)
    const bookData = data.data;
    if (!bookData || !bookData.levels) return;

    // levels[0] = bids, levels[1] = asks
    // Each level: { px: number, sz: number, n: number }
    const rawBids = bookData.levels[0] || [];
    const rawAsks = bookData.levels[1] || [];
    const bids = toPairs(rawBids);
    const asks = toPairs(rawAsks);
    const ts = bookData.time || Date.now();

    this.book.applySnapshot(bids, asks);
    this._emitDepth('snapshot', bids, asks, ts);
    this._notifyWsSnapshotReceived();

    this._ringBuf = [];
  }

  _handleTrade(data) {
    const trades = data.data || [];
    for (const t of trades) {
      // Hyperliquid side: 'B' = buy, 'A' = sell
      this._emitTrade(
        parseFloat(t.px),
        parseFloat(t.sz),
        t.side === 'B' ? 'buy' : 'sell',
        (t.time && Number(t.time) > 1e15) ? Math.floor(Number(t.time) / 1e6) : (t.time || Date.now()),
        String(t.tid || '')
      );
    }
  }

  /** Override: l2Book is always full snapshot, no init sync needed */
  async _syncBook() {
    this._setState('syncing');

    if (this._wsSnapshotReceived) {
      this._finalizeWsSnapshotSync();
      return;
    }

    this._beginWsSnapshotSync();

    try {
      await this._waitForWsSnapshot(WS_SNAPSHOT_TIMEOUT_MS, 'ws l2Book timeout');
      if (this._state !== 'error') {
        this._finalizeWsSnapshotSync();
      }
    } catch (err) {
      if (err?.code === 'WS_SNAPSHOT_ABORTED') return;
      this._failWsSnapshotSync(`l2Book not received within ${WS_SNAPSHOT_TIMEOUT_MS}ms: ${err.message}`);
    }
  }

  /** Hyperliquid has no REST snapshot endpoint needed, but provide fallback */
  async _fetchSnapshot() {
    throw new Error('Hyperliquid uses WS l2Book only, no REST snapshot');
  }

}
