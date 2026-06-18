// lib/kraken-connector.mjs — Kraken spot connector for btc-receiver v3.00

import { BaseConnector } from './base-connector.mjs';
import { FullBook } from './full-book.mjs';

const WS_SNAPSHOT_TIMEOUT_MS = 15000;

const toPairs = (rows = []) => rows.map(([p, q]) => [String(p), String(q)]);
const isBookArrayFrame = (data) => Array.isArray(data) && data[1] && typeof data[1] === 'object' && !Array.isArray(data[1]) && typeof data[2] === 'string' && data[2].startsWith('book-');
const isTradeArrayFrame = (data) => Array.isArray(data) && Array.isArray(data[1]) && data[2] === 'trade';
const restBookKey = (result, preferred) => {
  if (!result || typeof result !== 'object') return null;
  if (preferred && result[preferred]) return preferred;
  return Object.keys(result).find((k) => k !== 'last') || null;
};
const checksumToken = (value) => String(value).replace('.', '').replace(/^0+/, '');
const crc32 = (str) => {
  let crc = ~0;
  for (let i = 0; i < str.length; i++) {
    crc ^= str.charCodeAt(i);
    for (let j = 0; j < 8; j++) {
      crc = (crc >>> 1) ^ (0xEDB88320 & -(crc & 1));
    }
  }
  return (~crc) >>> 0;
};

export function calculateKrakenChecksum(book) {
  const { bids, asks } = book.getTop(10);
  let payload = '';
  for (const [price, qty] of asks) payload += checksumToken(price) + checksumToken(qty);
  for (const [price, qty] of bids) payload += checksumToken(price) + checksumToken(qty);
  return crc32(payload);
}

export class KrakenSpotConnector extends BaseConnector {
  constructor(config) {
    super(config, {
      market: 'kraken_spot',
      wsUrl: config.wsUrl || 'wss://ws.kraken.com',
      restUrl: config.restUrl || 'https://api.kraken.com/0/public/Depth?pair=XBTUSD&count=1000',
    });
    this.symbol = config.symbol || 'XBT/USD';
    this.restPair = config.restPair || 'XBTUSD';
    this.book = new FullBook('kraken_spot', { maxLevels: config.depthLimit ?? 1000 });
  }

  subscribe() {
    this._ws.send(JSON.stringify({
      event: 'subscribe',
      pair: [this.symbol],
      subscription: { name: 'book', depth: 1000 },
    }));
    this._ws.send(JSON.stringify({
      event: 'subscribe',
      pair: [this.symbol],
      subscription: { name: 'trade' },
    }));
  }

  _onMessage(data) {
    if (data.event === 'subscriptionStatus' || data.event === 'systemStatus' || data.event === 'heartbeat') return;
    if (data.event === 'pong' || data.event === 'ping') return;
    if (data.event === 'error') {
      this.emit('error', { market: this.market, message: `subscription error: ${data.errorMessage || JSON.stringify(data)}` });
      return;
    }
    if (Array.isArray(data)) {
      if (isBookArrayFrame(data)) {
        if (data[1].as || data[1].bs) return this._handleBookSnapshot(data[1]);
        if (data[1].a || data[1].b) return this._handleBookUpdate(data[1]);
      }
      if (isTradeArrayFrame(data)) return this._handleTrades(data);
      return;
    }
    if (data.as || data.bs) return this._handleBookSnapshot(data);
    if (data.a || data.b) return this._handleBookUpdate(data);
  }

  _handleBookSnapshot(data) {
    const bids = toPairs(data.bs || []);
    const asks = toPairs(data.as || []);
    const ts = Date.now();
    this.book.applySnapshot(bids, asks);
    this._emitDepth('snapshot', bids, asks, ts);
    this._notifyWsSnapshotReceived(null);
    this._replayRingBufAfterSnapshot();
    this._ringBuf = [];
  }

  _replayRingBufAfterSnapshot() {
    const pending = this._ringBuf;
    this._ringBuf = [];
    for (const msg of pending) {
      if (msg?.a || msg?.b) this._handleBookUpdate(msg);
    }
  }

  _handleBookUpdate(data) {
    if (!this._wsSnapshotReceived) {
      this._bufferMsg(data);
      return;
    }
    if (this._state === 'reconnecting') return;
    const bids = toPairs(data.b || []);
    const asks = toPairs(data.a || []);
    const ts = Date.now();
    const seq = Number.isFinite(Number(data.c)) ? Number(data.c) : undefined;
    for (const [p, q] of bids) this.book.applyDiff('bid', p, q, seq);
    for (const [p, q] of asks) this.book.applyDiff('ask', p, q, seq);
    if (seq !== undefined) {
      const localChecksum = calculateKrakenChecksum(this.book);
      if (localChecksum !== seq) {
        this._handleSequenceGap(`Kraken checksum mismatch: local=${localChecksum}, remote=${seq}`, data);
        return;
      }
    }
    this._emitDepth('update', bids, asks, ts, seq);
  }

  _handleTrades(msg) {
    const trades = msg[1] || [];
    for (const t of trades) {
      const [price, qty, time, side, orderType, misc] = t;
      this._emitTrade(parseFloat(price), parseFloat(qty), side === 's' ? 'sell' : 'buy', Math.floor(Number(time) * 1000) || Date.now(), `${time}-${price}-${qty}-${orderType}-${misc}`);
    }
  }

  async _syncBook() {
    this._setState('syncing');
    if (this._wsSnapshotReceived) {
      this._finalizeWsSnapshotSync();
      return;
    }
    this._beginWsSnapshotSync();
    try {
      await this._waitForWsSnapshot(WS_SNAPSHOT_TIMEOUT_MS, 'ws snapshot timeout');
    } catch (err) {
      if (err?.code === 'WS_SNAPSHOT_ABORTED') return;
      const snapshot = await this._fetchSnapshot();
      const key = restBookKey(snapshot.result, this.restPair);
      const bids = toPairs(snapshot.result?.[key]?.bids || []);
      const asks = toPairs(snapshot.result?.[key]?.asks || []);
      const ts = Date.now();
      this.book.applySnapshot(bids, asks);
      this._emitDepth('snapshot', bids, asks, ts);
      this._notifyWsSnapshotReceived(null);
    }
    if (this._state !== 'error') this._finalizeWsSnapshotSync();
  }

  async _fetchSnapshot() {
    const res = await fetch(this.restUrl, { signal: AbortSignal.timeout(10000) });
    if (!res.ok) throw new Error(`REST snapshot ${res.status}`);
    return res.json();
  }
}
