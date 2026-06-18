// lib/gemini-connector.mjs — Gemini BTC/USD spot connector

import { TradeOnlyConnector } from './trade-only-connector.mjs';

const WS_URL = 'wss://api.gemini.com/v2/marketdata/';
const SYMBOL = 'btcusd';

const toNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const normalizeSide = (trade) => {
  if (trade == null || typeof trade !== 'object') return null;
  if (trade.m != null) return trade.m ? 'sell' : 'buy';
  if (trade.side != null) {
    const v = String(trade.side).toLowerCase();
    if (v === 'buy' || v === 'bid' || v === 'b') return 'buy';
    if (v === 'sell' || v === 'ask' || v === 's') return 'sell';
  }
  if (trade.makerSide != null) {
    const v = String(trade.makerSide).toLowerCase();
    if (v === 'buy' || v === 'bid') return 'sell';
    if (v === 'sell' || v === 'ask') return 'buy';
  }
  return null;
};

const normalizeTs = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return Date.now();
  if (n > 1e15) return Math.floor(n / 1000);
  if (n > 1e12) return Math.floor(n);
  if (n > 1e9) return Math.floor(n * 1000);
  return Math.floor(n);
};

const collectTrades = (data) => {
  if (!data || typeof data !== 'object') return [];
  if (Array.isArray(data.events)) {
    const out = [];
    for (const event of data.events) {
      if (!event || typeof event !== 'object') continue;
      if (Array.isArray(event.trades)) {
        out.push(...event.trades);
      } else if (event.type === 'trade' || event.p != null || event.q != null || event.m != null) {
        out.push(event);
      }
    }
    if (out.length) return out;
  }
  if (Array.isArray(data.trades)) return data.trades;
  if (data.type === 'trade' || data.event === 'trade' || data.m != null || data.p != null || data.q != null) {
    return [data];
  }
  return [];
};

const DEPTH_WS_URL = 'wss://api.gemini.com/v2/marketdata/';

export class GeminiConnector extends TradeOnlyConnector {
  constructor(config) {
    super(config, {
      market: 'gemini_spot',
      wsUrl: config.wsUrl || WS_URL,
      restUrl: config.restUrl || '',
    });
    this._depthWs = null;
    this._depthSnapshotReceived = false;
  }

  subscribe() {
    // Subscribe to l2 on V2 marketdata endpoint (includes trades + depth)
    this._ws.send(JSON.stringify({
      type: 'subscribe',
      subscriptions: [{ name: 'l2', symbols: [SYMBOL] }],
    }));

    // Depth subscription on separate WS (same endpoint, also l2 — redundant but works)
    this._closeDepthWs();
    this._depthSnapshotReceived = false;
    this._connectDepthWs();
  }

  async _connectDepthWs() {
    const WebSocket = await this._getWsImpl();
    if (!WebSocket) {
      this.emit('error', { market: this.market, message: 'depth WS: no WebSocket implementation' });
      return;
    }

    this._depthWs = new WebSocket(DEPTH_WS_URL);

    this._depthWs.on('open', () => {
      this._depthWs.send(JSON.stringify({
        type: 'subscribe',
        subscriptions: [{ name: 'l2', symbols: [SYMBOL] }],
      }));
    });

    this._depthWs.on('message', (raw) => {
      this._lastMsgAt = Date.now();
      try {
        const data = JSON.parse(raw.toString());
        if (data.type === 'l2_updates') {
          this._handleDepthUpdate(data);
        }
      } catch { /* ignore parse errors */ }
    });

    this._depthWs.on('close', () => {
      // Depth WS closed; main WS reconnect cycle will recreate it
    });

    this._depthWs.on('error', (err) => {
      this.emit('error', { market: this.market, message: 'depth WS error: ' + err.message });
    });
  }

  _handleDepthUpdate(data) {
    const ts = Date.now();
    const changes = data.changes || [];

    // Apply each change to the local book
    for (const change of changes) {
      const side = change.side; // 'bid' or 'ask'
      const price = change.price;
      const remaining = change.remaining;
      // Delete level if qty is zero
      const qty = (remaining === '0' || remaining === '0.0') ? '' : remaining;
      this.book.applyDiff(side, price, qty);
    }

    // Emit full book state: first message is snapshot, subsequent are updates
    const snapshot = this.book.toSnapshot(ts);
    const type = this._depthSnapshotReceived ? 'update' : 'snapshot';
    this._depthSnapshotReceived = true;

    this._emitDepth(type, snapshot.bids, snapshot.asks, ts, null);

    // Gemini V2 l2_updates includes a trades array; emit individual trades
    const tradeRows = data.trades || [];
    for (const t of tradeRows) {
      if (!t || typeof t !== 'object') continue;
      const price = toNumber(t.price ?? t.px);
      const qty = toNumber(t.quantity ?? t.qty ?? t.size);
      if (price == null || qty == null) continue;
      // Gemini side is maker side; invert for taker side
      const side = normalizeSide(t);
      const tradeTs = Number.isFinite(t.timestamp) ? t.timestamp : (Number.isFinite(t.ts) ? t.ts : Date.parse(t.timestamp) || ts);
      const tradeId = String(t.event_id ?? t.trade_id ?? t.tradeId ?? t.id ?? `${tradeTs}-${price}-${qty}`);
      if (side) {
        this._emitTrade(price, qty, side, tradeTs, tradeId);
      }
    }
  }

  _closeDepthWs() {
    if (this._depthWs) {
      try { this._depthWs.close(1000, 'reconnect'); } catch { /* ignore */ }
      this._depthWs = null;
    }
  }

  _resetBook() {
    super._resetBook();
    this._closeDepthWs();
    this._depthSnapshotReceived = false;
  }

  disconnect() {
    this._closeDepthWs();
    super.disconnect();
  }

  _onMessage(data) {
    if (!data || typeof data !== 'object') return;
    if (data.type === 'heartbeat' || data.type === 'subscription_ack' || data.type === 'subscribed') return;

    // l2_updates from V2 marketdata: trades embedded in data.trades
    const trades = collectTrades(data);
    for (const t of trades) {
      const price = toNumber(t.p ?? t.price ?? (Array.isArray(t) ? t[1] : null));
      const qty = toNumber(t.q ?? t.amount ?? t.size ?? (Array.isArray(t) ? t[2] : null));
      const side = normalizeSide(t);
      const ts = normalizeTs(t.E ?? t.timestamp ?? t.time ?? data.timestamp ?? data.time ?? (Array.isArray(t) ? t[3] : null));
      const tradeId = String(t.tid ?? t.trade_id ?? t.tradeId ?? `${ts}-${price}-${qty}`);
      if (price == null || qty == null || !side) continue;
      this._emitTrade(price, qty, side, ts, tradeId);
    }
  }
}
