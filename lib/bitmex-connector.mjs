// lib/bitmex-connector.mjs — BitMEX XBTUSD perp connector

import { TradeOnlyConnector } from './trade-only-connector.mjs';

const WS_URL = 'wss://ws.bitmex.com/realtime?subscribe=trade:XBTUSD,orderBookL2:XBTUSD';

const toNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const normalizeSide = (value) => {
  if (value == null) return null;
  const v = String(value).toLowerCase();
  if (v === 'buy' || v === 'bid' || v === 'b') return 'buy';
  if (v === 'sell' || v === 'ask' || v === 's') return 'sell';
  return null;
};

const normalizeTs = (value) => {
  if (value == null) return Date.now();
  const n = Date.parse(value);
  if (Number.isFinite(n)) return n;
  const num = Number(value);
  if (!Number.isFinite(num)) return Date.now();
  if (num > 1e15) return Math.floor(num / 1000);
  if (num > 1e12) return Math.floor(num);
  return Math.floor(num * 1000);
};

const emitTrade = (conn, trade) => {
  if (!trade || typeof trade !== 'object') return;
  const price = toNumber(trade.price ?? trade.p);
  const contracts = toNumber(trade.size ?? trade.s);
  const side = normalizeSide(trade.side);
  const ts = normalizeTs(trade.timestamp ?? trade.time ?? trade.transactTime);
  const tradeId = String(trade.trdMatchID ?? trade.tradeID ?? trade.id ?? `${ts}-${price}-${contracts}`);
  if (price == null || contracts == null || !side) return;
  // XBTUSD is inverse; 1 contract = 1 USD of BTC notional.
  const qty = contracts / price;
  conn._emitTrade(price, qty, side, ts, tradeId);
};

export class BitmexConnector extends TradeOnlyConnector {
  constructor(config) {
    super(config, {
      market: 'bitmex_perp',
      wsUrl: config.wsUrl || WS_URL,
      restUrl: config.restUrl || '',
    });
    // Internal L2 orderbook: Map<id, {price, side, size}>
    // BitMEX orderBookL2 is ID-based with possible price duplicates.
    // We aggregate by price on each emit.
    this._l2book = new Map();
    this._pingTimer = setInterval(() => {
      if (this._ws && this._ws.readyState === 1) {
        this._ws.send('ping');
      }
    }, 20000);
    this._pingTimer.unref();
  }

  /** Handle raw 'pong' string before JSON parse. */
  _preprocessRaw(raw) {
    const str = raw.toString();
    if (str === 'pong' || str === '"pong"') return true; // handled
    return false;
  }

  _clearTimers() {
    super._clearTimers();
    if (this._pingTimer) {
      clearInterval(this._pingTimer);
      this._pingTimer = null;
    }
  }

  subscribe() {
    // Subscription is embedded in the websocket URL; no additional frame required.
  }

  _onMessage(data) {
    if (!data || typeof data !== 'object') return;
    if (data.table === 'orderBookL2') {
      this._handleDepth(data);
      return;
    }
    if (data.table !== 'trade') return;
    const trades = Array.isArray(data.data) ? data.data : [];
    for (const trade of trades) emitTrade(this, trade);
  }

  // ---- Depth handling ----

  /** @param {Object} data — BitMEX orderBookL2 message {table, action, data: [...]} */
  _handleDepth(data) {
    const action = data.action;
    const entries = Array.isArray(data.data) ? data.data : [];
    if (!action || entries.length === 0) return;

    switch (action) {
      case 'partial':
        this._l2book.clear();
        for (const e of entries) {
          this._insertLevel(e);
        }
        break;
      case 'insert':
        for (const e of entries) {
          this._insertLevel(e);
        }
        break;
      case 'update':
        for (const e of entries) {
          this._updateLevel(e);
        }
        break;
      case 'delete':
        for (const e of entries) {
          this._l2book.delete(e.id);
        }
        break;
      default:
        return;
    }

    const type = action === 'partial' ? 'snapshot' : 'update';
    const { bids, asks } = this._aggregateBook();
    this._emitDepth(type, bids, asks, Date.now(), null);
  }

  _insertLevel(entry) {
    if (entry.id == null || entry.price == null) return;
    this._l2book.set(entry.id, {
      price: toNumber(entry.price),
      side: entry.side || null,
      size: toNumber(entry.size) || 0,
    });
  }

  _updateLevel(entry) {
    if (entry.id == null) return;
    const existing = this._l2book.get(entry.id);
    if (!existing) {
      // Treat orphan update as insert
      this._insertLevel(entry);
      return;
    }
    if (entry.size != null) {
      existing.size = toNumber(entry.size) || 0;
    }
    if (entry.price != null) {
      existing.price = toNumber(entry.price);
    }
    if (entry.side != null) {
      existing.side = entry.side;
    }
  }

  /**
   * Aggregate the ID-based L2 book into price-based bids/asks.
   * Same-price entries have their sizes summed.
   * @returns {{ bids: Array<[number, number]>, asks: Array<[number, number]> }}
   */
  _aggregateBook() {
    const bidMap = new Map();  // price -> totalSize
    const askMap = new Map();

    for (const [, level] of this._l2book) {
      if (level.size <= 0 || level.price == null) continue;
      const map = level.side === 'Buy' ? bidMap : askMap;
      const prev = map.get(level.price) || 0;
      map.set(level.price, prev + level.size);
    }

    const bids = [...bidMap.entries()].sort((a, b) => b[0] - a[0]);
    const asks = [...askMap.entries()].sort((a, b) => a[0] - b[0]);

    return { bids, asks };
  }

  /** Clear internal L2 book on reconnect/reset (in addition to FullBook). */
  _resetBook() {
    super._resetBook();
    this._l2book.clear();
  }
}
