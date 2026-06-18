// lib/bitstamp-connector.mjs — Bitstamp BTC/USD spot connector

import { TradeOnlyConnector } from './trade-only-connector.mjs';

const WS_URL = 'wss://ws.bitstamp.net';
const CHANNEL = 'live_trades_btcusd';

const toNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const normalizeSide = (value) => {
  if (value == null) return null;
  if (typeof value === 'string') {
    const v = value.toLowerCase();
    if (v === 'buy' || v === 'b' || v === '0' || v === 'bid') return 'buy';
    if (v === 'sell' || v === 's' || v === '1' || v === 'ask') return 'sell';
  }
  if (value === 0) return 'buy';
  if (value === 1) return 'sell';
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

export class BitstampConnector extends TradeOnlyConnector {
  constructor(config) {
    super(config, {
      market: 'bitstamp_spot',
      wsUrl: config.wsUrl || WS_URL,
      restUrl: config.restUrl || '',
    });
    this._depthInitialized = false;
  }

  subscribe() {
    for (const ch of ['live_trades_btcusd', 'diff_order_book_btcusd']) {
      this._ws.send(JSON.stringify({
        event: 'bts:subscribe',
        data: { channel: ch },
      }));
    }
  }

  _onMessage(data) {
    if (!data || typeof data !== 'object') return;

    const event = data.event || data.event_type || data.type;
    const channel = data.channel || data.channel_name || '';

    if (event === 'bts:subscription_succeeded' || event === 'bts:unsubscription_succeeded' || event === 'bts:request_reconnect' || event === 'bts:heartbeat') {
      return;
    }

    // Depth channel
    if (event === 'data' && channel === 'diff_order_book_btcusd') {
      const payload = data.data;
      if (payload) {
        const bids = payload.bids || [];
        const asks = payload.asks || [];
        const ts = Number(payload.microtimestamp) / 1000;
        const type = this._depthInitialized ? 'update' : 'snapshot';
        this._handleDepth(type, bids, asks, ts, null);
        this._depthInitialized = true;
      }
      return;
    }

    if (event !== 'trade' && channel !== CHANNEL) return;

    const payload = data.data ?? data;
    const trades = Array.isArray(payload)
      ? payload
      : (payload && Array.isArray(payload.data) ? payload.data : [payload]);

    for (const t of trades) {
      if (!t) continue;
      const price = toNumber(t.price ?? t.price_str ?? t.p ?? (Array.isArray(t) ? t[1] : null));
      const qty = toNumber(t.amount ?? t.amount_str ?? t.qty ?? t.q ?? (Array.isArray(t) ? t[2] : null));
      const side = normalizeSide(t.side ?? t.type ?? t.order_type ?? (Array.isArray(t) ? t[3] : null));
      const ts = normalizeTs(t.microtimestamp ?? t.timestamp ?? t.time ?? t.E ?? (Array.isArray(t) ? t[4] : null));
      const tradeId = String(t.id ?? t.trade_id ?? t.microtimestamp ?? `${ts}-${price}-${qty}`);
      if (price == null || qty == null || !side) continue;
      this._emitTrade(price, qty, side, ts, tradeId);
    }
  }

  /**
   * Handle depth data from diff_order_book_btcusd channel.
   * @param {'snapshot'|'update'} type
   * @param {Array<[string, string]>} bids - [price, amount] pairs
   * @param {Array<[string, string]>} asks - [price, amount] pairs
   * @param {number} ts - timestamp in seconds
   * @param {number|null} seq - sequence number (null for Bitstamp)
   */
  _handleDepth(type, bids, asks, ts, seq) {
    if (type === 'snapshot') {
      this.book.applySnapshot(bids, asks, seq);
    } else {
      for (const [price, qty] of bids) {
        this.book.applyDiff('bid', price, qty, seq);
      }
      for (const [price, qty] of asks) {
        this.book.applyDiff('ask', price, qty, seq);
      }
    }
    this._emitDepth(type, bids, asks, ts, seq);
  }

  _resetBook() {
    this._depthInitialized = false;
    super._resetBook();
  }
}
