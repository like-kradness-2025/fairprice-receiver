// lib/fair-price-collector.mjs — Fair price / book / raw trades recorder for btc-receiver

import path from 'node:path';
import { BufferedWriter } from './buffered-writer.mjs';

const DEFAULT_TICK_INTERVAL_MS = 1000;
const DEFAULT_BOOK_SNAPSHOT_MS = 30000;
const DEFAULT_MARK_FETCH_MS = 5000;
const FETCH_TIMEOUT_MS = 10000;

/**
 * Pick fair price in priority order: mark price -> book mid -> last price.
 * @param {Object} params
 * @param {number|null|undefined} params.markPrice
 * @param {number|null|undefined} params.bookMid
 * @param {number|null|undefined} params.lastPrice
 * @returns {{ fairPrice: number, source: 'mark_price'|'book_mid'|'last_price' } | null}
 */
export function selectFairPrice({ markPrice, bookMid, lastPrice }) {
  if (Number.isFinite(markPrice) && markPrice > 0) {
    return { fairPrice: markPrice, source: 'mark_price' };
  }
  if (Number.isFinite(bookMid) && bookMid > 0) {
    return { fairPrice: bookMid, source: 'book_mid' };
  }
  if (Number.isFinite(lastPrice) && lastPrice > 0) {
    return { fairPrice: lastPrice, source: 'last_price' };
  }
  return null;
}

async function fetchJSON(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

function toPairPrice(value) {
  if (value == null) return null;
  const parsed = typeof value === 'number' ? value : parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Create a market-specific mark price fetcher.
 * Returns null for markets that do not expose a reliable mark price endpoint.
 * @param {string} market
 * @param {Object} cfg
 * @param {string} [cfg.symbol]
 * @returns {(() => Promise<number|null>) | null}
 */
export function createMarkPriceFetcher(market, cfg = {}) {
  const symbol = cfg.symbol || 'BTCUSDT';

  if (market === 'binance_perp') {
    const url = cfg.markPriceUrl || `https://fapi.binance.com/fapi/v1/premiumIndex?symbol=${symbol}`;
    return async () => {
      try {
        const d = await fetchJSON(url);
        return toPairPrice(d?.markPrice);
      } catch {
        return null;
      }
    };
  }

  if (market === 'bybit_perp') {
    const url = cfg.markPriceUrl || `https://api.bybit.com/v5/market/tickers?category=linear&symbol=${symbol}`;
    return async () => {
      try {
        const d = await fetchJSON(url, { headers: { 'User-Agent': 'btc-receiver/v3.00' } });
        return toPairPrice(d?.result?.list?.[0]?.markPrice);
      } catch {
        return null;
      }
    };
  }

  if (market === 'okx_perp') {
    const instId = cfg.symbol || 'BTC-USDT-SWAP';
    const url = cfg.markPriceUrl || `https://www.okx.com/api/v5/public/mark-price?instType=SWAP&instId=${instId}`;
    return async () => {
      try {
        const d = await fetchJSON(url, { headers: { 'User-Agent': 'btc-receiver/v3.00' } });
        return toPairPrice(d?.data?.[0]?.markPx);
      } catch {
        return null;
      }
    };
  }

  if (market === 'hyperliquid_perp') {
    const url = cfg.markPriceUrl || 'https://api.hyperliquid.xyz/info';
    return async () => {
      try {
        const d = await fetchJSON(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ type: 'metaAndAssetCtxs' }),
        });
        const ctxs = Array.isArray(d) ? d[1] : null;
        return toPairPrice(ctxs?.[0]?.markPx);
      } catch {
        return null;
      }
    };
  }

  return null;
}

export class FairPriceCollector {
  constructor(outputBase, options = {}) {
    this._outputBase = outputBase;
    this._tickIntervalMs = options.tickIntervalMs ?? DEFAULT_TICK_INTERVAL_MS;
    this._bookSnapshotMs = options.bookSnapshotMs ?? DEFAULT_BOOK_SNAPSHOT_MS;
    this._markFetchMs = options.markFetchMs ?? DEFAULT_MARK_FETCH_MS;

    /** @type {Map<string, { connector: any, book: any, markPriceFetcher: null | (() => Promise<number|null>) }>} */
    this._markets = new Map();
    /** @type {Map<string, BufferedWriter>} */
    this._fairPriceWriters = new Map();
    /** @type {Map<string, BufferedWriter>} */
    this._bookWriters = new Map();
    /** @type {Map<string, BufferedWriter>} */
    this._tradeWriters = new Map();

    /** @type {Map<string, number|null>} */
    this._lastMarkPrices = new Map();
    /** @type {Map<string, number>} */
    this._lastTradePrices = new Map();
    /** @type {Map<string, number>} */
    this._lastBookSnapshotAt = new Map();
    /** @type {Map<string, number>} */
    this._lastMarkFetchAt = new Map();

    this._timer = null;
    this._ticking = false;
    this._closed = false;
  }

  registerMarket(market, { connector, book, markPriceFetcher = null }) {
    if (this._markets.has(market)) return;

    const fairPriceWriter = new BufferedWriter(path.join(this._outputBase, 'fairprice', `${market}.jsonl`), {
      flushIntervalMs: 200,
    });
    const bookWriter = new BufferedWriter(path.join(this._outputBase, 'book', `${market}.jsonl`), {
      flushIntervalMs: 1000,
    });
    const tradeWriter = new BufferedWriter(path.join(this._outputBase, 'trades', `${market}.jsonl`), {
      flushIntervalMs: 200,
    });

    this._markets.set(market, { connector, book, markPriceFetcher });
    this._fairPriceWriters.set(market, fairPriceWriter);
    this._bookWriters.set(market, bookWriter);
    this._tradeWriters.set(market, tradeWriter);
    this._lastBookSnapshotAt.set(market, 0);
    this._lastMarkFetchAt.set(market, 0);

    connector.on('trade', (tradeEvent) => {
      this._lastTradePrices.set(market, tradeEvent.price);
      tradeWriter.write({ type: 'trade', ...tradeEvent });
    });
  }

  start() {
    if (this._timer) return;
    this._timer = setInterval(() => {
      this._tick().catch(err => {
        console.error(`[fairprice] tick error: ${err.message}`);
      });
    }, this._tickIntervalMs);
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
    const promises = [];
    for (const writer of this._fairPriceWriters.values()) promises.push(writer.close());
    for (const writer of this._bookWriters.values()) promises.push(writer.close());
    for (const writer of this._tradeWriters.values()) promises.push(writer.close());
    await Promise.allSettled(promises);
  }

  async _tick() {
    if (this._closed || this._ticking) return;
    this._ticking = true;
    try {
      const now = Date.now();

      await this._refreshMarks(now);

      for (const [market, entry] of this._markets) {
        const { connector, book } = entry;
        if (!connector || connector.getState() !== 'running' || !book || book.isEmpty()) {
          continue;
        }

        if (now - (this._lastBookSnapshotAt.get(market) ?? 0) >= this._bookSnapshotMs) {
          this._lastBookSnapshotAt.set(market, now);
          this._bookWriters.get(market)?.write(book.toSnapshot(now));
        }

        const bookMid = book.getMid();
        const lastPrice = this._lastTradePrices.get(market) ?? null;
        const markPrice = this._lastMarkPrices.get(market) ?? null;
        const picked = selectFairPrice({ markPrice, bookMid, lastPrice });
        if (!picked) continue;

        this._fairPriceWriters.get(market)?.write({
          type: 'fair_price',
          ts: now,
          market,
          fair_price: picked.fairPrice,
          fair_price_source: picked.source,
          mark_price: markPrice,
          book_mid: bookMid,
          last_price: lastPrice,
        });
      }
    } finally {
      this._ticking = false;
    }
  }

  async _refreshMarks(now) {
    const tasks = [];
    for (const [market, entry] of this._markets) {
      const { markPriceFetcher } = entry;
      if (!markPriceFetcher) continue;

      const lastFetch = this._lastMarkFetchAt.get(market) ?? 0;
      if (now - lastFetch < this._markFetchMs) continue;

      this._lastMarkFetchAt.set(market, now);
      tasks.push(
        markPriceFetcher().then((price) => {
          this._lastMarkPrices.set(market, price);
        }).catch(() => {
          this._lastMarkPrices.set(market, null);
        }),
      );
    }

    await Promise.allSettled(tasks);
  }
}