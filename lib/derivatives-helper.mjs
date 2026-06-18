// lib/derivatives-helper.mjs — Perp auxiliary data collector for btc-receiver v3.00
// Periodically fetches mark price, funding rate, and open interest from perp exchanges.
// Writes formatted rows to derivatives/{market}.jsonl

import { BufferedWriter } from './buffered-writer.mjs';

const DEFAULT_INTERVAL_MS = 5000;
const FETCH_TIMEOUT_MS = 10000;

/**
 * @typedef {Object} DerivativeRow
 * @property {number} ts
 * @property {string} market
 * @property {number|null} mark_price
 * @property {number|null} funding_rate
 * @property {number|null} open_interest
 * @property {number|null} next_funding_time
 * @property {string} source
 */

export class DerivativesHelper {
  /**
   * @param {string} outputBase - base directory for output files
   * @param {Object} [options]
   * @param {number} [options.intervalMs=5000]
   */
  constructor(outputBase, options = {}) {
    this._intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
    /** @type {Map<string, BufferedWriter>} */
    this._writers = new Map();
    this._timer = null;
    this._closed = false;
    this._outputBase = outputBase;
  }

  /**
   * Register a perp market for auxiliary data collection.
   * @param {string} market  e.g. 'binance_perp'
   * @param {Object} restUrls - REST endpoint URLs for auxiliary data
   * @param {string} [restUrls.premiumIndex] - Binance perp premiumIndex URL
   * @param {string} [restUrls.openInterest] - Binance perp / OKX open interest URL
   * @param {string} [restUrls.tickers] - Bybit tickers URL
   * @param {string} [restUrls.fundingRate] - OKX funding rate URL
   */
  registerMarket(market, restUrls) {
    if (this._writers.has(market)) return;
    const writer = new BufferedWriter(
      `${this._outputBase}/derivatives/${market}.jsonl`,
      { flushIntervalMs: 1000, maxBufferLines: 100 },
    );
    this._writers.set(market, { writer, restUrls });
  }

  /** Start periodic collection. */
  start() {
    if (this._timer) return;
    this._timer = setInterval(() => this._tick(), this._intervalMs);
    if (this._timer.unref) this._timer.unref();
  }

  /** Stop periodic collection. */
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
    for (const { writer } of this._writers.values()) {
      promises.push(writer.close());
    }
    await Promise.allSettled(promises);
  }

  async _tick() {
    if (this._closed) return;
    const now = Date.now();
    const promises = [];
    for (const [market, { writer }] of this._writers) {
      promises.push(
        this._fetchMarket(market, writer, now).catch(err => {
          console.error(`[derivatives] ${market} fetch error: ${err.message}`);
        }),
      );
    }
    await Promise.allSettled(promises);
  }

  async _fetchMarket(market, writer, now) {
    let row = null;
    if (market.startsWith('binance_perp')) {
      row = await this._fetchBinancePerp(now);
    } else if (market.startsWith('bybit_perp') || market.startsWith('bybit')) {
      row = await this._fetchBybit(now);
    } else if (market.startsWith('okx_perp') || market.startsWith('okx')) {
      row = await this._fetchOkx(now);
    } else if (market.startsWith('hyperliquid_perp') || market.startsWith('hyperliquid')) {
      row = await this._fetchHyperliquid(now);
    }
    if (row) {
      row.market = market;
      row.ts = now;
      row.source = market.replace(/_perp$|_.*$/, '');
      await writer.write(row);
    }
  }

  // ====== Binance perp ======

  async _fetchBinancePerp(now) {
    // premiumIndex gives markPrice + lastFundingRate
    let markPrice = null, fundingRate = null, nextFundingTime = null;
    let openInterest = null;

    try {
      const premiumRes = await fetch(
        'https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT',
        { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) },
      );
      if (premiumRes.ok) {
        const d = await premiumRes.json();
        markPrice = d.markPrice != null ? parseFloat(d.markPrice) : null;
        fundingRate = d.lastFundingRate != null ? parseFloat(d.lastFundingRate) : null;
        nextFundingTime = d.nextFundingTime || null;
      }
    } catch (err) {
      // silently continue
    }

    try {
      const oiRes = await fetch(
        'https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT',
        { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) },
      );
      if (oiRes.ok) {
        const d = await oiRes.json();
        openInterest = d.openInterest != null ? parseFloat(d.openInterest) : null;
      }
    } catch (err) {
      // silently continue
    }

    return { mark_price: markPrice, funding_rate: fundingRate, open_interest: openInterest, next_funding_time: nextFundingTime };
  }

  // ====== Bybit perp ======

  async _fetchBybit(now) {
    let markPrice = null, fundingRate = null, openInterest = null, nextFundingTime = null;

    try {
      const res = await fetch(
        'https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT',
        { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) },
      );
      if (res.ok) {
        const d = await res.json();
        if (d.result?.list?.length > 0) {
          const t = d.result.list[0];
          markPrice = t.markPrice != null ? parseFloat(t.markPrice) : null;
          fundingRate = t.fundingRate != null ? parseFloat(t.fundingRate) : null;
          openInterest = t.openInterest != null ? parseFloat(t.openInterest) : null;
          nextFundingTime = t.nextFundingTime ? parseInt(t.nextFundingTime, 10) : null;
        }
      }
    } catch (err) {
      // silently continue
    }

    return { mark_price: markPrice, funding_rate: fundingRate, open_interest: openInterest, next_funding_time: nextFundingTime };
  }

  // ====== OKX perp ======

  async _fetchOkx(now) {
    let markPrice = null, fundingRate = null, openInterest = null, nextFundingTime = null;

    // funding rate
    try {
      const frRes = await fetch(
        'https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP',
        { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) },
      );
      if (frRes.ok) {
        const d = await frRes.json();
        if (d.data?.length > 0) {
          fundingRate = d.data[0].fundingRate != null ? parseFloat(d.data[0].fundingRate) : null;
          nextFundingTime = d.data[0].fundingTime ? parseInt(d.data[0].fundingTime, 10) : null;
        }
      }
    } catch (err) {
      // silently continue
    }

    // open interest
    try {
      const oiRes = await fetch(
        'https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=BTC-USDT-SWAP',
        { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) },
      );
      if (oiRes.ok) {
        const d = await oiRes.json();
        if (d.data?.length > 0) {
          openInterest = d.data[0].oi != null ? parseFloat(d.data[0].oi) : null;
        }
      }
    } catch (err) {
      // silently continue
    }

    // mark price from dedicated mark-price endpoint (more reliable than ticker.markPx)
    try {
      const mkRes = await fetch(
        'https://www.okx.com/api/v5/public/mark-price?instType=SWAP&instId=BTC-USDT-SWAP',
        { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) },
      );
      if (mkRes.ok) {
        const d = await mkRes.json();
        if (d.data?.length > 0) {
          markPrice = d.data[0].markPx != null ? parseFloat(d.data[0].markPx) : null;
        }
      }
    } catch (err) {
      // silently continue
    }

    return { mark_price: markPrice, funding_rate: fundingRate, open_interest: openInterest, next_funding_time: nextFundingTime };
  }

  // ====== Hyperliquid perp ======
  // Uses info POST type=metaAndAssetCtxs which returns all asset contexts in one call.
  // Index 0 = BTC (first asset in the universe).

  async _fetchHyperliquid(now) {
    let markPrice = null, fundingRate = null, openInterest = null, nextFundingTime = null;

    try {
      const res = await fetch('https://api.hyperliquid.xyz/info', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'metaAndAssetCtxs' }),
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
      });
      if (res.ok) {
        const d = await res.json();
        // d is [universe[], assetCtxs[]] — assetCtxs[0] = BTC
        // asset ctx fields: { markPx, funding, openInterest, ... }
        if (Array.isArray(d) && d.length >= 2) {
          const ctxs = d[1]; // array of asset contexts
          if (Array.isArray(ctxs) && ctxs.length > 0) {
            const btc = ctxs[0]; // BTC is first in universe
            markPrice = btc.markPx != null ? parseFloat(btc.markPx) : null;
            fundingRate = btc.funding != null ? parseFloat(btc.funding) : null;
            openInterest = btc.openInterest != null ? parseFloat(btc.openInterest) : null;
            // nextFundingTime not available from metaAndAssetCtxs
          }
        }
      }
    } catch (err) {
      // silently continue
    }

    return { mark_price: markPrice, funding_rate: fundingRate, open_interest: openInterest, next_funding_time: nextFundingTime };
  }
}
