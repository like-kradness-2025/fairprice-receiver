// lib/feature-computer.mjs — Feature computation for btc-receiver v3.00

import { FullBook } from './full-book.mjs';

/**
 * @typedef {Object} FeatureRow
 * @property {number} ts
 * @property {string} market
 * @property {number|null} best_bid
 * @property {number|null} best_ask
 * @property {number|null} mid
 * @property {number|null} spread
 * @property {number|null} imbalance_1pct
 * @property {number} trade_count_1s
 * @property {number} buy_volume_1s
 * @property {number} sell_volume_1s
 */

export class FeatureComputer {
  /**
   * Compute a feature row from book and aggregated trade data.
   * @param {string} market
   * @param {FullBook} book
   * @param {import('./trade-aggregator.mjs').TradeAggregatedRow|null} aggTrade
   * @param {number} ts
   * @returns {FeatureRow}
   */
  compute(market, book, aggTrade, ts) {
    const bestBidStr = book.getBestBid();
    const bestAskStr = book.getBestAsk();
    const bestBid = bestBidStr !== null ? parseFloat(bestBidStr) : null;
    const bestAsk = bestAskStr !== null ? parseFloat(bestAskStr) : null;

    return {
      ts,
      market,
      best_bid: bestBid,
      best_ask: bestAsk,
      mid: book.getMid(),
      spread: book.getSpread(),
      imbalance_1pct: book.getImbalance(1.0),
      trade_count_1s: aggTrade ? aggTrade.trade_count : 0,
      buy_volume_1s: aggTrade ? aggTrade.buy_volume : 0,
      sell_volume_1s: aggTrade ? aggTrade.sell_volume : 0,
    };
  }
}
