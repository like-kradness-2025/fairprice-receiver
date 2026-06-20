// test/trade-aggregator.test.mjs — TradeAggregator unit tests

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { TradeAggregator, classifyTradeNotional } from '../lib/trade-aggregator.mjs';

describe('TradeAggregator', () => {
  describe('constructor', () => {
    it('should initialize empty', () => {
      const agg = new TradeAggregator('binance_spot', 1000);
      assert.strictEqual(agg.market, 'binance_spot');
      assert.strictEqual(agg.getPendingCount(), 0);
    });
  });

  describe('addTrade and flushIfDue', () => {
    it('should return null when buffer empty', () => {
      const agg = new TradeAggregator('test', 1000);
      assert.strictEqual(agg.flushIfDue(5000), null);
    });

    it('should return null when window not elapsed', () => {
      const agg = new TradeAggregator('test', 1000);
      agg.addTrade({ market: 'test', price: 65000, qty: 1, side: 'buy', ts: 1000 });
      assert.strictEqual(agg.flushIfDue(1500), null);
    });

    it('should aggregate trades within window', () => {
      const agg = new TradeAggregator('test', 1000);
      agg.addTrade({ market: 'test', price: 100, qty: 1, side: 'buy', ts: 1000 });
      agg.addTrade({ market: 'test', price: 102, qty: 2, side: 'sell', ts: 1100 });
      agg.addTrade({ market: 'test', price: 101, qty: 1.5, side: 'buy', ts: 1200 });

      const result = agg.flushIfDue(2000);
      assert.notStrictEqual(result, null);
      assert.strictEqual(result.market, 'test');
      assert.strictEqual(result.ts, 1000);
      assert.strictEqual(result.open, 100);
      assert.strictEqual(result.high, 102);
      assert.strictEqual(result.low, 100);
      assert.strictEqual(result.close, 101);
      assert.strictEqual(result.volume, 4.5);
      assert.strictEqual(result.buy_volume, 2.5);
      assert.strictEqual(result.sell_volume, 2);
      assert.strictEqual(result.trade_count, 3);
      assert.strictEqual(result.buy_count, 2);
      assert.strictEqual(result.sell_count, 1);
      // VWAP = (100*1 + 102*2 + 101*1.5) / (1+2+1.5) = (100 + 204 + 151.5) / 4.5 = 455.5 / 4.5 = 101.22
      assert.strictEqual(result.vwap, 101.22);
    });

    it('should reset after flush', () => {
      const agg = new TradeAggregator('test', 1000);
      agg.addTrade({ market: 'test', price: 100, qty: 1, side: 'buy', ts: 1000 });
      agg.flushIfDue(2000);
      assert.strictEqual(agg.getPendingCount(), 0);
      assert.strictEqual(agg.flushIfDue(3000), null);
    });

    it('should handle single trade', () => {
      const agg = new TradeAggregator('test', 1000);
      agg.addTrade({ market: 'test', price: 50000, qty: 0.5, side: 'sell', ts: 5000 });
      const result = agg.flushIfDue(6000);
      assert.strictEqual(result.open, 50000);
      assert.strictEqual(result.high, 50000);
      assert.strictEqual(result.low, 50000);
      assert.strictEqual(result.close, 50000);
      assert.strictEqual(result.vwap, 50000);
    });
  });

  describe('reset', () => {
    it('should clear buffer', () => {
      const agg = new TradeAggregator('test', 1000);
      agg.addTrade({ market: 'test', price: 100, qty: 1, side: 'buy', ts: 1000 });
      agg.reset();
      assert.strictEqual(agg.getPendingCount(), 0);
      assert.strictEqual(agg.flushIfDue(2000), null);
    });
  });

  describe('empty flush returns null', () => {
    it('should return null when no trades added', () => {
      const agg = new TradeAggregator('test', 1000);
      assert.strictEqual(agg.flushIfDue(Date.now()), null);
    });
  });

  describe('flushNow', () => {
    it('should return null when buffer is empty', () => {
      const agg = new TradeAggregator('test', 1000);
      assert.strictEqual(agg.flushNow(), null);
    });

    it('should return aggregate immediately even if window not elapsed', () => {
      const agg = new TradeAggregator('test', 1000);
      agg.addTrade({ market: 'test', price: 50000, qty: 1, side: 'buy', ts: 1000 });
      // still within window (now = 1100, windowMs = 1000)
      const result = agg.flushNow();
      assert.notStrictEqual(result, null);
      assert.strictEqual(result.ts, 1000);
      assert.strictEqual(result.open, 50000);
      assert.strictEqual(result.close, 50000);
      assert.strictEqual(result.volume, 1);
    });

    it('should clear buffer after flushNow', () => {
      const agg = new TradeAggregator('test', 1000);
      agg.addTrade({ market: 'test', price: 50000, qty: 0.5, side: 'sell', ts: 1000 });
      agg.flushNow();
      assert.strictEqual(agg.getPendingCount(), 0);
      assert.strictEqual(agg.flushNow(), null);
    });

    it('should correctly aggregate multiple trades in flushNow', () => {
      const agg = new TradeAggregator('test', 1000);
      agg.addTrade({ market: 'test', price: 100, qty: 1, side: 'buy', ts: 1000 });
      agg.addTrade({ market: 'test', price: 102, qty: 2, side: 'sell', ts: 1100 });
      const result = agg.flushNow();
      assert.strictEqual(result.open, 100);
      assert.strictEqual(result.high, 102);
      assert.strictEqual(result.low, 100);
      assert.strictEqual(result.close, 102);
      assert.strictEqual(result.volume, 3);
      assert.strictEqual(result.trade_count, 2);
    });
  });

  describe('classifyTradeNotional', () => {
    it('should return small for notional < $1k', () => {
      // $50 * 0.01 BTC = $0.5
      assert.strictEqual(classifyTradeNotional(50, 0.01), 'small');
      // $64000 * 0.01 BTC = $640
      assert.strictEqual(classifyTradeNotional(64000, 0.01), 'small');
      // boundary: $999.99
      assert.strictEqual(classifyTradeNotional(1, 999.99), 'small');
    });

    it('should return medium for notional >= $1k and < $10k', () => {
      // $64000 * 0.02 BTC = $1280
      assert.strictEqual(classifyTradeNotional(64000, 0.02), 'medium');
      // $64000 * 0.1 BTC = $6400
      assert.strictEqual(classifyTradeNotional(64000, 0.1), 'medium');
      // boundary: $1000
      assert.strictEqual(classifyTradeNotional(1000, 1), 'medium');
      // boundary: $9999.99
      assert.strictEqual(classifyTradeNotional(9999.99, 1), 'medium');
    });

    it('should return large for notional >= $10k', () => {
      // $64000 * 0.16 BTC = $10240
      assert.strictEqual(classifyTradeNotional(64000, 0.16), 'large');
      // $64000 * 1 BTC = $64000
      assert.strictEqual(classifyTradeNotional(64000, 1), 'large');
      // boundary: $10000
      assert.strictEqual(classifyTradeNotional(10000, 1), 'large');
    });
  });

  describe('size fields in aggregation output', () => {
    it('should report small/medium/large counts and volumes', () => {
      const agg = new TradeAggregator('test', 1000);
      // small: $500 * 1 = $500 (< $1k)
      agg.addTrade({ market: 'test', price: 500, qty: 1, side: 'buy', ts: 1000 });
      // medium: $500 * 3 = $1500 ($1k-$10k)
      agg.addTrade({ market: 'test', price: 500, qty: 3, side: 'sell', ts: 1100 });
      // large: $10000 * 2 = $20000 (>= $10k)
      agg.addTrade({ market: 'test', price: 10000, qty: 2, side: 'buy', ts: 1200 });
      // medium: $2000 * 1 = $2000
      agg.addTrade({ market: 'test', price: 2000, qty: 1, side: 'buy', ts: 1300 });

      const result = agg.flushIfDue(2000);
      assert.notStrictEqual(result, null);
      // counts
      assert.strictEqual(result.small_count, 1);
      assert.strictEqual(result.medium_count, 2);
      assert.strictEqual(result.large_count, 1);
      // volumes
      assert.strictEqual(result.small_volume, 1);
      assert.strictEqual(result.medium_volume, 4); // 3 + 1
      assert.strictEqual(result.large_volume, 2);
      // total volume sanity check
      assert.strictEqual(result.volume, 7);
    });

    it('should handle all-tiny window as all small', () => {
      const agg = new TradeAggregator('test', 1000);
      agg.addTrade({ market: 'test', price: 64000, qty: 0.005, side: 'buy', ts: 1000 });
      agg.addTrade({ market: 'test', price: 64000, qty: 0.003, side: 'sell', ts: 1100 });
      const result = agg.flushIfDue(2000);
      // 64000 * 0.005 = $320, 64000 * 0.003 = $192 → both small
      assert.strictEqual(result.small_count, 2);
      assert.strictEqual(result.medium_count, 0);
      assert.strictEqual(result.large_count, 0);
      assert.strictEqual(result.small_volume, 0.008);
    });
  });
});
