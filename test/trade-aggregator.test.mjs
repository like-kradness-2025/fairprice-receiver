// test/trade-aggregator.test.mjs — TradeAggregator unit tests

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { TradeAggregator } from '../lib/trade-aggregator.mjs';

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
});
