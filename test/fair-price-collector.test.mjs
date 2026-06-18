// test/fair-price-collector.test.mjs — Fair price selection unit tests

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { selectFairPrice } from '../lib/fair-price-collector.mjs';

describe('selectFairPrice', () => {
  it('prefers mark price over book mid and last price', () => {
    const picked = selectFairPrice({ markPrice: 65000, bookMid: 64000, lastPrice: 63000 });
    assert.ok(picked);
    assert.strictEqual(picked.fairPrice, 65000);
    assert.strictEqual(picked.source, 'mark_price');
  });

  it('falls back to book mid when mark price is missing', () => {
    const picked = selectFairPrice({ markPrice: null, bookMid: 64000, lastPrice: 63000 });
    assert.ok(picked);
    assert.strictEqual(picked.fairPrice, 64000);
    assert.strictEqual(picked.source, 'book_mid');
  });

  it('falls back to last price when mark price and book mid are missing', () => {
    const picked = selectFairPrice({ markPrice: null, bookMid: null, lastPrice: 63000 });
    assert.ok(picked);
    assert.strictEqual(picked.fairPrice, 63000);
    assert.strictEqual(picked.source, 'last_price');
  });

  it('returns null when no source exists', () => {
    assert.strictEqual(selectFairPrice({ markPrice: null, bookMid: null, lastPrice: null }), null);
  });
});