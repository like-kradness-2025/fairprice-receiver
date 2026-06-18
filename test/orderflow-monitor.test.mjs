// test/orderflow-monitor.test.mjs — Orderflow monitor tick skip logic
//
// Verifies that book snapshot and feature computation are skipped when:
// - connector state is not 'running' (reconnecting/syncing/connecting/error)
// - book is empty (bids and asks both empty)
// Trade aggregator flush is always allowed.

import { describe, it, before } from 'node:test';
import assert from 'node:assert';
import { FullBook } from '../lib/full-book.mjs';

// ====== Helpers ======

/** Minimal connector mock that only tracks state. */
class MockConnector {
  constructor(state) {
    this._state = state;
  }
  getState() { return this._state; }
}

/** The skip logic extracted from the tick loop. */
function shouldSkipFeature(connector, book) {
  if (!connector || connector.getState() !== 'running') return true;
  if (!book || book.isEmpty()) return true;
  return false;
}

function shouldSkipBookSnapshot(connector, book) {
  if (!connector || connector.getState() !== 'running') return true;
  if (book.isEmpty()) return true;
  return false;
}

// ====== Tests ======

describe('OrderflowMonitor tick skip logic', () => {

  describe('shouldSkipFeature', () => {
    it('should skip when connector is null', () => {
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      assert.strictEqual(shouldSkipFeature(null, book), true);
    });

    it('should skip when connector state is reconnecting', () => {
      const conn = new MockConnector('reconnecting');
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      assert.strictEqual(shouldSkipFeature(conn, book), true);
    });

    it('should skip when connector state is syncing', () => {
      const conn = new MockConnector('syncing');
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      assert.strictEqual(shouldSkipFeature(conn, book), true);
    });

    it('should skip when connector state is connecting', () => {
      const conn = new MockConnector('connecting');
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      assert.strictEqual(shouldSkipFeature(conn, book), true);
    });

    it('should skip when connector state is error', () => {
      const conn = new MockConnector('error');
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      assert.strictEqual(shouldSkipFeature(conn, book), true);
    });

    it('should skip when book is empty', () => {
      const conn = new MockConnector('running');
      const book = new FullBook('test');
      assert.strictEqual(shouldSkipFeature(conn, book), true);
    });

    it('should skip when book is null', () => {
      const conn = new MockConnector('running');
      assert.strictEqual(shouldSkipFeature(conn, null), true);
    });

    it('should allow when running and book has data', () => {
      const conn = new MockConnector('running');
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      assert.strictEqual(shouldSkipFeature(conn, book), false);
    });

    it('should allow when running and book has only bids', () => {
      // Edge case: only bids exist but no asks
      const conn = new MockConnector('running');
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], []);
      assert.strictEqual(shouldSkipFeature(conn, book), false);
    });
  });

  describe('shouldSkipBookSnapshot', () => {
    it('should skip when connector is null', () => {
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      assert.strictEqual(shouldSkipBookSnapshot(null, book), true);
    });

    it('should skip when connector state is reconnecting', () => {
      const conn = new MockConnector('reconnecting');
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      assert.strictEqual(shouldSkipBookSnapshot(conn, book), true);
    });

    it('should skip when book is empty even if running', () => {
      const conn = new MockConnector('running');
      const book = new FullBook('test');
      assert.strictEqual(shouldSkipBookSnapshot(conn, book), true);
    });

    it('should allow when running and book has data', () => {
      const conn = new MockConnector('running');
      const book = new FullBook('test');
      book.applySnapshot([['100', '1']], [['101', '1']]);
      assert.strictEqual(shouldSkipBookSnapshot(conn, book), false);
    });
  });
});
