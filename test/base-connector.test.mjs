// test/base-connector.test.mjs — BaseConnector connect() settle-once unit tests
//
// Verifies that connect() resolves on open and rejects on error/close before open,
// preventing the startup hang bug where an unresolved promise blocks
// Promise.allSettled(startPromises).

import { describe, it, after } from 'node:test';
import assert from 'node:assert';
import { EventEmitter } from 'node:events';
import { BaseConnector } from '../lib/base-connector.mjs';

// ====== Mock WebSocket ======

/**
 * Minimal ws-compatible mock using EventEmitter.
 * Emit events manually to simulate open/close/error in tests.
 */
class MockWebSocket extends EventEmitter {
  constructor(url) {
    super();
    this.url = url;
    this.readyState = 0; // CONNECTING
  }

  close(_code, _reason) {
    this.readyState = 3; // CLOSED
    // In real ws, close does not emit the event itself — the remote side does.
  }
}

// ====== Helpers ======

/** Track created connectors for cleanup. */
const _cleanup = [];

/**
 * Create a BaseConnector suitable for connect() testing.
 * Stubs subscribe() so _onOpen() doesn't throw.
 */
function createTestConnector() {
  const conn = new BaseConnector(
    {},
    { market: 'test_market', wsUrl: 'ws://localhost:9999', restUrl: 'http://localhost:9999' },
  );
  conn._setWebSocket(MockWebSocket);
  conn.subscribe = () => {}; // no-op to avoid throw in _onOpen
  _cleanup.push(conn);
  return conn;
}

after(() => {
  // Clear all pending reconnect timers so the test runner does not hang.
  for (const conn of _cleanup) {
    conn._clearTimers();
  }
  _cleanup.length = 0;
});

// ====== Tests ======

describe('BaseConnector connect() settle-once', () => {
  it('should resolve on open and set state to connected', async () => {
    const conn = createTestConnector();

    const connectPromise = conn.connect();

    // Simulate open after a tick
    setImmediate(() => {
      conn._ws.readyState = 1; // OPEN
      conn._ws.emit('open');
    });

    await assert.doesNotReject(connectPromise);
    assert.strictEqual(conn.getState(), 'connected');
  });

  it('should reject on error before open, emit error, and schedule reconnect', async () => {
    const conn = createTestConnector();

    const errors = [];
    conn.on('error', (ev) => errors.push(ev));

    const connectPromise = conn.connect();

    setImmediate(() => {
      conn._ws.emit('error', new Error('connection refused'));
    });

    await assert.rejects(connectPromise, /error before open/);
    // Verify error event was emitted
    assert.ok(errors.length > 0);
    assert.ok(errors[0].message.includes('connection refused'));
    // State should be reconnecting (scheduled reconnect on pre-open error)
    assert.strictEqual(conn.getState(), 'reconnecting');
  });

  it('should reject on close before open and schedule reconnect', async () => {
    const conn = createTestConnector();

    const connectPromise = conn.connect();

    setImmediate(() => {
      conn._ws.emit('close');
    });

    await assert.rejects(connectPromise, /closed before open/);
    assert.strictEqual(conn.getState(), 'reconnecting');
  });

  it('should not reject if error fires after open has already resolved', async () => {
    const conn = createTestConnector();

    const connectPromise = conn.connect();

    // Open first
    setImmediate(() => {
      conn._ws.readyState = 1;
      conn._ws.emit('open');
    });

    await assert.doesNotReject(connectPromise);
    assert.strictEqual(conn.getState(), 'connected');

    // Now fire error after open — should NOT reject (promise already resolved)
    // and should follow runtime error path (emit error + reconnect)
    const errors = [];
    conn.on('error', (ev) => errors.push(ev));
    conn._ws.readyState = 3; // CLOSED
    conn._ws.emit('error', new Error('runtime error'));
    assert.strictEqual(conn.getState(), 'reconnecting');
    assert.ok(errors.length > 0);
    assert.ok(errors[0].message.includes('runtime error'));
  });

  it('should not reject if close fires after open has already resolved', async () => {
    const conn = createTestConnector();

    const connectPromise = conn.connect();

    setImmediate(() => {
      conn._ws.readyState = 1;
      conn._ws.emit('open');
    });

    await assert.doesNotReject(connectPromise);
    assert.strictEqual(conn.getState(), 'connected');

    // Fire close after open — should trigger reconnect but not reject
    conn._ws.emit('close');
    assert.strictEqual(conn.getState(), 'reconnecting');
  });

  it('should complete all tests within timeout (no hang)', async () => {
    // This test ensures that multiple connect() calls with various
    // failure modes all settle promptly without hanging.
    const conn1 = createTestConnector();
    const conn2 = createTestConnector();
    const conn3 = createTestConnector();

    // Suppress error events to avoid ERR_UNHANDLED_ERROR
    conn1.on('error', () => {});
    conn2.on('error', () => {});

    const p1 = conn1.connect();
    const p2 = conn2.connect();
    const p3 = conn3.connect();

    // Trigger different settle paths
    setImmediate(() => {
      conn1._ws.emit('error', new Error('err1'));
      conn2._ws.emit('close');
      conn3._ws.readyState = 1;
      conn3._ws.emit('open');
    });

    const results = await Promise.allSettled([p1, p2, p3]);

    assert.strictEqual(results[0].status, 'rejected');
    assert.strictEqual(results[1].status, 'rejected');
    assert.strictEqual(results[2].status, 'fulfilled');
    assert.ok(results[0].reason.message.includes('error before open'));
    assert.ok(results[1].reason.message.includes('closed before open'));
  });
});
