// test/gemini-connector.test.mjs — Gemini BTC/USD connector parser tests

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { GeminiConnector } from '../lib/gemini-connector.mjs';

describe('GeminiConnector parser', () => {
  function createConn() {
    const conn = new GeminiConnector({});
    conn._ws = { send: () => {} };
    conn._connectDepthWs = () => {}; // suppress real WebSocket creation
    conn._setState('running');
    return conn;
  }

  it('should send the trades subscribe frame', () => {
    const conn = new GeminiConnector({});
    let sent = null;
    conn._ws = { send: (msg) => { sent = msg; } };
    conn._connectDepthWs = () => {}; // suppress real WebSocket creation
    conn.subscribe();
    assert.ok(sent);
    const parsed = JSON.parse(sent);
    assert.strictEqual(parsed.type, 'subscribe');
    assert.strictEqual(parsed.subscriptions[0].name, 'l2');
    assert.deepStrictEqual(parsed.subscriptions[0].symbols, ['btcusd']);
  });

  it('should parse trade payloads and convert maker flag to taker side', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => { emitted.push(ev); });

    conn._onMessage({
      type: 'update',
      timestamp: '1700000000.123456',
      events: [
        {
          type: 'trade',
          trades: [
            { p: '65001.5', q: '0.12', m: false, tid: 777, E: 1700000000123 },
            { p: '65002.5', q: '0.34', m: true, tid: 778, E: 1700000000456 },
          ],
        },
      ],
    });

    assert.strictEqual(emitted.length, 2);
    assert.strictEqual(emitted[0].market, 'gemini_spot');
    assert.strictEqual(emitted[0].price, 65001.5);
    assert.strictEqual(emitted[0].qty, 0.12);
    assert.strictEqual(emitted[0].side, 'buy');
    assert.strictEqual(emitted[0].ts, 1700000000123);
    assert.strictEqual(emitted[0].tradeId, '777');
    assert.strictEqual(emitted[1].side, 'sell');
    assert.strictEqual(emitted[1].tradeId, '778');
  });

  it('should ignore heartbeat messages', () => {
    const conn = createConn();
    let emitted = false;
    conn.on('trade', () => { emitted = true; });
    conn._onMessage({ type: 'heartbeat' });
    assert.strictEqual(emitted, false);
  });
});
