// test/derivatives-helper.test.mjs — DerivativesHelper unit tests
// Tests data row format, writer integration, and market registration.
// Network-dependent _fetch* methods are not tested here (requires live API).

import { describe, it } from 'node:test';
import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { DerivativesHelper } from '../lib/derivatives-helper.mjs';

function uniqueTmpDir() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'btc-rec-derivatives-'));
  // Pre-create the derivatives subdir so BufferedWriter stream won't fail
  fs.mkdirSync(path.join(dir, 'derivatives'), { recursive: true });
  return dir;
}

describe('DerivativesHelper row format', () => {
  it('should register market and create writer', async () => {
    const tmpDir = uniqueTmpDir();
    const helper = new DerivativesHelper(tmpDir, { intervalMs: 10000 });
    helper.registerMarket('binance_perp', {});
    assert.ok(helper._writers.has('binance_perp'));
    assert.ok(helper._writers.get('binance_perp').writer);
    await helper.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should write derivative row and flush to JSONL file', async () => {
    const tmpDir = uniqueTmpDir();
    const helper = new DerivativesHelper(tmpDir, { intervalMs: 10000 });
    helper.registerMarket('binance_perp', {});
    const entry = helper._writers.get('binance_perp');

    const row = {
      ts: Date.now(),
      market: 'binance_perp',
      mark_price: 65000.5,
      funding_rate: 0.0001,
      open_interest: 12345.67,
      next_funding_time: Date.now() + 28800000,
    };

    await entry.writer.write(row);
    await entry.writer.flush();

    const filePath = path.join(tmpDir, 'derivatives', 'binance_perp.jsonl');
    assert.ok(fs.existsSync(filePath));

    const content = fs.readFileSync(filePath, 'utf-8').trim();
    const parsed = JSON.parse(content);
    assert.strictEqual(parsed.market, 'binance_perp');
    assert.strictEqual(parsed.mark_price, 65000.5);
    assert.strictEqual(parsed.funding_rate, 0.0001);
    assert.strictEqual(parsed.open_interest, 12345.67);
    assert.ok(parsed.ts);
    assert.ok(parsed.next_funding_time);

    await helper.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should write row with null fields for missing data', async () => {
    const tmpDir = uniqueTmpDir();
    const helper = new DerivativesHelper(tmpDir, { intervalMs: 10000 });
    helper.registerMarket('okx_perp', {});

    const row = {
      ts: Date.now(),
      market: 'okx_perp',
      mark_price: null,
      funding_rate: -0.00005,
      open_interest: null,
      next_funding_time: null,
    };

    const entry = helper._writers.get('okx_perp');
    await entry.writer.write(row);
    await entry.writer.flush();

    const filePath = path.join(tmpDir, 'derivatives', 'okx_perp.jsonl');
    assert.ok(fs.existsSync(filePath));

    const content = fs.readFileSync(filePath, 'utf-8').trim();
    const parsed = JSON.parse(content);
    assert.strictEqual(parsed.market, 'okx_perp');
    assert.strictEqual(parsed.mark_price, null);
    assert.strictEqual(parsed.funding_rate, -0.00005);
    assert.strictEqual(parsed.open_interest, null);
    assert.strictEqual(parsed.next_funding_time, null);

    await helper.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should support multiple markets with separate writers', async () => {
    const tmpDir = uniqueTmpDir();
    const helper = new DerivativesHelper(tmpDir, { intervalMs: 10000 });
    helper.registerMarket('bybit_perp', {});
    helper.registerMarket('hyperliquid_perp', {});

    assert.ok(helper._writers.has('bybit_perp'));
    assert.ok(helper._writers.has('hyperliquid_perp'));

    const bybitEntry = helper._writers.get('bybit_perp');
    const hyperEntry = helper._writers.get('hyperliquid_perp');
    assert.notStrictEqual(bybitEntry.writer, hyperEntry.writer);

    await helper.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should handle multiple writes and read them back', async () => {
    const tmpDir = uniqueTmpDir();
    const helper = new DerivativesHelper(tmpDir, { intervalMs: 10000 });
    helper.registerMarket('binance_perp', {});

    const entry = helper._writers.get('binance_perp');
    await entry.writer.write({ ts: 1, market: 'binance_perp', mark_price: 100, funding_rate: 0, open_interest: null, next_funding_time: null });
    await entry.writer.write({ ts: 2, market: 'binance_perp', mark_price: 200, funding_rate: 0.001, open_interest: 5000, next_funding_time: null });
    await entry.writer.flush();

    const filePath = path.join(tmpDir, 'derivatives', 'binance_perp.jsonl');
    assert.ok(fs.existsSync(filePath));

    const content = fs.readFileSync(filePath, 'utf-8').trim();
    const lines = content.split('\n');
    assert.strictEqual(lines.length, 2);
    const first = JSON.parse(lines[0]);
    const second = JSON.parse(lines[1]);
    assert.strictEqual(first.mark_price, 100);
    assert.strictEqual(second.mark_price, 200);
    assert.strictEqual(second.funding_rate, 0.001);

    await helper.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

describe('DerivativesHelper Hyperliquid parse (network-less mock)', () => {
  it('should parse metaAndAssetCtxs response from mock fetch', async () => {
    const mockData = [
      [{ name: 'BTC', tokens: [] }],
      [{
        markPx: '67890.5',
        funding: '0.000095',
        openInterest: '12345.678',
      }],
    ];

    const origFetch = global.fetch;
    try {
      global.fetch = async (url, opts) => {
        assert.ok(url === 'https://api.hyperliquid.xyz/info');
        assert.ok(opts?.body?.includes('metaAndAssetCtxs'));
        return {
          ok: true,
          json: async () => mockData,
        };
      };

      const helper = new DerivativesHelper('/tmp/non-existent', { intervalMs: 60000 });
      const row = await helper._fetchHyperliquid(Date.now());
      assert.ok(row);
      assert.strictEqual(row.mark_price, 67890.5);
      assert.strictEqual(row.funding_rate, 0.000095);
      assert.strictEqual(row.open_interest, 12345.678);
      assert.strictEqual(row.next_funding_time, null);
    } finally {
      global.fetch = origFetch;
    }
  });
});

describe('DerivativesHelper OKX parse (network-less mock)', () => {
  it('should parse OKX _fetchOkx response with correct OI endpoint', async () => {
    const origFetch = global.fetch;
    try {
      let callCount = 0;
      global.fetch = async (url) => {
        const urlStr = typeof url === 'string' ? url : url.toString();
        if (urlStr.includes('funding-rate')) {
          return {
            ok: true,
            json: async () => ({
              data: [{ fundingRate: '0.0001', fundingTime: '1700000000000' }],
            }),
          };
        }
        if (urlStr.includes('open-interest')) {
          callCount++;
          // Verify correct endpoint
          assert.ok(urlStr.includes('/api/v5/public/open-interest'));
          assert.ok(urlStr.includes('instType=SWAP'));
          assert.ok(urlStr.includes('instId=BTC-USDT-SWAP'));
          // Old endpoint should NOT be used
          assert.ok(!urlStr.includes('/api/v5/market/open-interest'));
          return {
            ok: true,
            json: async () => ({
              data: [{ oi: '98765.432' }],
            }),
          };
        }
        if (urlStr.includes('mark-price')) {
          return {
            ok: true,
            json: async () => ({
              data: [{ markPx: '65000.5' }],
            }),
          };
        }
        return { ok: false };
      };

      const helper = new DerivativesHelper('/tmp/non-existent', { intervalMs: 60000 });
      const row = await helper._fetchOkx(Date.now());
      assert.ok(row);
      assert.strictEqual(row.open_interest, 98765.432);
      assert.strictEqual(row.funding_rate, 0.0001);
      assert.strictEqual(row.mark_price, 65000.5);
      assert.strictEqual(callCount, 1, 'should call open-interest endpoint exactly once');
    } finally {
      global.fetch = origFetch;
    }
  });
});
