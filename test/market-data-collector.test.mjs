// test/market-data-collector.test.mjs — MarketDataCollector unit tests
// Verifies parse shapes and request contracts for live public endpoints.

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { MarketDataCollector } from '../lib/market-data-collector.mjs';

function withMockFetch(handler, fn) {
  const origFetch = global.fetch;
  global.fetch = handler;
  return Promise.resolve()
    .then(fn)
    .finally(() => {
      global.fetch = origFetch;
    });
}

describe('MarketDataCollector parsers', () => {
  // ====================================================================
  // LS Ratio parsers
  // ====================================================================

  it('parses Bybit long-short ratio buy/sell fields', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({
        retCode: 0,
        result: {
          list: [{
            buyRatio: '0.6308',
            sellRatio: '0.3692',
            timestamp: '1781092800000',
          }],
        },
      }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchBybitLSRatio('https://api.bybit.com/v5/market/account-ratio?category=linear&symbol=BTCUSDT&period=1h');
      assert.ok(row);
      assert.strictEqual(row.long_account_ratio, 0.6308);
      assert.strictEqual(row.short_account_ratio, 0.3692);
      assert.strictEqual(row.long_short_ratio, 0.6308 / 0.3692);
      assert.strictEqual(row.ts_str, '1781092800000');
    });
  });

  it('parses OKX rubik long-short ratio rows', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({
        code: '0',
        data: [
          ['1781092800000', '1.95'],
          ['1781089200000', '1.96'],
        ],
      }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchOkxLSRatio('https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=BTC&period=1H');
      assert.ok(row);
      assert.strictEqual(row.long_short_ratio, 1.95);
      assert.strictEqual(row.ts_str, '1781092800000');
      assert.strictEqual(row.period, '1H');
      assert.strictEqual(row.long_account_ratio, null);
      assert.strictEqual(row.short_account_ratio, null);
    });
  });

  it('parses Binance long-short ratio fields', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ([{
        longAccount: '0.5230',
        shortAccount: '0.4770',
        longShortRatio: '1.0964',
        period: '1h',
      }]),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchBinanceLSRatio('https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=1h&limit=1');
      assert.ok(row);
      assert.strictEqual(row.long_account_ratio, 0.523);
      assert.strictEqual(row.short_account_ratio, 0.477);
      assert.strictEqual(row.long_short_ratio, 1.0964);
      assert.strictEqual(row.period, '1h');
    });
  });

  // ====================================================================
  // Ticker parsers
  // ====================================================================

  it('adds a User-Agent for Coinbase requests', async () => {
    let seenHeaders = null;
    await withMockFetch(async (_url, opts) => {
      seenHeaders = opts?.headers;
      return {
        ok: true,
        json: async () => ({
          price: '61590.88',
          volume: '11280.94122962',
          high: '61650.00',
          low: '61400.00',
          open: '61500.00',
        }),
      };
    }, async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchCoinbaseTicker('https://api.exchange.coinbase.com/products/BTC-USD/ticker', 'coinbase_spot');
      assert.ok(row);
      assert.strictEqual(row.last_price, 61590.88);
      assert.strictEqual(row.volume_24h, 11280.94122962);
      assert.strictEqual(row.high_24h, 61650);
      assert.strictEqual(row.low_24h, 61400);
      assert.strictEqual(row.open_24h, 61500);
    });
    assert.ok(seenHeaders);
    assert.strictEqual(seenHeaders['User-Agent'], 'btc-receiver/v3.00');
  });

  it('parses Binance ticker (spot) fields', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({
        lastPrice: '61590.88',
        volume: '11280.941',
        quoteVolume: '694567890.12',
        highPrice: '62706.10',
        lowPrice: '60614.50',
        priceChange: '150.00',
        priceChangePercent: '0.24',
      }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchBinanceTicker('https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT', 'binance_spot');
      assert.ok(row);
      assert.strictEqual(row.last_price, 61590.88);
      assert.strictEqual(row.volume_24h, 11280.941);
      assert.strictEqual(row.quote_vol_24h, 694567890.12);
      assert.strictEqual(row.high_24h, 62706.1);
      assert.strictEqual(row.low_24h, 60614.5);
      assert.strictEqual(row.price_change_24h, 150);
      assert.strictEqual(row.price_change_pct, 0.24);
    });
  });

  it('parses Binance ticker (USDC) fields', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({
        lastPrice: '61591.11',
        volume: '8123.456',
        quoteVolume: '500123456.78',
        highPrice: '62000.00',
        lowPrice: '61000.00',
        priceChange: '111.11',
        priceChangePercent: '0.18',
      }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchBinanceTicker('https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDC', 'binance_spot_usdc');
      assert.ok(row);
      assert.strictEqual(row.last_price, 61591.11);
      assert.strictEqual(row.volume_24h, 8123.456);
      assert.strictEqual(row.quote_vol_24h, 500123456.78);
      assert.strictEqual(row.high_24h, 62000);
      assert.strictEqual(row.low_24h, 61000);
      assert.strictEqual(row.price_change_24h, 111.11);
      assert.strictEqual(row.price_change_pct, 0.18);
    });
  });

  it('parses Binance perp ticker (same shape as spot)', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({
        lastPrice: '61624.90',
        volume: '103848.267',
        quoteVolume: '6389051122.2679',
        highPrice: '62706.10',
        lowPrice: '60614.50',
        priceChange: '75.60',
        priceChangePercent: '0.12',
      }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchBinanceTicker('https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=BTCUSDT', 'binance_perp');
      assert.ok(row);
      assert.strictEqual(row.last_price, 61624.9);
      assert.strictEqual(row.volume_24h, 103848.267);
    });
  });

  it('parses Bybit ticker fields', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({
        retCode: 0,
        result: {
          list: [{
            symbol: 'BTCUSDT',
            lastPrice: '61624.90',
            volume24h: '103848.2670',
            turnover24h: '6389051122.2679',
            highPrice24h: '62706.10',
            lowPrice24h: '60614.50',
            prevPrice24h: '62549.30',
            markPrice: '61625.85',
            fundingRate: '-0.00002157',
            openInterest: '55640.429',
            nextFundingTime: '1781107200000',
          }],
        },
      }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchBybitTicker('https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT', 'bybit_perp');
      assert.ok(row);
      assert.strictEqual(row.last_price, 61624.9);
      assert.strictEqual(row.volume_24h, 103848.267);
      assert.strictEqual(row.quote_vol_24h, 6389051122.2679);
      assert.strictEqual(row.high_24h, 62706.1);
      assert.strictEqual(row.low_24h, 60614.5);
      assert.strictEqual(row.open_24h, 62549.3);
      assert.strictEqual(row.mark_price, 61625.85);
    });
  });

  it('parses OKX ticker fields', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({
        code: '0',
        data: [{
          instType: 'SWAP',
          instId: 'BTC-USDT-SWAP',
          last: '61624.9',
          vol24h: '103848',
          volCcy24h: '6389051122',
          high24h: '62706.1',
          low24h: '60614.5',
          open24h: '62549.3',
          ts: '1781092800000',
        }],
      }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchOkxTicker('https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT-SWAP', 'okx_perp');
      assert.ok(row);
      assert.strictEqual(row.last_price, 61624.9);
      assert.strictEqual(row.volume_24h, 103848);
      assert.strictEqual(row.quote_vol_24h, 6389051122);
      assert.strictEqual(row.high_24h, 62706.1);
      assert.strictEqual(row.low_24h, 60614.5);
      assert.strictEqual(row.open_24h, 62549.3);
      assert.strictEqual(row.ts, 1781092800000);
    });
  });

  it('parses Kraken ticker fields', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({
        error: [],
        result: {
          XXBTZUSD: {
            c: ['61624.90', '0.10'],
            v: ['103848.267', '120000.000'],
            h: ['62706.10', '63000.00'],
            l: ['60614.50', '60000.00'],
          },
        },
      }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchKrakenTicker('https://api.kraken.com/0/public/Ticker?pair=XBTUSD', 'kraken_spot');
      assert.ok(row);
      assert.strictEqual(row.last_price, 61624.9);
      assert.strictEqual(row.volume_24h, 103848.267);
      assert.strictEqual(row.high_24h, 62706.1);
      assert.strictEqual(row.low_24h, 60614.5);
    });
  });

  it('parses Kraken OHLC fields', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({
        error: [],
        result: {
          XBTUSD: [
            [1700000000, '61000.0', '62000.0', '60500.0', '61500.0', '61600.0', '100.0', 42],
          ],
        },
      }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchKrakenCandles('https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=1');
      assert.ok(row);
      assert.strictEqual(row.close, 61500.0);
      assert.strictEqual(row.volume, 100.0);
    });
  });

  it('_fetchTicker stores in _lastTickers for premium computation', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({
        lastPrice: '61590.88',
        volume: '11280.941',
        quoteVolume: '694567890.12',
        highPrice: '62706.10',
        lowPrice: '60614.50',
        priceChange: '150.00',
        priceChangePercent: '0.24',
      }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      await collector._fetchBinanceTicker('https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT', 'binance_spot');
      assert.ok(collector._lastTickers.binance_spot);
      assert.strictEqual(collector._lastTickers.binance_spot.last_price, 61590.88);
    });
  });

  it('registers kraken_spot ticker and ohlcv fetchers', () => {
    const collector = new MarketDataCollector('/tmp/unused');
    collector.registerMarket('kraken_spot', {
      type: 'spot',
      urls: {
        ohlcv: 'https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=1',
        ticker: 'https://api.kraken.com/0/public/Ticker?pair=XBTUSD',
      },
    });

    assert.strictEqual(collector._fetchers.length, 2);
    assert.deepStrictEqual(collector._fetchers.map((f) => `${f.type}:${f.market}`).sort(), [
      'ohlcv:kraken_spot',
      'ticker:kraken_spot',
    ]);
  });

  // ====================================================================
  // OHLCV parsers
  // ====================================================================

  it('parses Binance klines', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ([
        [1781092800000, '61590.88', '61650.00', '61500.00', '61600.00', '112.5', 1781092859999, '6930000.0', 56, '56.2', '3465000.0', '0'],
      ]),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchBinanceKlines('https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1');
      assert.ok(row);
      assert.strictEqual(row.open_time, 1781092800000);
      assert.strictEqual(row.open, 61590.88);
      assert.strictEqual(row.high, 61650);
      assert.strictEqual(row.low, 61500);
      assert.strictEqual(row.close, 61600);
      assert.strictEqual(row.volume, 112.5);
      assert.strictEqual(row.quote_vol, 6930000);
      assert.strictEqual(row.taker_buy_vol, 56.2);
      assert.strictEqual(row.taker_buy_quote, 3465000);
    });
  });

  it('parses Binance USDC klines', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ([
        [1781092800000, '61591.11', '62000.00', '61000.00', '61610.00', '81.5', 1781092859999, '5020000.0', 42, '40.1', '2470000.0', '0'],
      ]),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchBinanceKlines('https://api.binance.com/api/v3/klines?symbol=BTCUSDC&interval=1m&limit=1');
      assert.ok(row);
      assert.strictEqual(row.open, 61591.11);
      assert.strictEqual(row.high, 62000);
      assert.strictEqual(row.low, 61000);
      assert.strictEqual(row.close, 61610);
      assert.strictEqual(row.volume, 81.5);
      assert.strictEqual(row.quote_vol, 5020000);
    });
  });

  it('parses Bybit klines', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({
        retCode: 0,
        result: {
          list: [
            ['1781092800000', '61590.88', '61650.00', '61500.00', '61600.00', '112.5', '6930000.0'],
          ],
        },
      }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchBybitKlines('https://api.bybit.com/v5/market/kline?category=linear&symbol=BTCUSDT&interval=1&limit=1');
      assert.ok(row);
      assert.strictEqual(row.open_time, 1781092800000);
      assert.strictEqual(row.open, 61590.88);
      assert.strictEqual(row.high, 61650);
      assert.strictEqual(row.low, 61500);
      assert.strictEqual(row.close, 61600);
      assert.strictEqual(row.volume, 112.5);
      assert.strictEqual(row.turnover, 6930000);
    });
  });

  it('parses OKX candles', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({
        code: '0',
        data: [
          ['1781092800000', '61590.88', '61650.00', '61500.00', '61600.00', '112.5', '6930000.0', '0.0018', '1'],
        ],
      }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchOkxCandles('https://www.okx.com/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=1m&limit=1');
      assert.ok(row);
      assert.strictEqual(row.open_time, 1781092800000);
      assert.strictEqual(row.open, 61590.88);
      assert.strictEqual(row.high, 61650);
      assert.strictEqual(row.low, 61500);
      assert.strictEqual(row.close, 61600);
      assert.strictEqual(row.vol, 112.5);
      assert.strictEqual(row.vol_currency, 6930000);
      assert.strictEqual(row.confirm, '1');
    });
  });

  it('parses Coinbase candles', async () => {
    let seenHeaders = null;
    await withMockFetch(async (_url, opts) => {
      seenHeaders = opts?.headers;
      return {
        ok: true,
        json: async () => ([
          [1781092800, 61590.88, 61650.00, 61500.00, 61600.00, 112.5],
        ]),
      };
    }, async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchCoinbaseCandles('https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=60');
      assert.ok(row);
      // Coinbase: [time(sec), low, high, open, close, volume]
      assert.strictEqual(row.open_time, 1781092800000);
      assert.strictEqual(row.low, 61590.88);
      assert.strictEqual(row.high, 61650);
      assert.strictEqual(row.open, 61500);
      assert.strictEqual(row.close, 61600);
      assert.strictEqual(row.volume, 112.5);
    });
    assert.ok(seenHeaders);
    assert.strictEqual(seenHeaders['User-Agent'], 'btc-receiver/v3.00');
  });

  // ====================================================================
  // Hyperliquid
  // ====================================================================

  it('sends Hyperliquid candleSnapshot with a time window and parses the latest candle', async () => {
    let body = null;
    await withMockFetch(async (_url, opts) => {
      body = JSON.parse(opts.body);
      return {
        ok: true,
        json: async () => ([
          { t: 1781096100000, T: 1781096159999, s: 'BTC', i: '1m', o: '61634.0', c: '61614.0', h: '61640.0', l: '61612.0', v: '10.03544', n: 299 },
          { t: 1781096160000, T: 1781096219999, s: 'BTC', i: '1m', o: '61614.0', c: '61649.0', h: '61650.0', l: '61614.0', v: '14.32389', n: 286 },
        ]),
      };
    }, async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchHyperliquidCandles('https://api.hyperliquid.xyz/info');
      assert.ok(row);
      assert.strictEqual(row.open_time, 1781096160000);
      assert.strictEqual(row.close, 61649);
      assert.strictEqual(row.high, 61650);
      assert.strictEqual(row.low, 61614);
      assert.strictEqual(row.volume, 14.32389);
    });
    assert.ok(body);
    assert.strictEqual(body.type, 'candleSnapshot');
    assert.strictEqual(body.req.coin, 'BTC');
    assert.strictEqual(body.req.interval, '1m');
    assert.ok(Number.isInteger(body.req.startTime));
    assert.ok(Number.isInteger(body.req.endTime));
    assert.ok(body.req.endTime >= body.req.startTime);
  });

  // ====================================================================
  // Taker Volume
  // ====================================================================

  it('parses Binance taker volume', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ([{
        buyVol: '12345.67',
        sellVol: '8910.11',
        buySellRatio: '1.385',
        period: '1h',
      }]),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchBinanceTakerVol('https://fapi.binance.com/futures/data/takerlongshortRatio?symbol=BTCUSDT&period=1h&limit=1');
      assert.ok(row);
      assert.strictEqual(row.buy_vol_24h, 12345.67);
      assert.strictEqual(row.sell_vol_24h, 8910.11);
      assert.strictEqual(row.buy_sell_ratio, 1.385);
    });
  });

  // ====================================================================
  // Generic Ticker (Hyperliquid pass-through)
  // ====================================================================

  it('_fetchGenericTicker returns raw data', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({ some: 'data', value: 42 }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchGenericTicker('https://api.hyperliquid.xyz/info');
      assert.ok(row);
      assert.deepStrictEqual(row.raw, { some: 'data', value: 42 });
    });
  });

  // ====================================================================
  // Error/edge-case handling
  // ====================================================================

  it('_fetchJSON returns null on HTTP error', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: false,
      status: 429,
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const result = await collector._fetchJSON('https://api.example.com/ticker');
      assert.strictEqual(result, null);
    });
  });

  it('returns null when exchange returns empty ticker data', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({ retCode: 0, result: { list: [] } }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchBybitTicker('https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT', 'bybit_perp');
      assert.strictEqual(row, null);
    });
  });

  it('returns null when OKX ticker response lacks data array', async () => {
    await withMockFetch(async (_url, _opts) => ({
      ok: true,
      json: async () => ({ code: '0', data: [] }),
    }), async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchOkxTicker('https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT-SWAP', 'okx_perp');
      assert.strictEqual(row, null);
    });
  });

  it('returns null on fetch exception', async () => {
    await withMockFetch(async (_url, _opts) => {
      throw new Error('Network timeout');
    }, async () => {
      const collector = new MarketDataCollector('/tmp/unused');
      const row = await collector._fetchBinanceTicker('https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT', 'binance_spot');
      assert.strictEqual(row, null);
    });
  });
});
