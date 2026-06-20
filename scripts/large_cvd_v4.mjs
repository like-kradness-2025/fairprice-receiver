#!/usr/bin/env node
// large_cvd_v4.mjs — 1s_agg → Large notional CVD per exchange, 5min buckets, perp/spot grouped
import duckdb from 'duckdb';
import fs from 'node:fs';

const exchangeMap = {
  'binance_coinm_perp':   { ex: 'Binance', type: 'perp' },
  'binance_perp':         { ex: 'Binance', type: 'perp' },
  'binance_perp_btcusdc': { ex: 'Binance', type: 'perp' },
  'binance_spot':         { ex: 'Binance', type: 'spot' },
  'binance_spot_usdc':    { ex: 'Binance', type: 'spot' },
  'bitfinex_spot':        { ex: 'Bitfinex', type: 'spot' },
  'bitmex_perp':          { ex: 'BitMEX', type: 'perp' },
  'bitstamp_spot':        { ex: 'Bitstamp', type: 'spot' },
  'bybit_perp':           { ex: 'Bybit', type: 'perp' },
  'bybit_spot':           { ex: 'Bybit', type: 'spot' },
  'coinbase_spot':        { ex: 'Coinbase', type: 'spot' },
  'crypto_com_spot':      { ex: 'Crypto.com', type: 'spot' },
  'gemini_spot':          { ex: 'Gemini', type: 'spot' },
  'hyperliquid_perp':     { ex: 'Hyperliquid', type: 'perp' },
  'kraken_spot':          { ex: 'Kraken', type: 'spot' },
  'okx_perp':             { ex: 'OKX', type: 'perp' },
  'okx_spot':             { ex: 'OKX', type: 'spot' },
};

const db = new duckdb.Database('data/parquet/1s_agg.db');

// 1s_agg → notional estimate per second, then 5min buckets, then cumulative per exchange
const sql = `
WITH notional_1s AS (
  SELECT market, ts,
    buy_large_qty * close AS buy_large_notional,
    sell_large_qty * close AS sell_large_notional,
    (buy_large_qty - sell_large_qty) * close AS large_delta_notional
  FROM agg_1s
),
bucket5m AS (
  SELECT market, CAST(ts / 300000 * 300000 AS BIGINT) AS bucket,
    SUM(buy_large_notional) AS buy_notional,
    SUM(sell_large_notional) AS sell_notional,
    SUM(large_delta_notional) AS delta,
    0 AS large_qty_total
  FROM notional_1s
  GROUP BY market, bucket
)
SELECT market, bucket, buy_notional, sell_notional, delta
FROM bucket5m
ORDER BY bucket, market
`;

db.all(sql, (err, rows) => {
  if (err) { console.error(err); process.exit(1); }
  console.log('Rows:', rows.length.toLocaleString());
  
  // Exchange grouping
  const exBuckets = {}; // key: "exchange|type|bucket" → {buy, sell}
  for (const r of rows) {
    const info = exchangeMap[r.market];
    if (!info) continue;
    const key = `${info.ex}|${info.type}|${Number(r.bucket)}`;
    if (!exBuckets[key]) exBuckets[key] = { ex: info.ex, type: info.type, bucket: Number(r.bucket), buy: 0, sell: 0 };
    exBuckets[key].buy += Number(r.buy_notional);
    exBuckets[key].sell += Number(r.sell_notional);
  }

  // Write CSV
  const csvLines = ['exchange,type,bucket,buy_notional,sell_notional,delta'];
  for (const v of Object.values(exBuckets)) {
    csvLines.push(`${v.ex},${v.type},${v.bucket},${v.buy.toFixed(2)},${v.sell.toFixed(2)},${(v.buy-v.sell).toFixed(2)}`);
  }
  fs.writeFileSync('/tmp/large_cvd_v4.csv', csvLines.join('\n'));
  console.log('CSV:', (Object.keys(exBuckets).length).toLocaleString(), 'rows');

  // Stats
  const exStats = {};
  for (const v of Object.values(exBuckets)) {
    if (!exStats[v.ex]) exStats[v.ex] = { buy: 0, sell: 0 };
    exStats[v.ex].buy += v.buy;
    exStats[v.ex].sell += v.sell;
  }
  console.log('\n=== Per Exchange (Large ~$10k+ notional, from 1s_agg size classification) ===');
  console.log('Exchange'.padEnd(14), 'Buy($)'.padStart(14), 'Sell($)'.padStart(14), 'Net($)'.padStart(14));
  console.log('-'.repeat(56));
  const sorted = Object.entries(exStats).sort((a,b) => Math.abs(b[1].buy-b[1].sell) - Math.abs(a[1].buy-a[1].sell));
  for (const [ex, t] of sorted) {
    console.log(ex.padEnd(14), t.buy.toFixed(0).padStart(14), t.sell.toFixed(0).padStart(14), (t.buy-t.sell).toFixed(0).padStart(14));
  }

  // Perp vs Spot
  let pBuy = 0, pSell = 0, sBuy = 0, sSell = 0;
  for (const v of Object.values(exBuckets)) {
    if (v.type === 'perp') { pBuy += v.buy; pSell += v.sell; }
    else { sBuy += v.buy; sSell += v.sell; }
  }
  console.log(`\n=== Perp vs Spot ===`);
  console.log(`Perp: Buy=${(pBuy/1e6).toFixed(1)}M Sell=${(pSell/1e6).toFixed(1)}M Net=${((pBuy-pSell)/1e6).toFixed(1)}M`);
  console.log(`Spot: Buy=${(sBuy/1e6).toFixed(1)}M Sell=${(sSell/1e6).toFixed(1)}M Net=${((sBuy-sSell)/1e6).toFixed(1)}M`);

  db.close();
});
