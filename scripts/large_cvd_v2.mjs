#!/usr/bin/env node
// large_cvd_v2.mjs — Large CVD chart data (>= $10k notional, 5min buckets, perp/spot grouped)
import duckdb from 'duckdb';
import fs from 'node:fs';

const TRADE_DIR = 'data/parquet/trades';

// Market -> exchange mapping
const marketInfo = {
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

const db = new duckdb.Database(':memory:');

// Get list of parquet files
const files = fs.readdirSync(TRADE_DIR).filter(f => f.endsWith('.parquet'));

console.log('Loading', files.length, 'trade files...');

// Build a query that unions all markets, computes notional, filters large
const queries = [];
for (const f of files) {
  const market = f.replace('.parquet', '');
  const info = marketInfo[market];
  if (!info) { console.log('  skip unknown:', market); continue; }
  
  queries.push(`
    SELECT 
      '${info.ex}' as exchange,
      '${info.type}' as mtype,
      ts,
      price,
      qty,
      price * qty as notional,
      CASE WHEN side = 'buy' THEN price * qty ELSE 0 END as buy_notional,
      CASE WHEN side = 'sell' THEN price * qty ELSE 0 END as sell_notional
    FROM '${TRADE_DIR}/${f}'
    WHERE price * qty >= 10000
  `);
}

const unionSQL = queries.join('\nUNION ALL\n');

// Aggregate to 5min buckets
const aggSQL = `
  SELECT 
    exchange,
    mtype,
    CAST(ts / 300000 * 300000 AS BIGINT) as bucket,
    SUM(buy_notional) as buy_notional,
    SUM(sell_notional) as sell_notional,
    SUM(buy_notional - sell_notional) as delta,
    COUNT(*) as trade_count
  FROM (${unionSQL}) t
  GROUP BY exchange, mtype, bucket
  ORDER BY bucket ASC
`;
// Note: ts is in milliseconds, 5min = 300000ms

db.all(aggSQL, (err, rows) => {
  if (err) { console.error('Query error:', err.message); process.exit(1); }
  console.log('Result rows:', rows.length);

  // Write CSV
  const lines = ['exchange,type,bucket,buy_notional,sell_notional,delta,trade_count'];
  for (const r of rows) {
    lines.push(`${r.exchange},${r.mtype},${Number(r.bucket)},${r.buy_notional},${r.sell_notional},${r.delta},${Number(r.trade_count)}`);
  }
  fs.writeFileSync('/tmp/large_cvd_v2.csv', lines.join('\n'));
  console.log('Saved to /tmp/large_cvd_v2.csv');

  // Also compute perp vs spot totals
  const perpRows = rows.filter(r => r.mtype === 'perp');
  const spotRows = rows.filter(r => r.mtype === 'spot');
  
  // Per-exchange totals
  const exTotals = {};
  for (const r of rows) {
    if (!exTotals[r.exchange]) exTotals[r.exchange] = { buy: 0, sell: 0, cnt: 0 };
    exTotals[r.exchange].buy += Number(r.buy_notional);
    exTotals[r.exchange].sell += Number(r.sell_notional);
    exTotals[r.exchange].cnt += Number(r.trade_count);
  }
  console.log('\n=== Per-Exchange Large Trade Stats ===');
  console.log('Exchange          Buy(USD)         Sell(USD)        Net(USD)         Trades');
  for (const [ex, t] of Object.entries(exTotals).sort((a,b) => Math.abs(b[1].buy - b[1].sell) - Math.abs(a[1].buy - a[1].sell))) {
    console.log(`${ex.padEnd(16)} ${t.buy.toFixed(0).padStart(14)} ${t.sell.toFixed(0).padStart(14)} ${(t.buy - t.sell).toFixed(0).padStart(14)} ${t.cnt}`);
  }
  
  // Perp vs Spot totals
  let pBuy = 0, pSell = 0, sBuy = 0, sSell = 0;
  for (const r of rows) {
    if (r.mtype === 'perp') { pBuy += Number(r.buy_notional); pSell += Number(r.sell_notional); }
    else { sBuy += Number(r.buy_notional); sSell += Number(r.sell_notional); }
  }
  console.log(`\n=== Perp vs Spot ===`);
  console.log(`Perp: Buy=${pBuy.toFixed(0)} Sell=${pSell.toFixed(0)} Net=${(pBuy-pSell).toFixed(0)}`);
  console.log(`Spot: Buy=${sBuy.toFixed(0)} Sell=${sSell.toFixed(0)} Net=${(sBuy-sSell).toFixed(0)}`);

  db.close();
});
