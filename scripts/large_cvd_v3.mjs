#!/usr/bin/env node
// large_cvd_v3.mjs — Large CVD chart (>= $10k, <= $10M notional, 5min, perp/spot)
import duckdb from 'duckdb';
import fs from 'node:fs';

const TRADE_DIR = 'data/parquet/trades';
const LARGE_MIN = 10000;    // $10k
const LARGE_MAX = 10_000_000; // $10M cap (filter corruption)

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
const files = fs.readdirSync(TRADE_DIR).filter(f => f.endsWith('.parquet'));
console.log('Loading', files.length, 'files...');

const queries = [];
for (const f of files) {
  const market = f.replace('.parquet', '');
  const info = marketInfo[market];
  if (!info) { console.log('  skip:', market); continue; }
  
  queries.push(`
    SELECT '${info.ex}' as exchange, '${info.type}' as mtype, ts,
      price * qty as notional,
      CASE WHEN side = 'buy' THEN price * qty ELSE 0 END as buy_n,
      CASE WHEN side = 'sell' THEN price * qty ELSE 0 END as sell_n
    FROM '${TRADE_DIR}/${f}'
    WHERE price * qty >= ${LARGE_MIN} AND price * qty <= ${LARGE_MAX}
  `);
}

const unionSQL = queries.join('\nUNION ALL\n');

// 5min buckets
const aggSQL = `
  SELECT exchange, mtype,
    CAST(ts / 300000 * 300000 AS BIGINT) as bucket,
    SUM(buy_n) as buy_notional, SUM(sell_n) as sell_notional,
    SUM(buy_n - sell_n) as delta, COUNT(*) as trade_count
  FROM (${unionSQL}) t
  GROUP BY exchange, mtype, bucket ORDER BY bucket
`;

db.all(aggSQL, (err, rows) => {
  if (err) { console.error('Error:', err.message); process.exit(1); }
  console.log('Rows:', rows.length.toLocaleString());
  
  // Write raw data
  const lines = ['exchange,type,bucket,buy_notional,sell_notional,delta,trade_count'];
  for (const r of rows) {
    lines.push(`${r.exchange},${r.mtype},${Number(r.bucket)},${Number(r.buy_notional).toFixed(2)},${Number(r.sell_notional).toFixed(2)},${Number(r.delta).toFixed(2)},${Number(r.trade_count)}`);
  }
  fs.writeFileSync('/tmp/large_cvd_v3.csv', lines.join('\n'));
  console.log('Saved CSV');

  // Per exchange stats
  const exStats = {};
  for (const r of rows) {
    const ex = r.exchange;
    if (!exStats[ex]) exStats[ex] = { buy: 0, sell: 0, cnt: 0 };
    exStats[ex].buy += Number(r.buy_notional);
    exStats[ex].sell += Number(r.sell_notional);
    exStats[ex].cnt += Number(r.trade_count);
  }
  console.log('\n=== Per Exchange (Large $10k-$10M) ===');
  console.log('Exchange'.padEnd(14), 'Buy($)'.padStart(14), 'Sell($)'.padStart(14), 'Net($)'.padStart(14), 'Trades'.padStart(10));
  console.log('-'.repeat(66));
  const sorted = Object.entries(exStats).sort((a,b) => Math.abs(b[1].buy-b[1].sell) - Math.abs(a[1].buy-a[1].sell));
  for (const [ex, t] of sorted) {
    console.log(ex.padEnd(14), t.buy.toFixed(0).padStart(14), t.sell.toFixed(0).padStart(14), (t.buy-t.sell).toFixed(0).padStart(14), t.cnt.toString().padStart(10));
  }

  // Perp vs Spot totals
  let pBuy = 0, pSell = 0, sBuy = 0, sSell = 0;
  for (const r of rows) {
    if (r.mtype === 'perp') { pBuy += Number(r.buy_notional); pSell += Number(r.sell_notional); }
    else { sBuy += Number(r.buy_notional); sSell += Number(r.sell_notional); }
  }
  console.log(`\n=== Perp vs Spot Large CVD ===`);
  console.log(`Perp: Buy=${(pBuy/1e6).toFixed(2)}M Sell=${(pSell/1e6).toFixed(2)}M Net=${((pBuy-pSell)/1e6).toFixed(2)}M`);
  console.log(`Spot: Buy=${(sBuy/1e6).toFixed(2)}M Sell=${(sSell/1e6).toFixed(2)}M Net=${((sBuy-sSell)/1e6).toFixed(2)}M`);
  
  db.close();
});
