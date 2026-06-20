#!/usr/bin/env node
// scripts/aggregate-1s.mjs — 1-second trade aggregation for long-term storage
// Run: cron every 60s, processes last 120s with 60s overlap

import duckdb from 'duckdb';
import fs from 'node:fs';
import path from 'node:path';

const DB_PATH = 'data/parquet/1s_agg.db';
const TRADE_DIR = 'data/live_fairprice/trades';
const LOOKBACK_MS = 120_000;

const SCHEMA = `
CREATE TABLE IF NOT EXISTS agg_1s (
  market VARCHAR, ts BIGINT,
  open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, vwap DOUBLE,
  trade_count BIGINT,
  buy_count BIGINT DEFAULT 0, sell_count BIGINT DEFAULT 0,
  total_qty DOUBLE, buy_qty DOUBLE DEFAULT 0, sell_qty DOUBLE DEFAULT 0,
  total_notional DOUBLE, buy_notional DOUBLE DEFAULT 0, sell_notional DOUBLE DEFAULT 0,
  buy_small_count BIGINT DEFAULT 0, buy_medium_count BIGINT DEFAULT 0, buy_large_count BIGINT DEFAULT 0,
  sell_small_count BIGINT DEFAULT 0, sell_medium_count BIGINT DEFAULT 0, sell_large_count BIGINT DEFAULT 0,
  buy_small_qty DOUBLE DEFAULT 0, buy_medium_qty DOUBLE DEFAULT 0, buy_large_qty DOUBLE DEFAULT 0,
  sell_small_qty DOUBLE DEFAULT 0, sell_medium_qty DOUBLE DEFAULT 0, sell_large_qty DOUBLE DEFAULT 0,
  total_small_count BIGINT DEFAULT 0, total_medium_count BIGINT DEFAULT 0, total_large_count BIGINT DEFAULT 0,
  total_small_qty DOUBLE DEFAULT 0, total_medium_qty DOUBLE DEFAULT 0, total_large_qty DOUBLE DEFAULT 0,
  delta_qty DOUBLE DEFAULT 0, delta_notional DOUBLE DEFAULT 0,
  imbalance_ratio DOUBLE DEFAULT 0,
  avg_trade_size_qty DOUBLE DEFAULT 0, avg_trade_notional DOUBLE DEFAULT 0,
  first_trade_id VARCHAR, last_trade_id VARCHAR,
  PRIMARY KEY (market, ts)
)`;

function classify(notional) {
  if (notional < 1000) return 'small';
  if (notional < 10000) return 'medium';
  return 'large';
}

async function q(sql) {
  const d = new duckdb.Database(':memory:');
  return new Promise((resolve, reject) => {
    d.all(sql, (err, rows) => { d.close(); err ? reject(err) : resolve(rows); });
  });
}

async function e(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (err) => err ? reject(err) : resolve());
  });
}

// Helper: convert BigInt → Number in query results
function convertRow(obj) {
  const r = {};
  for (const [k, v] of Object.entries(obj)) {
    r[k] = typeof v === 'bigint' ? Number(v) : v;
  }
  return r;
}

async function main() {
  const t0 = Date.now();
  const cutoff = t0 - LOOKBACK_MS;

  const db = new duckdb.Database(DB_PATH);
  await e(db, SCHEMA);

  // List markets by scanning trade directory
  const tradeFiles = fs.readdirSync(TRADE_DIR).filter(f => f.endsWith('.jsonl'));
  const markets = tradeFiles.map(f => f.replace('.jsonl', ''));

  let totalRows = 0;
  let totalMkts = 0;

  for (const market of markets) {
    const src = path.join(TRADE_DIR, `${market}.jsonl`);
    if (fs.statSync(src).size < 10) continue; // skip empty

    // Read trades in window via DuckDB temp query
    const srcEsc = src.replace(/'/g, "\\'");
    let rows;
    try {
      rows = await q(`
        SELECT ts::DOUBLE AS ts, price::DOUBLE AS price, qty::DOUBLE AS qty, side, tradeId
        FROM read_json_auto('${srcEsc}')
        WHERE ts >= ${cutoff}
        ORDER BY ts
      `);
    } catch {
      continue; // skip broken files
    }
    if (rows.length === 0) continue;

    // Group by 1s window
    const windows = new Map();
    for (const r of rows) {
      const win = Math.floor(r.ts / 1000) * 1000;
      if (!windows.has(win)) {
        windows.set(win, { market, ts: win, prices: [], qties: [], sides: [], nots: [], ids: [] });
      }
      const w = windows.get(win);
      w.prices.push(r.price);
      w.qties.push(r.qty);
      w.sides.push(r.side);
      w.nots.push(r.price * r.qty);
      w.ids.push(r.tradeId || '');
    }

    let inserted = 0;
    for (const [win, w] of windows) {
      const n = w.prices.length;
      const opens = [], highs = [], lows = [], closes = [], vwaps = [];
      
      // Compute OHLC+VWAP per window
      let open, high, low, close, vwapNum = 0, vwapDen = 0;
      let buyQty = 0, sellQty = 0, buyNotional = 0, sellNotional = 0;
      let buyCnt = 0, sellCnt = 0;
      let bSmall = 0, bMed = 0, bLarge = 0;
      let sSmall = 0, sMed = 0, sLarge = 0;
      let bSmallQ = 0, bMedQ = 0, bLargeQ = 0;
      let sSmallQ = 0, sMedQ = 0, sLargeQ = 0;
      let firstId = '', lastId = '';

      for (let i = 0; i < n; i++) {
        const price = w.prices[i], qty = w.qties[i], side = w.sides[i], notional = w.nots[i], id = w.ids[i];
        const isBuy = side === 'buy';

        if (i === 0) { open = price; firstId = id; }
        if (i === 0 || price > high) high = price;
        if (i === 0 || price < low) low = price;
        close = price;
        lastId = id;
        vwapNum += price * qty;
        vwapDen += qty;

        if (isBuy) { buyQty += qty; buyNotional += notional; buyCnt++; }
        else { sellQty += qty; sellNotional += notional; sellCnt++; }

        const bucket = classify(notional);
        if (isBuy) {
          if (bucket === 'small') { bSmall++; bSmallQ += qty; }
          else if (bucket === 'medium') { bMed++; bMedQ += qty; }
          else { bLarge++; bLargeQ += qty; }
        } else {
          if (bucket === 'small') { sSmall++; sSmallQ += qty; }
          else if (bucket === 'medium') { sMed++; sMedQ += qty; }
          else { sLarge++; sLargeQ += qty; }
        }
      }

      const vwap = vwapDen > 0 ? vwapNum / vwapDen : 0;
      const totalQty = buyQty + sellQty;
      const totalNotional = buyNotional + sellNotional;
      const deltaQty = buyQty - sellQty;
      const deltaNotional = buyNotional - sellNotional;
      const imbalance = totalQty > 0 ? deltaQty / totalQty : 0;

      const safe = String;
      
      await e(db, `
        INSERT OR REPLACE INTO agg_1s VALUES (
          '${safe(w.market)}', ${win},
          ${open}, ${high}, ${low}, ${close}, ${vwap},
          ${n}, ${buyCnt}, ${sellCnt},
          ${totalQty}, ${buyQty}, ${sellQty},
          ${totalNotional}, ${buyNotional}, ${sellNotional},
          ${bSmall}, ${bMed}, ${bLarge},
          ${sSmall}, ${sMed}, ${sLarge},
          ${bSmallQ}, ${bMedQ}, ${bLargeQ},
          ${sSmallQ}, ${sMedQ}, ${sLargeQ},
          ${bSmall + sSmall}, ${bMed + sMed}, ${bLarge + sLarge},
          ${bSmallQ + sSmallQ}, ${bMedQ + sMedQ}, ${bLargeQ + sLargeQ},
          ${deltaQty}, ${deltaNotional}, ${imbalance},
          ${totalQty / (n || 1)}, ${totalNotional / (n || 1)},
          '${safe(firstId).replace(/'/g, "\\'")}', '${safe(lastId).replace(/'/g, "\\'")}'
        )
      `);
      inserted++;
    }

    if (inserted > 0) {
      totalRows += inserted;
      totalMkts++;
    }
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`[1s_agg] ${totalMkts} markets, ${totalRows} rows in ${elapsed}s`);

  if (totalRows > 0) {
    const s = await new Promise((resolve, reject) => {
      db.all(`SELECT count(*)::varchar AS total, min(ts)::varchar AS min_ts, max(ts)::varchar AS max_ts FROM agg_1s`, (err, rows) => err ? reject(err) : resolve(rows));
    });
    if (s.length > 0) {
      console.log(`[1s_agg] DB: ${s[0].total} rows, ${new Date(Number(s[0].min_ts)).toISOString()} – ${new Date(Number(s[0].max_ts)).toISOString()}`);
    }
  }

  db.close();
}

async function daemonLoop(intervalSec) {
  const intervalMs = (intervalSec || 60) * 1000;
  console.log(`[1s_agg] daemon mode, interval=${intervalSec}s`);
  let running = true;
  process.on('SIGINT', () => { console.log('[1s_agg] SIGINT, exiting'); running = false; });
  process.on('SIGTERM', () => { console.log('[1s_agg] SIGTERM, exiting'); running = false; });

  while (running) {
    const start = Date.now();
    try { await main(); }
    catch (err) { console.error('[1s_agg] error:', err.message); }
    const elapsed = Date.now() - start;
    const wait = Math.max(100, intervalMs - elapsed);
    if (running) await new Promise(r => setTimeout(r, wait));
  }
}

const intervalSec = parseInt(process.argv.find(a => a.startsWith('--interval='))?.split('=')[1] || '60', 10);
if (process.argv.includes('--daemon')) {
  daemonLoop(intervalSec);
} else {
  main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
}
