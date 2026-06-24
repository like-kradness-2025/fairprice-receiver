#!/usr/bin/env node
// scripts/aggregate-1s.mjs — 1-second trade + book $10bin + features → per-market Parquet
// Run: cron every 60s, processes last 120s with 60s overlap
//
// Output: data/agg/{market}.parquet (one file per market, merged with historical data)
// Schema: ts, market, type, trade OHLCV + size buckets, book mid/spread/depth/$10bin

import duckdb from 'duckdb';
import fs from 'node:fs';
import path from 'node:path';

const TRADE_DIR = 'data/live_fairprice/trades';
const BOOK_DIR = 'data/live_fairprice/book';
const AGG_DIR = 'data/agg';
const LOOKBACK_MS = 120_000;

// -- helpers -----------------------------------------------------------

/** Classify trade notional: small (<$1k), medium ($1k-$10k), large (>=$10k) */
function classify(notional) {
  if (notional < 1000) return 'small';
  if (notional < 10000) return 'medium';
  return 'large';
}

/** Round price to nearest $10 bin */
function toBin10(price) {
  return Math.round(price / 10) * 10;
}

/** Basis points distance from mid */
function bpsFromMid(price, mid) {
  return Math.abs(price - mid) / mid * 10000;
}

/** Validate market name (alphanumeric + underscore/hyphen only) */
function safeMarket(m) {
  if (!/^[a-zA-Z0-9_-]+$/.test(m)) throw new Error(`Unsafe market name: ${m}`);
  return m;
}

/** Determine market type from name */
function marketType(market) {
  // perp markets contain _perp or _coinm_
  if (market.includes('_perp') || market.includes('_coinm_')) return 'perp';
  return 'spot';
}

/** Escape a value for SQL literal: numbers as-is, strings single-quote-escaped, null/undefined → NULL */
function sqlVal(v) {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'number') {
    if (!Number.isFinite(v)) return 'NULL';
    return String(v);
  }
  return `'${String(v).replace(/'/g, "''")}'`;
}

// -- DuckDB wrappers ---------------------------------------------------

async function q(db, sql) {
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => { err ? reject(err) : resolve(rows); });
  });
}

async function e(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (err) => err ? reject(err) : resolve());
  });
}

// -- Book feature extraction ------------------------------------------

/**
 * Given an array of book snapshots (each with {ts, bids, asks} where bids/asks are [[priceStr, qtyStr],...]),
 * find the snapshot closest to winTs and compute mid/spread/depth/$10bin.
 */
function computeBookFeatures(bookSnaps, winTs) {
  const result = {
    mid_price: null,
    spread_bps: null,
    bid_depth_5bps: 0,
    bid_depth_25bps: 0,
    bid_depth_100bps: 0,
    ask_depth_5bps: 0,
    ask_depth_25bps: 0,
    ask_depth_100bps: 0,
    bid_bucketed: '{}',
    ask_bucketed: '{}',
  };

  if (!bookSnaps || bookSnaps.length === 0) return result;

  // Find nearest snapshot by timestamp (linear scan with early exit; snaps are sorted)
  let bestSnap = null;
  let bestDist = Infinity;
  for (const snap of bookSnaps) {
    const dist = Math.abs(Number(snap.ts) - winTs);
    if (dist < bestDist) { bestDist = dist; bestSnap = snap; }
    if (Number(snap.ts) > winTs && dist > bestDist) break;
  }
  if (!bestSnap) return result;

  const bids = bestSnap.bids;
  const asks = bestSnap.asks;
  if (!bids || !asks || bids.length === 0 || asks.length === 0) return result;

  const bestBid = Number(bids[0][0]);
  const bestAsk = Number(asks[0][0]);
  const mid = (bestBid + bestAsk) / 2;
  if (mid <= 0 || !Number.isFinite(mid)) return result;

  result.mid_price = mid;
  result.spread_bps = (bestAsk - bestBid) / mid * 10000;

  // Depth within bps ranges
  for (const [priceStr, qtyStr] of bids) {
    const px = Number(priceStr);
    const bps = bpsFromMid(px, mid);
    const qty = Number(qtyStr);
    if (bps <= 5)   result.bid_depth_5bps += qty;
    if (bps <= 25)  result.bid_depth_25bps += qty;
    if (bps <= 100) result.bid_depth_100bps += qty;
  }
  for (const [priceStr, qtyStr] of asks) {
    const px = Number(priceStr);
    const bps = bpsFromMid(px, mid);
    const qty = Number(qtyStr);
    if (bps <= 5)   result.ask_depth_5bps += qty;
    if (bps <= 25)  result.ask_depth_25bps += qty;
    if (bps <= 100) result.ask_depth_100bps += qty;
  }

  // $10 bin bucketing
  const bidBuckets = new Map();
  const askBuckets = new Map();
  for (const [priceStr, qtyStr] of bids) {
    const bin = toBin10(Number(priceStr));
    bidBuckets.set(bin, (bidBuckets.get(bin) || 0) + Number(qtyStr));
  }
  for (const [priceStr, qtyStr] of asks) {
    const bin = toBin10(Number(priceStr));
    askBuckets.set(bin, (askBuckets.get(bin) || 0) + Number(qtyStr));
  }
  // Sort bins: bids descending, asks ascending; convert to JSON object string
  const bidSorted = Object.fromEntries([...bidBuckets.entries()].sort((a, b) => b[0] - a[0]));
  const askSorted = Object.fromEntries([...askBuckets.entries()].sort((a, b) => a[0] - b[0]));
  result.bid_bucketed = JSON.stringify(bidSorted);
  result.ask_bucketed = JSON.stringify(askSorted);

  return result;
}

// -- Per-market processing ---------------------------------------------

async function processMarket(market, cutoff) {
  const mn = safeMarket(market);
  const mtype = marketType(market);
  const tradePath = path.join(TRADE_DIR, `${market}.jsonl`);
  const bookPath = path.join(BOOK_DIR, `${market}.jsonl`);
  const outPath = path.join(AGG_DIR, `${market}.parquet`);

  // Skip if no trade data
  if (!fs.existsSync(tradePath) || fs.statSync(tradePath).size < 10) return null;

  const db = new duckdb.Database(':memory:');

  // --- Schema ---
  await e(db, `CREATE TABLE agg_1s (
    ts BIGINT, market VARCHAR, type VARCHAR,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, vwap DOUBLE,
    trade_count BIGINT,
    buy_qty DOUBLE, sell_qty DOUBLE,
    buy_notional DOUBLE, sell_notional DOUBLE, delta_notional DOUBLE,
    buy_small_qty DOUBLE, buy_medium_qty DOUBLE, buy_large_qty DOUBLE,
    sell_small_qty DOUBLE, sell_medium_qty DOUBLE, sell_large_qty DOUBLE,
    mid_price DOUBLE, spread_bps DOUBLE,
    bid_depth_5bps DOUBLE, bid_depth_25bps DOUBLE, bid_depth_100bps DOUBLE,
    ask_depth_5bps DOUBLE, ask_depth_25bps DOUBLE, ask_depth_100bps DOUBLE,
    bid_bucketed VARCHAR, ask_bucketed VARCHAR
  )`);

  // --- Read trades ---
  let tradeRows;
  try {
    tradeRows = await q(db, `SELECT ts::DOUBLE AS ts, price::DOUBLE AS price, qty::DOUBLE AS qty, side
      FROM read_json_auto('${tradePath}')
      WHERE ts >= ${cutoff}
      ORDER BY ts`);
  } catch (err) {
    db.close();
    console.error(`[agg] ${market}: trade read error: ${err.message}`);
    return null;
  }
  if (!tradeRows || tradeRows.length === 0) { db.close(); return null; }

  // --- Read book snapshots ---
  let bookSnaps = [];
  if (fs.existsSync(bookPath) && fs.statSync(bookPath).size > 10) {
    try {
      bookSnaps = await q(db, `SELECT ts::BIGINT AS ts, bids, asks
        FROM read_json_auto('${bookPath}')
        WHERE ts >= ${cutoff}
        ORDER BY ts`);
    } catch (err) {
      // Book file may have parse errors; continue with empty
      console.error(`[agg] ${market}: book read warning: ${err.message}`);
    }
  }

  // --- Group trades by 1s window ---
  const windows = new Map();
  for (const r of tradeRows) {
    const win = Math.floor(r.ts / 1000) * 1000;
    if (!windows.has(win)) {
      windows.set(win, { ts: win, prices: [], qties: [], sides: [], nots: [] });
    }
    const w = windows.get(win);
    w.prices.push(r.price);
    w.qties.push(r.qty);
    w.sides.push(r.side);
    w.nots.push(r.price * r.qty);
  }

  // --- Process each window ---
  let inserted = 0;
  for (const [winTs, w] of windows) {
    const n = w.prices.length;
    let open, high = -Infinity, low = Infinity, close;
    let buyQty = 0, sellQty = 0, buyNotional = 0, sellNotional = 0;
    let bSmallQ = 0, bMedQ = 0, bLargeQ = 0;
    let sSmallQ = 0, sMedQ = 0, sLargeQ = 0;
    let vwapNum = 0, vwapDen = 0;

    for (let i = 0; i < n; i++) {
      const price = w.prices[i], qty = w.qties[i], side = w.sides[i], notional = w.nots[i];
      const isBuy = side === 'buy';

      if (i === 0) open = price;
      if (price > high) high = price;
      if (price < low) low = price;
      close = price;
      vwapNum += price * qty;
      vwapDen += qty;

      if (isBuy) { buyQty += qty; buyNotional += notional; }
      else       { sellQty += qty; sellNotional += notional; }

      const bucket = classify(notional);
      if (isBuy) {
        if (bucket === 'small')  bSmallQ += qty;
        else if (bucket === 'medium') bMedQ += qty;
        else bLargeQ += qty;
      } else {
        if (bucket === 'small')  sSmallQ += qty;
        else if (bucket === 'medium') sMedQ += qty;
        else sLargeQ += qty;
      }
    }

    const vwap = vwapDen > 0 ? vwapNum / vwapDen : 0;
    const deltaNotional = buyNotional - sellNotional;

    // Book features for this window
    const bf = computeBookFeatures(bookSnaps, winTs);

    // SQL INSERT — all values are numbers or safely-escaped strings
    await e(db, `INSERT INTO agg_1s VALUES (
      ${winTs}, ${sqlVal(mn)}, ${sqlVal(mtype)},
      ${sqlVal(open)}, ${sqlVal(high === -Infinity ? null : high)}, ${sqlVal(low === Infinity ? null : low)}, ${sqlVal(close)}, ${sqlVal(vwap)},
      ${n},
      ${buyQty}, ${sellQty},
      ${buyNotional}, ${sellNotional}, ${deltaNotional},
      ${bSmallQ}, ${bMedQ}, ${bLargeQ},
      ${sSmallQ}, ${sMedQ}, ${sLargeQ},
      ${sqlVal(bf.mid_price)}, ${sqlVal(bf.spread_bps)},
      ${bf.bid_depth_5bps}, ${bf.bid_depth_25bps}, ${bf.bid_depth_100bps},
      ${bf.ask_depth_5bps}, ${bf.ask_depth_25bps}, ${bf.ask_depth_100bps},
      ${sqlVal(bf.bid_bucketed)}, ${sqlVal(bf.ask_bucketed)}
    )`);
    inserted++;
  }

  // --- Write Parquet (merge with existing if present) ---
  if (inserted > 0) {
    if (fs.existsSync(outPath)) {
      try {
        // Read existing parquet, keep rows whose ts is NOT in the new batch
        // Do everything in DuckDB to avoid BigInt serialization issues
        await e(db, `CREATE TEMP TABLE existing AS SELECT * FROM read_parquet('${outPath}')`);
        await e(db, `DELETE FROM existing WHERE ts IN (SELECT ts FROM agg_1s)`);
        await e(db, `INSERT INTO agg_1s SELECT * FROM existing`);
        await e(db, `DROP TABLE IF EXISTS existing`);
      } catch (err) {
        console.error(`[agg] ${market}: merge warning (will overwrite): ${err.message}`);
      }
    }

    await e(db, `COPY (SELECT * FROM agg_1s ORDER BY ts) TO '${outPath}' (FORMAT PARQUET, COMPRESSION ZSTD)`);
  }

  db.close();
  return { market, rows: inserted };
}

// -- Main / Daemon -----------------------------------------------------

async function main() {
  const t0 = Date.now();
  const cutoffOverride = process.argv.find(a => a.startsWith('--cutoff='));
  const cutoff = cutoffOverride ? parseInt(cutoffOverride.split('=')[1], 10) : (t0 - LOOKBACK_MS);

  fs.mkdirSync(AGG_DIR, { recursive: true });

  // List markets from trade directory
  const tradeFiles = fs.readdirSync(TRADE_DIR).filter(f => f.endsWith('.jsonl'));
  const markets = tradeFiles.map(f => f.replace('.jsonl', ''));

  let totalRows = 0;
  let totalMkts = 0;

  for (const market of markets) {
    try {
      const result = await processMarket(market, cutoff);
      if (result && result.rows > 0) {
        totalRows += result.rows;
        totalMkts++;
        console.log(`[agg] ${result.market.padEnd(25)} ${String(result.rows).padStart(6)} rows → data/agg/${result.market}.parquet`);
      }
    } catch (err) {
      console.error(`[agg] ${market}: error: ${err.message}`);
    }
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`[agg] ${totalMkts} markets, ${totalRows} rows in ${elapsed}s`);

  // Summary from parquet files
  if (totalRows > 0) {
    const summaryDb = new duckdb.Database(':memory:');
    try {
      const parquetFiles = fs.readdirSync(AGG_DIR).filter(f => f.endsWith('.parquet'));
      for (const pf of parquetFiles) {
        const pp = path.join(AGG_DIR, pf);
        if (fs.statSync(pp).size < 100) continue;
        try {
          const rows = await q(summaryDb, `SELECT count(*)::VARCHAR AS cnt, min(ts)::VARCHAR AS min_ts, max(ts)::VARCHAR AS max_ts FROM read_parquet('${pp}')`);
          if (rows.length > 0 && rows[0].cnt > 0) {
            console.log(`[agg]   ${pf.padEnd(30)} ${rows[0].cnt.padStart(6)} rows, ${new Date(Number(rows[0].min_ts)).toISOString()} – ${new Date(Number(rows[0].max_ts)).toISOString()}`);
          }
        } catch { /* skip broken parquet */ }
      }
    } catch { /* skip summary errors */ }
    summaryDb.close();
  }
}

async function daemonLoop(intervalSec) {
  const intervalMs = (intervalSec || 60) * 1000;
  console.log(`[agg] daemon mode, interval=${intervalSec}s`);
  let running = true;
  process.on('SIGINT', () => { console.log('[agg] SIGINT, exiting'); running = false; });
  process.on('SIGTERM', () => { console.log('[agg] SIGTERM, exiting'); running = false; });

  while (running) {
    const start = Date.now();
    try { await main(); }
    catch (err) { console.error('[agg] error:', err.message); }
    const elapsed = Date.now() - start;
    const wait = Math.max(100, intervalMs - elapsed);
    if (running) await new Promise(r => setTimeout(r, wait));
  }
}

// -- Entry -------------------------------------------------------------

const intervalSec = parseInt(process.argv.find(a => a.startsWith('--interval='))?.split('=')[1] || '60', 10);
if (process.argv.includes('--daemon')) {
  daemonLoop(intervalSec);
} else if (process.argv.includes('--export')) {
  // --export: dump agg data as JSONL for downstream scripts
  const hours = parseFloat(process.argv.find(a => a.startsWith('--hours='))?.split('=')[1] || '24');
  const marketFilter = process.argv.find(a => a.startsWith('--market='))?.split('=')[1] || '';
  const cutoff = Date.now() - hours * 3600 * 1000;
  const showCols = ['ts', 'market', 'type',
    'buy_notional', 'sell_notional', 'delta_notional',
    'buy_small_qty', 'buy_medium_qty', 'buy_large_qty',
    'sell_small_qty', 'sell_medium_qty', 'sell_large_qty',
    'mid_price', 'spread_bps'];
  const colStr = showCols.join(', ');
  const db = new duckdb.Database(':memory:');
  try {
    const markets = fs.readdirSync(AGG_DIR).filter(f => f.endsWith('.parquet'));
    for (const f of markets) {
      const mkt = f.replace('.parquet', '');
      if (marketFilter && !mkt.includes(marketFilter)) continue;
      const path = `${AGG_DIR}/${f}`;
      const rows = await new Promise((resolve, reject) => {
        db.all(`SELECT ${colStr} FROM read_parquet('${path.replace(/'/g, "\\'")}') WHERE ts >= ${cutoff} ORDER BY ts`, (err, r) => err ? reject(err) : resolve(r));
      });
      for (const r of rows) {
        const obj = {};
        for (const [k, v] of Object.entries(r)) {
          obj[k] = typeof v === 'bigint' ? Number(v) : v;
        }
        console.log(JSON.stringify(obj));
      }
    }
  } finally { db.close(); }
} else {
  main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
}
