#!/usr/bin/env node
// scripts/cleanup.mjs — Data retention enforcement
// Usage: node scripts/cleanup.mjs
// Run via cron daily.
//
// Policies:
// - Raw JSONL trades: keep 1 day → convert to Parquet → delete
// - 1s aggregation: keep 7 days → delete older rows
// - Book snapshots: keep 1 day → convert to Parquet → delete

import duckdb from 'duckdb';
import fs from 'node:fs';
import path from 'node:path';

const BASE = 'data/live_fairprice';
const PARQUET_DIR = 'data/parquet';
const RETENTION = {
  trades: 86400,     // 1 day (seconds)
  book: 86400,       // 1 day
  fairprice: 86400,  // 1 day
};
const AGG_DB = 'data/parquet/1s_agg.db';
const AGG_RETENTION_MS = 7 * 86400 * 1000; // 7 days

function listMarkets(cat) {
  const dir = path.join(BASE, cat);
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir)
    .filter(f => f.endsWith('.jsonl'))
    .map(f => f.replace('.jsonl', ''));
}

async function main() {
  const now = Date.now();
  let totalDel = 0;

  for (const [cat, maxAgeSec] of Object.entries(RETENTION)) {
    const cutoff = now - maxAgeSec * 1000;
    const markets = listMarkets(cat);

    for (const market of markets) {
      const srcPath = path.join(BASE, cat, `${market}.jsonl`);
      const stats = fs.statSync(srcPath);
      if (stats.mtimeMs > cutoff) continue; // still fresh

      const dstDir = path.join(PARQUET_DIR, cat);
      const dstPath = path.join(dstDir, `${market}.parquet`);
      fs.mkdirSync(dstDir, { recursive: true });

      // Convert to Parquet (only if not already converted or newer)
      if (!fs.existsSync(dstPath) || stats.mtimeMs > fs.statSync(dstPath).mtimeMs) {
        const db = new duckdb.Database(':memory:');
        try {
          await new Promise((resolve, reject) => {
            db.exec(
              `COPY (SELECT * FROM read_json_auto('${srcPath.replace(/'/g, "\\'")}')) 
               TO '${dstPath.replace(/'/g, "\\'")}' (FORMAT PARQUET, COMPRESSION ZSTD)`,
              (err) => err ? reject(err) : resolve()
            );
          });
        } finally {
          db.close();
        }
      }

      // Delete stale JSONL (after successful Parquet write)
      fs.unlinkSync(srcPath);
      totalDel++;
      const srcMb = (stats.size / 1048576).toFixed(0);
      const dstMb = (fs.existsSync(dstPath) ? fs.statSync(dstPath).size / 1048576 : 0).toFixed(1);
      console.log(`[${cat}] ${market}: ${srcMb}MB JSONL → ${dstMb}MB Parquet, deleted`);
    }
  }

  // Cleanup 1s aggregation (delete rows older than 7 days)
  if (fs.existsSync(AGG_DB)) {
    const db = new duckdb.Database(AGG_DB);
    try {
      const deleteCutoff = now - AGG_RETENTION_MS;
      const countBefore = await new Promise((resolve, reject) => {
        db.all(`SELECT count(*)::varchar as cnt FROM agg_1s`, (err, rows) =>
          err ? reject(err) : resolve(parseInt(rows[0]?.cnt || '0', 10))
        );
      });
      await new Promise((resolve, reject) => {
        db.exec(`DELETE FROM agg_1s WHERE ts < ${deleteCutoff}`, (err) =>
          err ? reject(err) : resolve()
        );
      });
      const countAfter = await new Promise((resolve, reject) => {
        db.all(`SELECT count(*)::varchar as cnt FROM agg_1s`, (err, rows) =>
          err ? reject(err) : resolve(parseInt(rows[0]?.cnt || '0', 10))
        );
      });
      console.log(`[1s_agg] removed ${countBefore - countAfter} stale rows (${countAfter} remaining)`);
    } finally {
      db.close();
    }
  }

  const totalMb = totalDel > 0 ? 'done' : 'up to date';
  console.log(`[cleanup] ${totalDel} files archived, ${totalMb}`);
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
