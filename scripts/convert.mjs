#!/usr/bin/env node
// scripts/convert.mjs — Convert JSONL to Parquet via DuckDB
// Usage: node scripts/convert.mjs [--all] [--recent]
//
// --all      : convert all JSONL files (one-shot initial run)
// --recent   : skip files already converted (compare mtime)
// (default)  : convert all, skip if .parquet already exists

import duckdb from 'duckdb';
import fs from 'node:fs';
import path from 'node:path';

const BASE = 'data/live_fairprice';
const OUT = 'data/parquet';

function srcPath(cat, market) {
  const p = path.join(BASE, cat, `${market}.jsonl`);
  return fs.existsSync(p) ? p : null;
}

function listMarkets(cat) {
  const dir = path.join(BASE, cat);
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir)
    .filter(f => f.endsWith('.jsonl'))
    .map(f => f.replace('.jsonl', ''));
}

function needsConversion(cat, market) {
  const src = srcPath(cat, market);
  if (!src) return false;
  const dst = path.join(OUT, cat, `${market}.parquet`);
  if (!fs.existsSync(dst)) return true;
  // Re-convert if source is newer than parquet
  return fs.statSync(src).mtimeMs > fs.statSync(dst).mtimeMs;
}

async function main() {
  const start = Date.now();
  const args = process.argv.slice(2);
  const mode = args.includes('--recent') ? 'recent' : 'all';

  const db = new duckdb.Database(':memory:');
  const exec = (sql) => new Promise((resolve, reject) => {
    db.exec(sql, (err) => err ? reject(err) : resolve());
  });
  const all = (sql) => new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => err ? reject(err) : resolve(rows));
  });

  let totalSrcMb = 0;
  let totalDstMb = 0;
  let totalFiles = 0;

  for (const [cat, label] of [['trades','trades'], ['book','book'], ['fairprice','fairprice']]) {
    const outDir = path.join(OUT, cat);
    fs.mkdirSync(outDir, { recursive: true });
    const markets = listMarkets(cat);
    const toConvert = mode === 'all'
      ? markets
      : markets.filter(m => needsConversion(cat, m));

    if (toConvert.length === 0) { console.log(`[${label}] up to date`); continue; }
    console.log(`\n[${label}] ${toConvert.length}/${markets.length} files to convert`);

    for (const market of toConvert) {
      const src = srcPath(cat, market);
      const dst = path.join(outDir, `${market}.parquet`);
      const srcMb = fs.statSync(src).size / 1048576;

      try {
        // CONVERT: read JSONL → write Parquet (zstd compression)
        await exec(`COPY (
          SELECT * FROM read_json_auto('${src.replace(/'/g, "\\'")}')
        ) TO '${dst.replace(/'/g, "\\'")}' (FORMAT PARQUET, COMPRESSION ZSTD)`);

        const dstMb = fs.statSync(dst).size / 1048576;
        totalSrcMb += srcMb;
        totalDstMb += dstMb;
        totalFiles++;

        // Row count
        const rows = await all(`SELECT count(*)::varchar as cnt FROM read_parquet('${dst.replace(/'/g, "\\'")}')`);
        const cnt = parseInt(rows[0]?.cnt || '0', 10);

        console.log(`  ${market}: ${(cnt/1000).toFixed(0)}k rows, ${srcMb.toFixed(0)} MB → ${dstMb.toFixed(1)} MB (${(srcMb/dstMb).toFixed(0)}x)`);
      } catch (err) {
        console.error(`  ${market}: FAILED — ${err.message}`);
      }
    }
  }

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  const totalMb = totalSrcMb.toFixed(0);
  const compMb = totalDstMb.toFixed(1);
  const ratio = totalDstMb > 0 ? (totalSrcMb / totalDstMb).toFixed(0) : '?';
  console.log(`\n=== DONE in ${elapsed}s ===`);
  console.log(`  ${totalFiles} files: ${totalMb} MB → ${compMb} MB (${ratio}x)`);
  db.close();
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
