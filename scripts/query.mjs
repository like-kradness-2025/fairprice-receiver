#!/usr/bin/env node
// scripts/query.mjs — Query Parquet data via DuckDB
// Usage:
//   node scripts/query.mjs "SELECT market, count(*)::varchar FROM read_parquet('data/parquet/trades/*.parquet') GROUP BY market"
//   node scripts/query.mjs --trades "SELECT avg(price)::varchar as avg_price FROM this"
//   node scripts/query.mjs --book "SELECT count(*)::varchar FROM this WHERE market='binance_spot'"
//   node scripts/query.mjs --fairprice "SELECT * FROM this LIMIT 5"

import duckdb from 'duckdb';

const db = new duckdb.Database(':memory:');
const args = process.argv.slice(2);

function usage() {
  console.error(`Usage:
  node scripts/query.mjs <sql>                        — raw SQL
  node scripts/query.mjs --trades|--book|--fairprice <sql>  — scoped SQL
`);
  process.exit(1);
}

if (args.length === 0) usage();

let sql;
let label;

if (args[0].startsWith('--')) {
  const shortcut = args[0].slice(2); // trades | book | fairprice
  if (!['trades', 'book', 'fairprice'].includes(shortcut)) usage();
  const userSql = args.slice(1).join(' ');
  sql = userSql.replace(/\bthis\b/g, `read_parquet('data/parquet/${shortcut}/*.parquet')`);
  label = shortcut;
} else {
  sql = args.join(' ');
  label = 'raw';
}

console.error(`[query] ${label}: ${sql.substring(0, 120)}${sql.length > 120 ? '...' : ''}`);

db.all(sql, (err, rows) => {
  if (err) {
    console.error('ERROR:', err.message);
    process.exit(1);
  }
  // BigInt-safe JSON output
  const bigintReplacer = (key, val) =>
    typeof val === 'bigint' ? val.toString() : val;
  console.log(JSON.stringify(rows, bigintReplacer, 2));
  db.close();
});
