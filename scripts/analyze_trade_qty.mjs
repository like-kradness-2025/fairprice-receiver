#!/usr/bin/env node
// Analyze trade qty distribution for threshold design
import fs from 'node:fs';
import path from 'node:path';

const dir = '/home/weed420/fairprice-receiver/data/live_fairprice/trades';
const files = fs.readdirSync(dir).filter(f => f.endsWith('.jsonl')).sort();

const results = {};

for (const file of files) {
  const market = file.replace('.jsonl', '');
  const lines = fs.readFileSync(path.join(dir, file), 'utf-8').trim().split('\n').filter(Boolean);
  const qtys = [];

  for (const line of lines) {
    const t = JSON.parse(line);
    qtys.push(t.qty);
  }

  qtys.sort((a, b) => a - b);
  const n = qtys.length;
  if (n < 10) continue;

  const percentiles = [50, 75, 90, 95, 99, 99.5, 99.9, 100].map(p => {
    const idx = Math.floor(p / 100 * (n - 1));
    return { p, v: qtys[idx] };
  });

  const avg = qtys.reduce((s, v) => s + v, 0) / n;
  const sum = qtys.reduce((s, v) => s + v, 0);

  results[market] = {
    count: n,
    sum: sum.toFixed(4),
    avg: avg.toFixed(6),
    min: qtys[0],
    max: qtys[n - 1],
    percentiles: percentiles,
    // Bin-based distribution (log scale)
    bins: {
      '~0.0001': qtys.filter(v => v <= 0.0001).length,
      '0.0001-0.001': qtys.filter(v => v > 0.0001 && v <= 0.001).length,
      '0.001-0.01': qtys.filter(v => v > 0.001 && v <= 0.01).length,
      '0.01-0.1': qtys.filter(v => v > 0.01 && v <= 0.1).length,
      '0.1-1': qtys.filter(v => v > 0.1 && v <= 1).length,
      '1-10': qtys.filter(v => v > 1 && v <= 10).length,
      '10+': qtys.filter(v => v > 10).length,
    },
  };
}

// Print summary
console.log('MARKET'.padEnd(28) + 'COUNT'.padStart(8) + 'SUM(BTC)'.padStart(12) + 'AVG'.padStart(10) + 'MIN'.padStart(10) + 'P50'.padStart(10) + 'P75'.padStart(10) + 'P90'.padStart(10) + 'P95'.padStart(10) + 'P99'.padStart(10) + 'MAX'.padStart(10));
console.log('-'.repeat(130));
for (const [market, r] of Object.entries(results).sort((a, b) => b[1].count - a[1].count)) {
  const p = r.percentiles;
  const p50 = p[0].v.toFixed(6);
  const p75 = p[1].v.toFixed(6);
  const p90 = p[2].v.toFixed(6);
  const p95 = p[3].v.toFixed(6);
  const p99 = p[4].v.toFixed(6);
  const max = r.max.toFixed(6);
  console.log(market.padEnd(28) + String(r.count).padStart(8) + r.sum.padStart(12) + r.avg.padStart(10) + r.min.toFixed(6).padStart(10) + p50.padStart(10) + p75.padStart(10) + p90.padStart(10) + p95.padStart(10) + p99.padStart(10) + max.padStart(10));
}

console.log('\n=== Bin Distribution (% per market) ===');
console.log('MARKET'.padEnd(28) + '~0.0001'.padStart(8) + '~0.001'.padStart(8) + '~0.01'.padStart(8) + '~0.1'.padStart(8) + '~1'.padStart(8) + '~10'.padStart(8) + '10+'.padStart(6));
console.log('-'.repeat(80));
for (const [market, r] of Object.entries(results).sort((a, b) => b[1].count - a[1].count)) {
  const b = r.bins;
  const total = r.count;
  console.log(market.padEnd(28) +
    (b['~0.0001'] / total * 100).toFixed(1).padStart(7) + '%' +
    (b['0.0001-0.001'] / total * 100).toFixed(1).padStart(7) + '%' +
    (b['0.001-0.01'] / total * 100).toFixed(1).padStart(7) + '%' +
    (b['0.01-0.1'] / total * 100).toFixed(1).padStart(7) + '%' +
    (b['0.1-1'] / total * 100).toFixed(1).padStart(7) + '%' +
    (b['1-10'] / total * 100).toFixed(1).padStart(7) + '%' +
    (b['10+'] / total * 100).toFixed(1).padStart(6) + '%');
}

console.log('\n=== Aggregate across all markets ===');
let totalAll = 0;
const aggBins = { '~0.0001': 0, '0.0001-0.001': 0, '0.001-0.01': 0, '0.01-0.1': 0, '0.1-1': 0, '1-10': 0, '10+': 0 };
for (const r of Object.values(results)) {
  totalAll += r.count;
  for (const [k, v] of Object.entries(r.bins)) aggBins[k] += v;
}
for (const [k, v] of Object.entries(aggBins)) {
  console.log(`${k.padEnd(12)}: ${String(v).padStart(8)}行 ${(v / totalAll * 100).toFixed(1)}%`);
}
