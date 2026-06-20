const duckdb = require('duckdb');
const fs = require('fs');
const path = require('path');

const DB_PATH = 'data/parquet/1s_agg.db';
const TRADE_DIR = 'data/live_fairprice/trades';

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

async function main() {
  const t0 = Date.now();
  const db = new duckdb.Database(DB_PATH);

  // Create schema
  await new Promise((resolve, reject) => {
    db.exec(`CREATE TABLE IF NOT EXISTS agg_1s (
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
    )`, (e) => e ? reject(e) : resolve());
  });

  const tradeFiles = fs.readdirSync(TRADE_DIR).filter(f => f.endsWith('.jsonl'));
  const markets = tradeFiles.map(f => f.replace('.jsonl', ''));
  console.log('Processing', markets.length, 'markets...');

  let totalRows = 0;
  let totalMkts = 0;

  for (const market of markets) {
    const src = path.join(TRADE_DIR, market + '.jsonl');
    const stat = fs.statSync(src);
    if (stat.size < 10) { console.log('  skip (empty):', market); continue; }

    let rows;
    try {
      rows = await q(`SELECT ts::DOUBLE AS ts, price::DOUBLE AS price, qty::DOUBLE AS qty, side, tradeId FROM read_json_auto('${src.replace(/'/g, "\\'")}') ORDER BY ts`);
    } catch (e) {
      console.log('  skip (error):', market, e.message.substring(0, 80));
      continue;
    }
    if (rows.length === 0) { console.log('  skip (empty):', market); continue; }

    const windows = new Map();
    for (const r of rows) {
      const win = Math.floor(Number(r.ts) / 1000) * 1000;
      if (!windows.has(win)) {
        windows.set(win, { market, ts: win, prices: [], qties: [], sides: [], nots: [], ids: [] });
      }
      const w = windows.get(win);
      w.prices.push(Number(r.price));
      w.qties.push(Number(r.qty));
      w.sides.push(r.side);
      w.nots.push(Number(r.price) * Number(r.qty));
      w.ids.push(r.tradeId || '');
    }

    let inserted = 0;
    for (const [win, w] of windows) {
      const n = w.prices.length;
      let open, high = -Infinity, low = Infinity, close, vwapNum = 0, vwapDen = 0;
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
        if (price > high) high = price;
        if (price < low) low = price;
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

      await new Promise((resolve, reject) => {
        db.exec(`INSERT OR REPLACE INTO agg_1s VALUES (
          '${w.market}', ${win},
          ${open}, ${high === -Infinity ? 0 : high}, ${low === Infinity ? 0 : low}, ${close}, ${vwap},
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
          '${(firstId || '').replace(/'/g, "''")}', '${(lastId || '').replace(/'/g, "''")}'
        )`, (e) => e ? reject(e) : resolve());
      });
      inserted++;
    }

    const mb = (stat.size / 1048576).toFixed(0);
    if (inserted > 0) {
      totalRows += inserted;
      totalMkts++;
      console.log(market.padEnd(25) + inserted + ' rows (' + mb + 'MB)');
    }
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('\nDone: ' + totalMkts + ' markets, ' + totalRows + ' rows in ' + elapsed + 's');
  db.close();
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
