#!/usr/bin/env python3
"""CVD Size Bucket Charts — aggregate & per-market from agg/ Parquet.

Usage:
  # Aggregate (3-panel: Price / Spot CVD / Perp CVD)
  python3 scripts/cvd_size_buckets.py --agg

  # Per-market individual charts
  python3 scripts/cvd_size_buckets.py --markets

  # Single market
  python3 scripts/cvd_size_buckets.py --market binance_perp

  # Custom output directory
  python3 scripts/cvd_size_buckets.py --agg --out /path/to/output

  # Custom hours window
  python3 scripts/cvd_size_buckets.py --agg --hours 6
"""

import json, os, sys, subprocess, argparse
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from matplotlib.gridspec import GridSpec
from datetime import datetime, timezone

# ── Paths ──
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AGG_DIR = os.path.join(BASE_DIR, 'data', 'agg')
OUTPUT_DIR = BASE_DIR

# ── Style ──
plt.rcParams.update({
    'font.size': 8,
    'axes.facecolor': '#0b1628',
    'figure.facecolor': '#0b1628',
    'text.color': '#c8d6e5',
    'axes.edgecolor': '#1e3a5f',
    'axes.labelcolor': '#c8d6e5',
    'axes.grid': True,
    'grid.color': '#1e3a5f',
    'grid.alpha': 0.3,
    'legend.facecolor': '#0b1628',
    'legend.edgecolor': '#1e3a5f',
    'legend.labelcolor': '#c8d6e5',
})

# ── Colors ──
C_SMALL = '#4ade80'
C_MEDIUM = '#fbbf24'
C_LARGE = '#f43f5e'
C_PRICE = '#60a5fa'
TEXT_COLOR = '#c8d6e5'

SIZE_LABELS = ['Small (<$1k)', 'Medium ($1k-$10k)', 'Large (>=$10k)']
SIZE_COLORS = [C_SMALL, C_MEDIUM, C_LARGE]


def load_data(hours=24):
    """Load CVD and price data from agg/ parquet via DuckDB (Node.js)."""
    cutoff_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000) - hours * 3600 * 1000
    # Dump to temp file to avoid pipe buffer limits
    tmp_path = os.path.join(BASE_DIR, 'tmp_agg_export.json')
    script = f"""
    const fs = require('fs');
    const duckdb = require('duckdb');
    const db = new duckdb.Database(':memory:');
    db.all(`SELECT ts, market, type,
      buy_small_qty, buy_medium_qty, buy_large_qty,
      sell_small_qty, sell_medium_qty, sell_large_qty,
      delta_notional, trade_count, mid_price
      FROM read_parquet('{AGG_DIR}/*.parquet')
      WHERE ts >= {cutoff_ms}
      ORDER BY ts, market`, (err, rows) => {{
      if (err) {{ fs.writeFileSync('{tmp_path}', JSON.stringify({{error: err.message}})); process.exit(1); }}
      const out = rows.map(r => ({{
        ts: Number(r.ts), market: r.market, type: r.type,
        b_s: Number(r.buy_small_qty) || 0, b_m: Number(r.buy_medium_qty) || 0, b_l: Number(r.buy_large_qty) || 0,
        s_s: Number(r.sell_small_qty) || 0, s_m: Number(r.sell_medium_qty) || 0, s_l: Number(r.sell_large_qty) || 0,
        mid_price: Number(r.mid_price) || 0
      }}));
      fs.writeFileSync('{tmp_path}', JSON.stringify(out));
      process.exit(0);
    }});
    """
    result = subprocess.run(
        ['node', '-e', script],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        cwd=BASE_DIR, timeout=60
    )
    if result.returncode != 0:
        raise RuntimeError(f"DuckDB export failed (exit {result.returncode})")
    with open(tmp_path) as f:
        raw = json.load(f)
    os.unlink(tmp_path)
    if not raw:
        raise RuntimeError(f"No data found in {AGG_DIR} for last {hours}h")

    # Parse into structured arrays
    import pandas as pd
    df = pd.DataFrame(raw)
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    df['type'] = df['type'].str.strip().str.lower()
    df['market'] = df['market'].str.strip()
    for c in ['b_s','b_m','b_l','s_s','s_m','s_l']:
        df[c] = df[c].fillna(0)
    df['cvd_s'] = df['b_s'] - df['s_s']
    df['cvd_m'] = df['b_m'] - df['s_m']
    df['cvd_l'] = df['b_l'] - df['s_l']
    df = df.sort_values(['ts', 'market']).reset_index(drop=True)

    # Price reference: binance_perp mid_price
    tmp = df[df['market'] == 'binance_perp'][['ts','mid_price']].drop_duplicates('ts').sort_values('ts')
    if tmp.empty:
        tmp = df[df['type'] == 'perp'].groupby('ts')['mid_price'].mean().reset_index()
    price_df = tmp.set_index('ts').sort_index().resample('1s').ffill().dropna().reset_index()

    return df, price_df


def plot_price(ax, price_df):
    """Draw BTC price line on ax."""
    ax.plot(price_df['ts'], price_df['mid_price'], color=C_PRICE, linewidth=1.2, alpha=0.9)
    ax.set_ylabel('BTC Price', color=TEXT_COLOR, fontsize=7)
    ax.tick_params(axis='y', colors=C_PRICE, labelsize=6)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'${y:,.0f}'))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.tick_params(colors=TEXT_COLOR, labelsize=6)


def plot_cvd_twinx(ax, ts, s, m, l):
    """Plot 3 size CVD lines on a shared frame with independent Y axes.
    Small = left axis, Medium = right inner, Large = right outer.
    """
    ax.plot(ts, s, color=C_SMALL, linewidth=1.0, alpha=0.8, label='Small')
    ax.set_ylabel('Small CVD', color=C_SMALL, fontsize=7)
    ax.tick_params(axis='y', colors=C_SMALL, labelsize=6)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{y:.4f}'))
    ax.axhline(0, color=TEXT_COLOR, linewidth=0.4, alpha=0.3)

    a2 = ax.twinx()
    a2.plot(ts, m, color=C_MEDIUM, linewidth=1.0, alpha=0.8, label='Medium')
    a2.spines['right'].set_position(('outward', 0))
    a2.set_ylabel('Medium CVD', color=C_MEDIUM, fontsize=7)
    a2.tick_params(axis='y', colors=C_MEDIUM, labelsize=6)
    a2.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{y:.4f}'))

    a3 = ax.twinx()
    a3.plot(ts, l, color=C_LARGE, linewidth=1.0, alpha=0.8, label='Large')
    a3.spines['right'].set_position(('outward', 60))
    a3.set_ylabel('Large CVD', color=C_LARGE, fontsize=7)
    a3.tick_params(axis='y', colors=C_LARGE, labelsize=6)
    a3.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{y:.4f}'))


def add_legend(ax):
    """Add unified size legend on ax."""
    from matplotlib.lines import Line2D
    handles = [
        Line2D([0], [0], color=c, lw=1.5)
        for c in [C_SMALL, C_MEDIUM, C_LARGE]
    ]
    leg = ax.legend(handles, SIZE_LABELS, loc='upper left', fontsize=6, framealpha=0.8)
    for t, c in zip(leg.get_texts(), SIZE_COLORS):
        t.set_color(c)


def chart_aggregate(df, price_df, out_path):
    """Generate 3-panel aggregate chart: Price / Spot CVD / Perp CVD."""
    spot = df[df['type'] == 'spot'].groupby('ts')[['cvd_s','cvd_m','cvd_l']].sum().cumsum()
    perp = df[df['type'] == 'perp'].groupby('ts')[['cvd_s','cvd_m','cvd_l']].sum().cumsum()

    fig = plt.figure(figsize=(16, 10))
    gs = GridSpec(3, 1, height_ratios=[1.2, 2, 2], hspace=0.08,
                  left=0.06, right=0.88, bottom=0.06, top=0.96)

    ax_p = fig.add_subplot(gs[0])
    plot_price(ax_p, price_df)
    ax_p.set_title('Aggregate CVD by Size (Spot / Perp)', color=TEXT_COLOR,
                   fontsize=10, fontweight='bold')
    ax_p.tick_params(labelbottom=False)

    ax_s = fig.add_subplot(gs[1], sharex=ax_p)
    plot_cvd_twinx(ax_s, spot.index, spot['cvd_s'], spot['cvd_m'], spot['cvd_l'])
    ax_s.set_title('Spot CVD (Cumulative)', color=TEXT_COLOR,
                   fontsize=9, fontweight='bold')
    ax_s.tick_params(labelbottom=False)
    add_legend(ax_s)

    ax_pp = fig.add_subplot(gs[2], sharex=ax_p)
    plot_cvd_twinx(ax_pp, perp.index, perp['cvd_s'], perp['cvd_m'], perp['cvd_l'])
    ax_pp.set_title('Perp CVD (Cumulative)', color=TEXT_COLOR,
                    fontsize=9, fontweight='bold')
    ax_pp.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax_pp.tick_params(axis='x', labelsize=7)

    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def chart_per_market(df, price_df, out_dir, market_filter=None):
    """Generate per-market 2-panel charts: Price / Size CVD."""
    markets = sorted(df['market'].unique())
    if market_filter:
        markets = [m for m in markets if market_filter in m]

    saved = []
    for mkt in markets:
        mdf = df[df['market'] == mkt].sort_values('ts').set_index('ts')
        cum = mdf[['cvd_s','cvd_m','cvd_l']].cumsum()

        fig = plt.figure(figsize=(14, 6))
        gs = GridSpec(2, 1, height_ratios=[1, 2], hspace=0.06,
                      left=0.06, right=0.88, bottom=0.08, top=0.96)

        ax1 = fig.add_subplot(gs[0])
        plot_price(ax1, price_df)
        ax1.set_title(f'{mkt} — Size CVD', color=TEXT_COLOR,
                      fontsize=10, fontweight='bold')
        ax1.tick_params(labelbottom=False)

        ax2 = fig.add_subplot(gs[1], sharex=ax1)
        plot_cvd_twinx(ax2, cum.index, cum['cvd_s'], cum['cvd_m'], cum['cvd_l'])
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax2.tick_params(axis='x', labelsize=7)

        path = os.path.join(out_dir, f'cvd_{mkt}.png')
        fig.savefig(path, dpi=150)
        plt.close(fig)
        saved.append(path)
    return saved


def main():
    p = argparse.ArgumentParser(description='CVD Size Bucket Charts')
    p.add_argument('--agg', action='store_true', help='Aggregate 3-panel chart')
    p.add_argument('--markets', action='store_true', help='Per-market charts')
    p.add_argument('--market', type=str, default=None, help='Single market filter')
    p.add_argument('--hours', type=int, default=6, help='Lookback hours (default 6)')
    p.add_argument('--out', type=str, default=None, help='Output path (for --agg) or dir (for --markets)')
    args = p.parse_args()

    if not args.agg and not args.markets and not args.market:
        p.print_help()
        sys.exit(1)

    # Load data
    df, price_df = load_data(args.hours)
    print(f"Loaded {len(df)} rows, {len(df['market'].unique())} markets, "
          f"price range: {price_df['ts'].min()} → {price_df['ts'].max()}")

    if args.agg:
        out = args.out or os.path.join(BASE_DIR, 'agg_cvd_size.png')
        path = chart_aggregate(df, price_df, out)
        sz = os.path.getsize(path) / 1024
        print(f"Aggregate chart: {path} ({sz:.0f} KB)")

    if args.markets or args.market:
        out_dir = args.out or os.path.join(BASE_DIR, 'cvd_charts')
        os.makedirs(out_dir, exist_ok=True)
        saved = chart_per_market(df, price_df, out_dir, args.market)
        for p in saved:
            sz = os.path.getsize(p) / 1024
            print(f"  {p} ({sz:.0f} KB)")
        print(f"Saved {len(saved)} per-market charts to {out_dir}/")


if __name__ == '__main__':
    main()
