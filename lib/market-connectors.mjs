// lib/market-connectors.mjs — lightweight market-specific connector aliases

import { BinancePerpConnector } from './binance-connector.mjs';
import { BybitConnector } from './bybit-connector.mjs';
import { OkxConnector } from './okx-connector.mjs';
import { KrakenSpotConnector } from './kraken-connector.mjs';
import { FullBook } from './full-book.mjs';

export class BinanceCoinmPerpConnector extends BinancePerpConnector {
  constructor(config) {
    super(config);
    this.market = 'binance_coinm_perp';
    this.wsUrl = config.wsUrl || 'wss://dstream.binance.com/stream?streams=btcusd_perp@trade/btcusd_perp@depth@100ms/btcusd_perp@forceOrder';
    this.restUrl = config.restUrl || 'https://dapi.binance.com/dapi/v1/depth?symbol=BTCUSD_PERP&limit=1000';
    this.book = new FullBook('binance_coinm_perp', { maxLevels: config.depthLimit ?? 1000 });
  }

  _validateSync(snapshot) {
    if (this._ringBuf.length === 0) {
      // COIN-M BTCUSD_PERP can be quiet enough that no diff arrives while the
      // REST snapshot is fetched. Accept snapshot-only for this low-volume
      // alias; high-volume USD-M perps still require a buffered bridge.
      return true;
    }
    return super._validateSync(snapshot);
  }

  async _syncBook() {
    this._setState('syncing');
    this._ringBuf = [];
    this._ringBufPos = 0;

    for (let attempt = 0; attempt < 10; attempt++) {
      try {
        const snapshot = await this._fetchSnapshot();
        if (this._ringBuf.length === 0) {
          // Low-volume COIN-M can be quiet, but we still want a bridged diff
          // before entering running to avoid first-diff bridge gaps.
          // Use multiple retries with generous wait: up to 30s total.
          await new Promise((resolve) => setTimeout(resolve, 3000));
        }
        const valid = this._validateSync(snapshot);
        if (valid) {
          this._applyRingBuf(snapshot);
          if (!this._ringBufApplied) {
            continue;
          }
          this._stats.resyncCount++;
          this._stats.lastSeq = this.book._lastSeq || snapshot.lastUpdateId || 0;
          this._firstRunningDiff = false;
          this._setState('running');
          this._ringBuf = [];
          return;
        }
      } catch (err) {
        this.emit('error', { market: this.market, message: `sync attempt ${attempt} failed: ${err.message}` });
      }
    }
    this._setState('error');
    this.emit('error', { market: this.market, message: 'init sync failed after 10 retries' });
  }

  /** Override: COIN-M perp qty is in contracts (1 contract = 100 USD notional).
   *  Normalize to BTC: qty_btc = contracts * 100 / price */
  _handleTrade(event) {
    const price = parseFloat(event.p);
    const contracts = parseFloat(event.q);
    if (!price || !contracts) return;
    const qty = contracts * 100 / price; // 1 contract = 100 USD → base BTC qty
    this._emitTrade(
      price,
      qty,
      event.m ? 'sell' : 'buy',
      event.T,
      String(event.t),
    );
  }
}

export class BinancePerpBtcusdcConnector extends BinancePerpConnector {
  constructor(config) {
    super(config);
    this.market = 'binance_perp_btcusdc';
    this.wsUrl = config.wsUrl || 'wss://fstream.binance.com/stream?streams=btcusdc@trade/btcusdc@depth@100ms/btcusdc@forceOrder';
    this.restUrl = config.restUrl || 'https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDC&limit=1000';
    this.book = new FullBook('binance_perp_btcusdc', { maxLevels: config.depthLimit ?? 1000 });
  }
}

export class BybitSpotConnector extends BybitConnector {
  constructor(config) {
    super(config);
    this.market = 'bybit_spot';
    this.wsUrl = config.wsUrl || 'wss://stream.bybit.com/v5/public/spot';
    this.restUrl = config.restUrl || 'https://api.bybit.com/v5/market/orderbook?category=spot&symbol=BTCUSDT&limit=200';
    this.book = new FullBook('bybit_spot', { maxLevels: config.depthLimit ?? 200 });
  }

  subscribe() {
    // Spot endpoint does NOT support allLiquidation; max orderbook depth is 200.
    this._ws.send(JSON.stringify({
      op: 'subscribe',
      args: ['publicTrade.BTCUSDT', 'orderbook.200.BTCUSDT'],
    }));
  }
}

export class OkxSpotConnector extends OkxConnector {
  constructor(config) {
    super(config);
    this.market = 'okx_spot';
    this._contractValue = 1; // spot sz is already in BTC
    this.wsUrl = config.wsUrl || 'wss://ws.okx.com:8443/ws/v5/public';
    this.restUrl = config.restUrl || 'https://www.okx.com/api/v5/market/books?instId=BTC-USDT&sz=400';
    this.book = new FullBook('okx_spot', { maxLevels: config.depthLimit ?? 400 });
  }

  subscribe() {
    this._ws.send(JSON.stringify({
      op: 'subscribe',
      args: [
        { channel: 'trades', instId: 'BTC-USDT' },
        { channel: 'books', instId: 'BTC-USDT' },
      ],
    }));
  }
}

export class KrakenSpotConnectorAlias extends KrakenSpotConnector {}