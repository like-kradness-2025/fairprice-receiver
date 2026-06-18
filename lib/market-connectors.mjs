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

  /** Override: COIN-M perp qty is in contracts (1 contract = 100 USD notional).
   *  Normalize to BTC: qty_btc = contracts * 100 / price */
  _handleTrade(event) {
    const price = parseFloat(event.p);
    const contracts = parseFloat(event.q);
    if (!price || !contracts) return;
    const qty = (contracts * 100) / price;
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