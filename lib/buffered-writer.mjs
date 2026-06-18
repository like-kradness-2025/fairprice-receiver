// lib/buffered-writer.mjs — Crash-safe JSONL buffered writer for btc-receiver v3.00

import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';

/**
 * @typedef {Object} BufferedWriterOptions
 * @property {number} [flushIntervalMs=200]
 * @property {number} [maxBufferLines=4096]
 * @property {number} [maxLossMs=1000]
 * @property {boolean} [autoFlush=true]
 */

export class BufferedWriter {
  /** @type {string} */ _filePath;
  /** @type {string} */ _dir;
  /** @type {fs.WriteStream|null} */ _stream;
  /** @type {string[]} */ _lines;
  /** @type {number} */ _numLines;
  /** @type {number} */ _bufferedBytes;
  /** @type {number} */ _flushIntervalMs;
  /** @type {number} */ _maxBufferLines;
  /** @type {number} */ _maxLossMs;
  /** @type {boolean} */ _autoFlush;

  /** @type {number} */ _totalWrites;
  /** @type {number} */ _totalBytes;
  /** @type {number} */ _totalFlushes;
  /** @type {number} */ _backpressureHits;

  /** @type {boolean} */ _isFlushing;
  /** @type {boolean} */ _closed;
  /** @type {boolean} */ _isRotating;
  /** @type {NodeJS.Timeout|null} */ _timer;
  /** @type {Array<() => void>} */ _drainWaiters;

  /**
   * @param {string} filePath
   * @param {BufferedWriterOptions} [options]
   */
  constructor(filePath, options = {}) {
    this._filePath = filePath;
    this._dir = path.dirname(filePath);
    this._lines = [];
    this._numLines = 0;
    this._bufferedBytes = 0;
    this._flushIntervalMs = options.flushIntervalMs ?? 200;
    this._maxBufferLines = options.maxBufferLines ?? 4096;
    this._maxLossMs = options.maxLossMs ?? 1000;
    this._autoFlush = options.autoFlush ?? true;

    this._totalWrites = 0;
    this._totalBytes = 0;
    this._totalFlushes = 0;
    this._backpressureHits = 0;

    this._isFlushing = false;
    this._closed = false;
    this._isRotating = false;
    this._timer = null;
    this._drainWaiters = [];

    // Ensure output directory exists
    fs.mkdirSync(this._dir, { recursive: true });

    // Open append stream
    this._stream = fs.createWriteStream(this._filePath, {
      flags: 'a',
      encoding: 'utf-8',
      highWaterMark: 64 * 1024,
    });

    this._stream.on('drain', () => {
      this._fireDrainWaiters();
    });

    this._stream.on('error', (err) => {
      console.error(`[BufferedWriter] stream error (${this._filePath}):`, err.message);
    });

    // Auto-flush timer
    if (this._autoFlush) {
      this._timer = setInterval(() => {
        this.flush().catch(err => {
          console.error(`[BufferedWriter] auto-flush error:`, err.message);
        });
      }, this._flushIntervalMs);
      if (this._timer.unref) this._timer.unref();
    }
  }

  // ====== Public API ======

  /**
   * Serialize and buffer an object.
   * @param {Object} obj
   */
  async write(obj) {
    if (this._closed) {
      console.error(`[BufferedWriter] write to closed writer (${this._filePath})`);
      return;
    }

    const line = JSON.stringify(obj) + '\n';
    this._lines.push(line);
    this._numLines++;
    this._bufferedBytes += Buffer.byteLength(line, 'utf-8');
    this._totalWrites++;

    if (this._numLines >= this._maxBufferLines) {
      await this.flush();
    }
  }

  /**
   * Flush buffered lines to disk.
   */
  async flush() {
    if (this._isFlushing || this._numLines === 0 || this._closed) return;
    this._isFlushing = true;

    try {
      const batch = this._lines.splice(0, this._numLines);
      const count = this._numLines;
      const bytes = this._bufferedBytes;
      this._numLines = 0;
      this._bufferedBytes = 0;

      const chunk = batch.join('');
      await new Promise((resolve, reject) => {
        const ok = this._stream.write(chunk, 'utf-8', (err) => {
          if (err) reject(err);
          else resolve();
        });
        if (!ok) {
          this._backpressureHits++;
        }
      });
      this._totalBytes += bytes;
      this._totalFlushes++;
    } finally {
      this._isFlushing = false;
    }
  }

  /**
   * Rotate file: flush, close, rename, reopen.
   */
  async rotate() {
    if (this._closed) return;
    this._isRotating = true;

    try {
      await this.flush();
      await this._closeStream();

      const now = new Date();
      const ext = path.extname(this._filePath);
      const base = this._filePath.slice(0, -ext.length);
      const timestamp = now.toISOString().replace(/[:.]/g, '-').slice(0, 19);
      const archivePath = `${base}_${timestamp}${ext}`;
      await fsp.rename(this._filePath, archivePath);

      this._stream = fs.createWriteStream(this._filePath, {
        flags: 'a',
        encoding: 'utf-8',
        highWaterMark: 64 * 1024,
      });
      this._stream.on('drain', () => { this._fireDrainWaiters(); });
      this._stream.on('error', (err) => {
        console.error(`[BufferedWriter] stream error after rotate (${this._filePath}):`, err.message);
      });

      if (this._numLines > 0) {
        await this.flush();
      }
    } finally {
      this._isRotating = false;
    }
  }

  /**
   * Close writer: final flush and stream end.
   */
  async close() {
    if (this._closed) return;

    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }

    // Flush before marking closed so stream write can proceed
    await this.flush();
    this._closed = true;
    await this._closeStream();
  }

  /** @returns {Object} stats */
  getStats() {
    return {
      bufferedBytes: this._bufferedBytes,
      pendingFlushes: this._numLines,
      totalWrites: this._totalWrites,
      totalBytes: this._totalBytes,
      totalFlushes: this._totalFlushes,
      backpressureHits: this._backpressureHits,
    };
  }

  // ====== Internal helpers ======

  _closeStream() {
    return new Promise((resolve) => {
      if (!this._stream || this._stream.destroyed) { resolve(); return; }
      this._stream.end(() => resolve());
      setTimeout(resolve, 2000);
    });
  }

  _waitForDrain() {
    return new Promise((resolve) => {
      this._drainWaiters.push(resolve);
      setTimeout(() => {
        this._fireDrainWaiters();
      }, 5000);
    });
  }

  _fireDrainWaiters() {
    const waiters = this._drainWaiters;
    this._drainWaiters = [];
    for (const cb of waiters) cb();
  }
}
