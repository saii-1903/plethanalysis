#!/usr/bin/env python3
"""
PlethAnalysis Web Server.

Serves the real-time waveform page, manages BLE connection to the
BerryMed watch, processes PPG signals, and saves data to JSON.

Usage:
    python server.py                # web-only (use Connect button in browser)
    python server.py --connect      # auto-connect to watch on startup
    python server.py --simulate     # synthetic data, no hardware needed
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import random
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, List, Optional, Set

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from config import SETTINGS
from dataparse import DataParser, NUM_ADC_SAMPLES
from datasave import JSONRecorder
from bleak import BleakScanner
from pair import BLEManager, ConnectionFailed, DiscoveryTimeout
from singalProcess import PPGProcessor

logger = logging.getLogger("server")

# ---------------------------------------------------------------------------
# JSON recorder — Lifesigns v1.1 format (one record per packet, JSONL file)
# ---------------------------------------------------------------------------

_json_recorder: JSONRecorder = JSONRecorder()

# ---------------------------------------------------------------------------
# BLE pipeline — manager, parser, processor (all singletons)
# ---------------------------------------------------------------------------

_ble_manager   = BLEManager()
_data_parser   = DataParser()
_ble_processor = PPGProcessor(fs=200.0)   # dedicated to live BLE path

# Live BLE connection state (read by /ble/status endpoint)
_ble_status: dict = {
    "state":            "idle",   # idle | scanning | connecting | connected | error | stopped
    "address":          None,
    "device_name":      None,
    "error":            None,
    "packets_received": 0,
    "packets_saved":    0,
}
_ble_task: Optional[asyncio.Task] = None

# Active target address — overrideable per session from /ble/connect body
_ble_target_address: str = SETTINGS.device_address

# Cache of BLEDevice objects from the last /ble/scan — keyed by uppercase address.
# Reused by _ble_runner so we don't need a second advertisement scan after the
# user picks a device; the BLEDevice handle is still valid for connection.
_scan_device_cache: dict = {}   # {address_upper: BLEDevice}

# ---------------------------------------------------------------------------
# Connected browser clients
# ---------------------------------------------------------------------------

browser_clients: Set[WebSocket] = set()

# ---------------------------------------------------------------------------
# Broadcast helper — push a JSON packet to all browsers
# ---------------------------------------------------------------------------


def broadcast(packet_dict: dict) -> None:
    if not browser_clients:
        return
    dead: Set[WebSocket] = set()
    payload = json.dumps(packet_dict)
    for ws in browser_clients:
        try:
            asyncio.ensure_future(_send_ws(ws, payload))
        except (RuntimeError, Exception):
            dead.add(ws)
    if dead:
        browser_clients.difference_update(dead)


def broadcast_ble_state(state: str, **extra) -> None:
    """Push a BLE state-change event to all browsers immediately."""
    broadcast({"_ble_event": True, "state": state, **extra})


async def _send_ws(ws: WebSocket, payload: str) -> None:
    try:
        await ws.send_text(payload)
    except Exception:
        browser_clients.discard(ws)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

_simulate_mode = False


# ---------------------------------------------------------------------------
# BLE data callback — runs on every raw notification from the watch
# ---------------------------------------------------------------------------

def _on_ble_data(data: bytes) -> None:
    """Called synchronously by bleak on each BLE notification."""
    pkt = _data_parser.feed(data)
    if pkt is None or not pkt.valid:
        return

    _ble_status["packets_received"] += 1

    # ── Signal processing (real-time filters + 30 s block) ────────────────
    proc = _ble_processor.process(pkt.adc_samples)

    # ── Save to Lifesigns v1.1 JSON-Lines (includes accelerometer x/y/z) ──
    saved = _json_recorder.save_packet(pkt)
    if saved:
        _ble_status["packets_saved"] += 1

    # ── Broadcast to all connected browser clients ─────────────────────────
    broadcast({
        "ts":    pkt.timestamp.isoformat(timespec="milliseconds"),
        "pkt":   pkt.pkt_index,
        "spo2":  pkt.spo2,
        "spo2r": pkt.spo2_real,
        "hr":    pkt.pulse_rate,
        "hrr":   pkt.pulse_rate_real,
        "pi":    pkt.perfu_index,
        "pir":   pkt.perfu_index_real,
        # Accelerometer — Lifesigns v1.1 Axis-X/Y/Z, signed int8
        "ax": pkt.axis_x,
        "ay": pkt.axis_y,
        "az": pkt.axis_z,
        "bat":    pkt.battery,
        "status": pkt.status_str,
        # Waveform arrays
        "samples":           pkt.adc_samples,
        "filtered_samples":  proc["filtered1"],
        "filtered_samples2": proc["filtered2"],
        "raw_block":         proc["raw_block"],
        "filt_block":        proc["filt_block"],
    })


# ---------------------------------------------------------------------------
# BLE runner — scans, connects, streams; auto-reconnects on drop
# ---------------------------------------------------------------------------

async def _ble_runner() -> None:
    """
    Persistent async task:
      1. Keeps scanning until the BerryMed watch is seen (no give-up limit)
      2. Connects immediately when found and requests original-waveform mode
      3. Streams packets → _on_ble_data
      4. On disconnect, goes back to scanning automatically
    Cancelled only by /ble/disconnect or server shutdown.
    """
    while True:
        try:
            # ── SCAN until the watch appears ──────────────────────────────
            _ble_status.update({"state": "scanning", "error": None})
            broadcast_ble_state("scanning")

            # Update SETTINGS so pair.py reads the right address
            SETTINGS.device_address = _ble_target_address

            addr_upper = _ble_target_address.upper()

            # ── Fast path: reuse device from the modal scan cache ─────────
            if addr_upper in _scan_device_cache:
                cached = _scan_device_cache.pop(addr_upper)
                _ble_manager._device = cached
                logger.info("[BLE] Using cached device %s [%s] — skipping re-scan",
                            cached.name, cached.address)
            else:
                # ── Slow path: scan until the watch is seen ───────────────
                logger.info("[BLE] Scanning for %s …", _ble_target_address)
                while True:
                    try:
                        await _ble_manager.scan(timeout=SETTINGS.scan_timeout)
                        break
                    except DiscoveryTimeout:
                        logger.info("[BLE] Not seen yet — rescanning …")
                        await asyncio.sleep(1)

            # ── CONNECT ───────────────────────────────────────────────────
            _ble_status["state"] = "connecting"
            dev_name = getattr(_ble_manager._device, "name", None) or _ble_target_address
            broadcast_ble_state("connecting", device_name=dev_name)
            logger.info("[BLE] Watch found (%s) — connecting …", dev_name)
            await _ble_manager.connect()

            # Request original (unfiltered) ADC waveform from the device
            cmd = _data_parser.send_command(SETTINGS.cmd_original_wave)
            await _ble_manager.send_command(cmd)

            _ble_manager.on_data(_on_ble_data)
            await _ble_manager.start_stream()

            _ble_status.update({
                "state":       "connected",
                "address":     _ble_target_address,
                "device_name": dev_name,
            })
            broadcast_ble_state("connected",
                                device_name=dev_name, address=_ble_target_address)
            logger.info("[BLE] Connected — streaming data")

            # ── STREAM — keep alive until watch drops ─────────────────────
            while _ble_manager.connected:
                await asyncio.sleep(0.5)

            logger.warning("[BLE] Watch disconnected — going back to scan")
            _ble_status["state"] = "scanning"
            broadcast_ble_state("disconnected")
            await asyncio.sleep(SETTINGS.reconnect_delay)

        except ConnectionFailed as exc:
            msg = str(exc)
            _ble_status.update({"state": "error", "error": msg})
            broadcast_ble_state("error", error=msg)
            logger.warning("[BLE] Connection failed: %s — retrying scan in %ds",
                           msg, SETTINGS.reconnect_delay)
            await asyncio.sleep(SETTINGS.reconnect_delay)

        except asyncio.CancelledError:
            logger.info("[BLE] Runner cancelled")
            break

        except Exception as exc:
            msg = str(exc)
            _ble_status.update({"state": "error", "error": msg})
            broadcast_ble_state("error", error=msg)
            logger.error("[BLE] Unexpected error: %s", exc, exc_info=True)
            await asyncio.sleep(SETTINGS.reconnect_delay)

    _ble_status["state"] = "idle"
    broadcast_ble_state("idle")
    logger.info("[BLE] Runner stopped")


@asynccontextmanager
async def lifespan(app: FastAPI):
    bg_task = None
    if _simulate_mode:
        bg_task = asyncio.create_task(_run_simulator())
    logger.info("Web server ready")
    yield
    # clean up simulator
    if bg_task:
        bg_task.cancel()
        try:
            await bg_task
        except asyncio.CancelledError:
            pass
    # clean up BLE runner
    global _ble_task
    if _ble_task and not _ble_task.done():
        _ble_task.cancel()
        try:
            await _ble_task
        except asyncio.CancelledError:
            pass
    await _ble_manager.disconnect()
    _json_recorder.close()
    logger.info("Web server stopped")


app = FastAPI(lifespan=lifespan)

# ---------------------------------------------------------------------------
# HTML page (embedded)
# ---------------------------------------------------------------------------

HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Pleth Analysis — Multi-Mode Digital Monitor</title>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html, body { width: 100%; height: 100%; overflow: hidden;
  background: #040406; color: #f0f0f5; font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; }

#app { display: flex; flex-direction: column; height: 100vh; }

#dash { display: flex; align-items: center; gap: 16px; background: #07070a;
  padding: 10px 24px; border-bottom: 1px solid #14141a; flex-shrink: 0; height: 80px; }

.dash-item { display: flex; flex-direction: column; align-items: flex-start;
  justify-content: center; min-width: 120px; padding: 6px 16px;
  background: rgba(255, 255, 255, 0.015); border: 1px solid rgba(255, 255, 255, 0.04);
  border-radius: 8px; transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1); }

.dash-item:hover { background: rgba(255, 255, 255, 0.035); border-color: rgba(255, 255, 255, 0.08);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.5); transform: translateY(-1px); }

.dash-label { font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 1.5px;
  color: #7a7a85; margin-bottom: 2px; }

.dash-value { font-size: 26px; font-weight: 700; font-variant-numeric: tabular-nums;
  letter-spacing: -0.5px; line-height: 1; }

.dash-value.spo2 { color: #00d2ff; text-shadow: 0 0 10px rgba(0, 210, 255, 0.2); }
.dash-value.hr   { color: #00ff66; text-shadow: 0 0 10px rgba(0, 255, 102, 0.2); }
.dash-value.pi   { color: #bf5af2; text-shadow: 0 0 10px rgba(191, 90, 242, 0.2); }
.dash-value.bat  { color: #ff9f0a; text-shadow: 0 0 10px rgba(255, 159, 10, 0.2); }

.dash-unit { font-size: 12px; color: #5a5a65; font-weight: 500; margin-left: 2px; }

#tab-bar { display: flex; gap: 8px; margin-left: 16px; }
.tab-btn { background: rgba(255, 255, 255, 0.02); color: #7a7a85; border: 1px solid rgba(255, 255, 255, 0.05);
  padding: 8px 16px; border-radius: 8px; font-size: 11px; font-weight: 700; text-transform: uppercase;
  letter-spacing: 1px; cursor: pointer; transition: all 0.2s ease; font-family: inherit; }
.tab-btn:hover { background: rgba(255, 255, 255, 0.05); color: #f0f0f5; border-color: rgba(255, 255, 255, 0.1); }
.tab-btn.active { background: rgba(0, 210, 255, 0.1); color: #00d2ff; border-color: #00d2ff;
  box-shadow: 0 0 12px rgba(0, 210, 255, 0.15); }

/* ── BLE Connect button ─────────────────────────────────────────────────── */
#btn-ble {
  display: flex; align-items: center; gap: 8px;
  padding: 9px 18px; border-radius: 8px; border: 1px solid #00d2ff;
  background: rgba(0, 210, 255, 0.08); color: #00d2ff;
  font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px;
  cursor: pointer; font-family: inherit; transition: all 0.25s ease;
  white-space: nowrap; margin-left: 8px; }
#btn-ble:hover:not(:disabled) { background: rgba(0, 210, 255, 0.18);
  box-shadow: 0 0 14px rgba(0, 210, 255, 0.25); }
#btn-ble:disabled { opacity: 0.55; cursor: default; }
#btn-ble.state-connected {
  border-color: #00ff66; background: rgba(0, 255, 102, 0.08); color: #00ff66; }
#btn-ble.state-connected:hover { background: rgba(0, 255, 102, 0.18);
  box-shadow: 0 0 14px rgba(0, 255, 102, 0.25); }
#btn-ble.state-busy {
  border-color: #ff9f0a; background: rgba(255, 159, 10, 0.08); color: #ff9f0a; }
#btn-ble.state-error {
  border-color: #ff453a; background: rgba(255, 69, 58, 0.08); color: #ff453a; }

#ble-dot { width: 8px; height: 8px; border-radius: 50%; background: #00d2ff;
  flex-shrink: 0; transition: background 0.3s; }
#btn-ble.state-connected  #ble-dot { background: #00ff66; box-shadow: 0 0 6px #00ff66; }
#btn-ble.state-busy       #ble-dot { background: #ff9f0a;
  animation: blink 0.8s ease-in-out infinite; }
#btn-ble.state-error      #ble-dot { background: #ff453a; }
@keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.2} }

#ble-pkts { font-size: 10px; color: #7a7a85; font-weight: 500;
  font-variant-numeric: tabular-nums; min-width: 48px; text-align: right; }

.dash-status { display: flex; align-items: center; gap: 8px; margin-left: auto;
  padding: 8px 16px; background: rgba(255, 255, 255, 0.015); border: 1px solid rgba(255, 255, 255, 0.04);
  border-radius: 8px; font-size: 13px; font-weight: 500; }

.status-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.status-dot.ok           { background: #00ff66; box-shadow: 0 0 8px #00ff66cc; }
.status-dot.warn         { background: #ff453a; box-shadow: 0 0 8px #ff453acc; }
.status-dot.disconnected { background: #3a3a3c; box-shadow: none; }

#plot-wrap { flex: 1; display: flex; flex-direction: column; gap: 6px; padding: 6px;
  background: #020203; overflow: hidden; position: relative; }

.tab-content { display: flex; flex-direction: column; gap: 6px; flex: 1; height: 100%; overflow: hidden; }

.plot-pane { flex: 1; position: relative; background: #040406; border: 1px solid #14141a;
  border-radius: 8px; overflow: hidden; }
.plot-pane canvas { display: block; width: 100%; height: 100%; }

.pane-title { position: absolute; top: 8px; left: 12px; font-size: 10px; font-weight: 700;
  text-transform: uppercase; letter-spacing: 1.2px; color: #7a7a85; z-index: 10;
  pointer-events: none; background: rgba(4, 4, 6, 0.75); padding: 3px 8px; border-radius: 4px;
  border: 1px solid rgba(255, 255, 255, 0.03); backdrop-filter: blur(4px); }

#controls { position: absolute; top: 16px; right: 24px; display: flex; gap: 8px; z-index: 20; }
#controls button { background: rgba(15, 15, 20, 0.85); color: #8e8e93; border: 1px solid #2c2c35;
  padding: 6px 16px; border-radius: 6px; font-size: 12px; font-weight: 600; cursor: pointer;
  font-family: inherit; letter-spacing: 0.5px; backdrop-filter: blur(8px); transition: all 0.2s ease; }
#controls button:hover  { background: #1c1c24; color: #f0f0f5; border-color: #48485a; }
#controls button.active { background: rgba(0, 255, 102, 0.1); color: #00ff66; border-color: #00ff66; }

/* ── Device picker modal ────────────────────────────────────────────────── */
#modal-overlay {
  display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.72);
  z-index: 100; align-items: center; justify-content: center; }
#modal-overlay.open { display: flex; }
#modal-box {
  background: #0d0d12; border: 1px solid #2a2a35; border-radius: 14px;
  padding: 24px 28px; min-width: 420px; max-width: 560px; width: 90%;
  box-shadow: 0 24px 60px rgba(0,0,0,0.7); }
#modal-title { font-size: 14px; font-weight: 700; text-transform: uppercase;
  letter-spacing: 1.2px; color: #f0f0f5; margin-bottom: 6px; }
#modal-sub   { font-size: 12px; color: #7a7a85; margin-bottom: 18px; }
#modal-scan-btn {
  width: 100%; padding: 10px; border-radius: 8px; border: 1px solid #00d2ff;
  background: rgba(0,210,255,0.08); color: #00d2ff; font-size: 12px;
  font-weight: 700; text-transform: uppercase; letter-spacing: 1px;
  cursor: pointer; font-family: inherit; transition: all 0.2s;
  margin-bottom: 14px; }
#modal-scan-btn:hover:not(:disabled) { background: rgba(0,210,255,0.18); }
#modal-scan-btn:disabled { opacity: 0.5; cursor: default; }
#device-list { max-height: 280px; overflow-y: auto; display: flex; flex-direction: column; gap: 6px; }
.dev-row {
  display: flex; align-items: center; gap: 12px; padding: 10px 14px;
  border-radius: 8px; border: 1px solid #1e1e28; cursor: pointer;
  transition: all 0.18s; background: rgba(255,255,255,0.02); }
.dev-row:hover { background: rgba(255,255,255,0.06); border-color: #3a3a48; }
.dev-row.berry { border-color: rgba(0,210,255,0.3); background: rgba(0,210,255,0.05); }
.dev-row.berry:hover { background: rgba(0,210,255,0.12); border-color: #00d2ff; }
.dev-icon { font-size: 18px; flex-shrink: 0; }
.dev-info { flex: 1; min-width: 0; }
.dev-name { font-size: 13px; font-weight: 600; color: #f0f0f5;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.dev-addr { font-size: 11px; color: #7a7a85; font-variant-numeric: tabular-nums; margin-top: 2px; }
.dev-badge { font-size: 10px; font-weight: 700; padding: 2px 8px; border-radius: 4px;
  background: rgba(0,210,255,0.15); color: #00d2ff; flex-shrink: 0; }
#modal-status { font-size: 12px; color: #7a7a85; text-align: center; margin-top: 12px; min-height: 18px; }
#modal-close-btn {
  margin-top: 16px; width: 100%; padding: 8px; border-radius: 8px;
  border: 1px solid #2a2a35; background: transparent; color: #7a7a85;
  font-size: 12px; cursor: pointer; font-family: inherit; transition: all 0.2s; }
#modal-close-btn:hover { background: rgba(255,255,255,0.04); color: #f0f0f5; }
</style>
</head>
<body>
<div id="app">
  <div id="dash">
    <div class="dash-item">
      <span class="dash-label">SpO₂</span>
      <span class="dash-value spo2"><span id="v-spo2">--</span><span class="dash-unit">%</span></span>
    </div>
    <div class="dash-item">
      <span class="dash-label">Heart Rate</span>
      <span class="dash-value hr"><span id="v-hr">--</span><span class="dash-unit">bpm</span></span>
    </div>
    <div class="dash-item">
      <span class="dash-label">Perfusion Idx</span>
      <span class="dash-value pi"><span id="v-pi">--</span><span class="dash-unit">‰</span></span>
    </div>
    <div class="dash-item">
      <span class="dash-label">Battery</span>
      <span class="dash-value bat"><span id="v-bat">--</span><span class="dash-unit">%</span></span>
    </div>

    <!-- Tab Controls -->
    <div id="tab-bar">
      <button class="tab-btn active" id="btn-tab1" onclick="setTab('tab1')">Live Sweep Monitor</button>
      <button class="tab-btn"        id="btn-tab2" onclick="setTab('tab2')">30s Block View</button>
    </div>

    <!-- BLE Connect / Disconnect button -->
    <button id="btn-ble" onclick="bleButtonClick()">
      <span id="ble-dot"></span>
      <span id="ble-label">Connect Watch</span>
      <span id="ble-pkts"></span>
    </button>

    <div class="dash-status" id="dash-status">
      <span class="status-dot disconnected" id="status-dot"></span>
      <span id="status-text">Waiting for data</span>
    </div>
  </div>

  <!-- Device picker modal -->
  <div id="modal-overlay" onclick="if(event.target===this)closeModal()">
    <div id="modal-box">
      <div id="modal-title">Select BerryMed Device</div>
      <div id="modal-sub">Scan for nearby BLE devices and tap one to connect.</div>
      <button id="modal-scan-btn" onclick="modalScan()">&#x1F50D; Scan for Devices</button>
      <div id="device-list"><div id="modal-status">Press scan to search for devices.</div></div>
      <button id="modal-close-btn" onclick="closeModal()">Cancel</button>
    </div>
  </div>

  <div id="plot-wrap">
    <!-- TAB 1: LIVE SWEEP MONITOR -->
    <div id="tab1-content" class="tab-content">
      <div class="plot-pane">
        <div class="pane-title">Unfiltered Waveform (Raw)</div>
        <canvas id="waveform-raw"></canvas>
      </div>
      <div class="plot-pane">
        <div class="pane-title">Filtered Waveform (PPG Butterworth: 2nd Order, 0.5Hz – 8.0Hz)</div>
        <canvas id="waveform-filtered"></canvas>
      </div>
      <div class="plot-pane">
        <div class="pane-title">Filtered Waveform (PPG Butterworth: 4th Order, 0.5Hz – 40.0Hz)</div>
        <canvas id="waveform-filtered2"></canvas>
      </div>
    </div>

    <!-- TAB 2: 30S BLOCK PROCESSOR VIEW -->
    <div id="tab2-content" class="tab-content" style="display: none;">
      <div class="plot-pane">
        <div class="pane-title">30-Second Historical Buffer (Raw ADC)</div>
        <canvas id="block-raw"></canvas>
      </div>
      <div class="plot-pane">
        <div class="pane-title">30-Second Block-Filtered (Zero-Phase Bandpass, 0.5Hz – 40.0Hz)</div>
        <canvas id="block-filtered"></canvas>
      </div>
    </div>

    <div id="controls">
      <button id="btn-pause">Pause</button>
    </div>
  </div>
</div>

<script>
(function() {
'use strict';

// ── Canvas setup ────────────────────────────────────────────────────────────
const canvasRaw      = document.getElementById('waveform-raw');
const canvasFilt     = document.getElementById('waveform-filtered');
const canvasFilt2    = document.getElementById('waveform-filtered2');
const canvasBlockRaw = document.getElementById('block-raw');
const canvasBlockFilt= document.getElementById('block-filtered');

const ctx_raw   = canvasRaw.getContext('2d');
const ctx_filt  = canvasFilt.getContext('2d');
const ctx_filt2 = canvasFilt2.getContext('2d');

let W = 0, H = 0;
let currentTab = 'tab1';
let cached_raw_block = [];
let cached_filt_block = [];

function resize() {
  const firstPane = document.querySelector('.plot-pane');
  if (!firstPane) return;
  const rect = firstPane.getBoundingClientRect();
  W = rect.width; H = rect.height;
  if (W > 0 && H > 0) {
    canvasRaw.width   = W; canvasRaw.height   = H;
    canvasFilt.width  = W; canvasFilt.height  = H;
    canvasFilt2.width = W; canvasFilt2.height = H;
    if (canvasBlockRaw)  { canvasBlockRaw.width  = W; canvasBlockRaw.height  = H; }
    if (canvasBlockFilt) { canvasBlockFilt.width = W; canvasBlockFilt.height = H; }
    if (currentTab === 'tab2') drawBlockPlots();
  }
}
window.addEventListener('resize', resize);

window.setTab = function(tabId) {
  currentTab = tabId;
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('btn-' + tabId).classList.add('active');
  document.getElementById('tab1-content').style.display = tabId === 'tab1' ? 'flex' : 'none';
  document.getElementById('tab2-content').style.display = tabId === 'tab2' ? 'flex' : 'none';
  if (tabId === 'tab2') drawBlockPlots();
  resize();
};

// ── Sweep state ─────────────────────────────────────────────────────────────
const N = 960;
const erase_gap = 40;
const raw_sweep_data      = new Array(N).fill(null);
const filtered_sweep_data = new Array(N).fill(null);
const filtered_sweep_data2= new Array(N).fill(null);
let write_idx = 0;

let rawQueue = [], filtQueue = [], filtQueue2 = [];
let paused = false;

// ── WebSocket (browser ↔ server) ─────────────────────────────────────────────
const wsUrl = (location.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + location.host + '/ws';
let ws = null;

function connectWS() {
  ws = new WebSocket(wsUrl);
  ws.onopen = () => {
    document.getElementById('status-dot').className = 'status-dot ok';
    document.getElementById('status-text').textContent = 'Normal';
  };
  ws.onmessage = (e) => {
    try {
      const pkt = JSON.parse(e.data);

      // ── BLE state-change event (no waveform data) ──────────────────────
      if (pkt._ble_event) {
        applyBleUI(pkt.state, _blePackets, pkt.device_name || _bleDevName);
        if (pkt.device_name) _bleDevName = pkt.device_name;
        if (pkt.error) showBleError(pkt.error);
        return;
      }

      // ── Normal waveform packet ──────────────────────────────────────────
      if (!paused) {
        updateDashboard(pkt);
        if (pkt.samples && pkt.samples.length) {
          _blePackets++;
          for (let i = 0; i < pkt.samples.length; i++) {
            rawQueue.push(pkt.samples[i]);
            filtQueue.push(pkt.filtered_samples  ? pkt.filtered_samples[i]  : pkt.samples[i]);
            filtQueue2.push(pkt.filtered_samples2 ? pkt.filtered_samples2[i] : pkt.samples[i]);
          }
        }
        if (pkt.raw_block && pkt.raw_block.length) {
          cached_raw_block  = pkt.raw_block;
          cached_filt_block = pkt.filt_block;
          if (currentTab === 'tab2') drawBlockPlots();
        }
      }
    } catch(_) {}
  };
  ws.onclose = () => {
    ws = null;
    document.getElementById('status-dot').className = 'status-dot disconnected';
    document.getElementById('status-text').textContent = 'Disconnected';
    setTimeout(connectWS, 1000);
  };
  ws.onerror = () => { ws && ws.close(); };
}
connectWS();

function updateDashboard(pkt) {
  const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v != null ? v : '--'; };
  set('v-spo2', pkt.spo2);
  set('v-hr',   pkt.hr);
  set('v-pi',   pkt.pi);
  set('v-bat',  pkt.bat);
}

// ── BLE state & button ───────────────────────────────────────────────────────
let _bleState   = 'idle';
let _blePackets = 0;
let _bleDevName = '';

function showBleError(msg) {
  const el = document.getElementById('status-text');
  if (el) { el.textContent = '⚠ ' + msg; el.style.color='#ff453a'; }
  setTimeout(() => { if(el){el.style.color='';} }, 5000);
}

const BLE_UI = {
  idle:        { label: 'Connect Watch',  cls: '',               disabled: false },
  scanning:    { label: 'Scanning…', cls: 'state-busy',     disabled: true  },
  connecting:  { label: 'Connecting…',cls:'state-busy',     disabled: true  },
  connected:   { label: 'Disconnect',     cls: 'state-connected',disabled: false },
  disconnected:{ label: 'Connect Watch',  cls: '',               disabled: false },
  error:       { label: 'Retry Connect',  cls: 'state-error',    disabled: false },
  stopped:     { label: 'Connect Watch',  cls: '',               disabled: false },
};

function applyBleUI(state, pkts, devName) {
  _bleState = state;
  const cfg = BLE_UI[state] || BLE_UI.idle;
  const btn = document.getElementById('btn-ble');
  btn.setAttribute('class', cfg.cls || '');
  btn.id = 'btn-ble';
  btn.disabled = cfg.disabled;
  document.getElementById('ble-label').textContent = cfg.label;
  const pktsEl = document.getElementById('ble-pkts');
  if (state === 'connected' && pkts != null) {
    pktsEl.textContent = (devName ? devName + ' · ' : '') + pkts + ' pkts';
  } else {
    pktsEl.textContent = '';
  }
}

// Called when the main button is clicked
window.bleButtonClick = async function() {
  if (_bleState === 'connected') {
    // Disconnect immediately
    await fetch('/ble/disconnect', { method: 'POST' });
    applyBleUI('idle', 0);
  } else if (_bleState === 'scanning' || _bleState === 'connecting') {
    // do nothing while busy
  } else {
    // Open device picker modal
    openModal();
  }
};

// ── Device picker modal ───────────────────────────────────────────────────────
function openModal() {
  document.getElementById('modal-overlay').classList.add('open');
}
window.closeModal = function() {
  document.getElementById('modal-overlay').classList.remove('open');
};

window.modalScan = async function() {
  const scanBtn  = document.getElementById('modal-scan-btn');
  const listEl   = document.getElementById('device-list');
  const statusEl = document.getElementById('modal-status');

  scanBtn.disabled = true;
  scanBtn.textContent = '⏳ Scanning (8s)…';
  listEl.innerHTML = '';
  statusEl.textContent = 'Scanning for nearby BLE devices…';

  try {
    const r = await fetch('/ble/scan?timeout=8');
    const data = await r.json();

    listEl.innerHTML = '';
    if (!data.ok) {
      statusEl.textContent = 'Scan error: ' + (data.error || 'unknown');
    } else if (data.devices.length === 0) {
      statusEl.textContent = 'No devices found. Make sure the watch is powered on.';
    } else {
      statusEl.textContent = data.devices.length + ' device(s) found. Tap one to connect.';
      data.devices.forEach(dev => {
        const row = document.createElement('div');
        row.className = 'dev-row' + (dev.is_berrymed ? ' berry' : '');
        row.innerHTML =
          '<span class="dev-icon">' + (dev.is_berrymed ? '💚' : '📶') + '</span>' +
          '<div class="dev-info">' +
            '<div class="dev-name">' + escHtml(dev.name) + '</div>' +
            '<div class="dev-addr">' + escHtml(dev.address) + '</div>' +
          '</div>' +
          (dev.is_berrymed ? '<span class="dev-badge">BerryMed</span>' : '');
        row.addEventListener('click', () => connectToDevice(dev.address, dev.name));
        listEl.appendChild(row);
      });
    }
  } catch(e) {
    statusEl.textContent = 'Scan failed: ' + e;
  } finally {
    scanBtn.disabled = false;
    scanBtn.textContent = '🔍 Scan Again';
  }
};

async function connectToDevice(address, name) {
  document.getElementById('modal-status').textContent =
    'Connecting to ' + name + '…';
  document.getElementById('modal-scan-btn').disabled = true;

  // Disable all device rows while connecting
  document.querySelectorAll('.dev-row').forEach(r => r.style.pointerEvents = 'none');

  const r = await fetch('/ble/connect', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ address: address }),
  });
  const d = await r.json();
  applyBleUI(d.status ? d.status.state : 'scanning', 0, name);
  closeModal();
}

function escHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// Poll BLE status every 2 s and keep button in sync
async function pollBleStatus() {
  try {
    const r = await fetch('/ble/status');
    const s = await r.json();
    applyBleUI(s.state, s.packets_received, s.device_name);
  } catch(_) {}
}
setInterval(pollBleStatus, 2000);
pollBleStatus();

// ── Auto-scaling ─────────────────────────────────────────────────────────────
let raw_smoothed_min = 20000, raw_smoothed_max = 45000;
let filt_smoothed_min = -2000, filt_smoothed_max = 2000;
let filt_smoothed_min2 = -2000, filt_smoothed_max2 = 2000;

function updateScales() {
  let rawMin=Infinity,rawMax=-Infinity,filtMin=Infinity,filtMax=-Infinity,filtMin2=Infinity,filtMax2=-Infinity,count=0;
  for (let i=0;i<N;i++){
    const r=raw_sweep_data[i],f1=filtered_sweep_data[i],f2=filtered_sweep_data2[i];
    if(r!==null){if(r<rawMin)rawMin=r;if(r>rawMax)rawMax=r;count++;}
    if(f1!==null){if(f1<filtMin)filtMin=f1;if(f1>filtMax)filtMax=f1;}
    if(f2!==null){if(f2<filtMin2)filtMin2=f2;if(f2>filtMax2)filtMax2=f2;}
  }
  if(count<10)return;
  let pR=(rawMax-rawMin)*0.1||500; rawMin-=pR; rawMax+=pR;
  let pF=(filtMax-filtMin)*0.1||100; filtMin-=pF; filtMax+=pF;
  let pF2=(filtMax2-filtMin2)*0.1||100; filtMin2-=pF2; filtMax2+=pF2;
  raw_smoothed_min+=(rawMin-raw_smoothed_min)*0.05;
  raw_smoothed_max+=(rawMax-raw_smoothed_max)*0.05;
  filt_smoothed_min+=(filtMin-filt_smoothed_min)*0.05;
  filt_smoothed_max+=(filtMax-filt_smoothed_max)*0.05;
  filt_smoothed_min2+=(filtMin2-filt_smoothed_min2)*0.05;
  filt_smoothed_max2+=(filtMax2-filt_smoothed_max2)*0.05;
}

function scaleY_raw(v){
  let r=raw_smoothed_max-raw_smoothed_min;if(r<=0)r=1;
  const pH=H-20,pad=pH*0.15,dH=pH*0.70;
  return pH-pad-((v-raw_smoothed_min)/r)*dH;
}
function scaleY_filt(v){
  let m=Math.max(Math.abs(filt_smoothed_min),Math.abs(filt_smoothed_max));if(m<=0)m=100;
  const pH=H-20,pad=pH*0.15,dH=pH*0.70;
  return pH-pad-((v-(-m))/(2*m))*dH;
}
function scaleY_filt2(v){
  let m=Math.max(Math.abs(filt_smoothed_min2),Math.abs(filt_smoothed_max2));if(m<=0)m=100;
  const pH=H-20,pad=pH*0.15,dH=pH*0.70;
  return pH-pad-((v-(-m))/(2*m))*dH;
}

// ── Grid drawing ─────────────────────────────────────────────────────────────
function drawGridAndXAxis(ctx) {
  const pH=H-20;
  ctx.strokeStyle='#14141a';ctx.lineWidth=1;
  ctx.beginPath();ctx.moveTo(0,pH);ctx.lineTo(W,pH);ctx.stroke();
  const n=20,sw=W/n;
  ctx.font="9px 'Segoe UI',system-ui";ctx.fillStyle='#5a5a65';
  ctx.textAlign='center';ctx.textBaseline='top';
  for(let i=0;i<=n;i++){
    const x=i*sw;
    if(i>0&&i<n){
      ctx.beginPath();ctx.moveTo(x,0);ctx.lineTo(x,pH);
      ctx.strokeStyle=i%2===0?'rgba(255,255,255,0.03)':'rgba(255,255,255,0.01)';
      ctx.stroke();
    }
    if(i%2===0) ctx.fillText((i*0.24).toFixed(2)+'s',x,pH+5);
  }
  ctx.strokeStyle='rgba(255,255,255,0.01)';
  for(let i=1;i<6;i++){const y=(i/6)*pH;ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(W,y);ctx.stroke();}
}

// ── 30s block plots ──────────────────────────────────────────────────────────
function drawBlockPlots() {
  if (!canvasBlockRaw || !canvasBlockFilt) return;
  const cb = canvasBlockRaw.getContext('2d');
  const cf = canvasBlockFilt.getContext('2d');
  cb.fillStyle='#040406'; cb.fillRect(0,0,W,H);
  cf.fillStyle='#040406'; cf.fillRect(0,0,W,H);

  function grid30(ctx){
    const pH=H-20;
    ctx.strokeStyle='#14141a';ctx.lineWidth=1;
    ctx.beginPath();ctx.moveTo(0,pH);ctx.lineTo(W,pH);ctx.stroke();
    const n=6,sw=W/n;
    ctx.font="9px 'Segoe UI',system-ui";ctx.fillStyle='#5a5a65';
    ctx.textAlign='center';ctx.textBaseline='top';
    for(let i=0;i<=n;i++){
      const x=i*sw;
      if(i>0&&i<n){ctx.beginPath();ctx.moveTo(x,0);ctx.lineTo(x,pH);
        ctx.strokeStyle='rgba(255,255,255,0.03)';ctx.stroke();}
      ctx.fillText((i*5)+'s',x,pH+5);
    }
    ctx.strokeStyle='rgba(255,255,255,0.01)';
    for(let i=1;i<6;i++){const y=(i/6)*pH;ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(W,y);ctx.stroke();}
  }
  grid30(cb); grid30(cf);

  if (!cached_raw_block || cached_raw_block.length < 400) {
    const s = cached_raw_block ? (cached_raw_block.length/200).toFixed(1) : '0.0';
    cb.font='14px Segoe UI';cb.fillStyle='#00d2ff';cb.textAlign='center';
    cb.fillText('Accumulating buffer… ('+s+'s / 2.0s)',W/2,H/2);
    cf.font='14px Segoe UI';cf.fillStyle='#7a7a85';cf.textAlign='center';
    cf.fillText('Waiting for 2s minimum…',W/2,H/2);
    return;
  }

  const len=cached_raw_block.length, pH=H-20, pad=pH*0.1, dH=pH*0.8;
  const rMin=Math.min(...cached_raw_block), rMax=Math.max(...cached_raw_block);
  let rR=rMax-rMin; if(rR<=0)rR=1;
  cb.beginPath();cb.strokeStyle='#00d2ff';cb.lineWidth=1.5;
  cb.shadowColor='rgba(0,210,255,0.15)';cb.shadowBlur=4;
  for(let i=0;i<len;i++){
    const x=(i/(len-1))*W, y=pH-pad-((cached_raw_block[i]-rMin)/rR)*dH;
    i===0?cb.moveTo(x,y):cb.lineTo(x,y);
  }
  cb.stroke();cb.shadowBlur=0;
  cb.font='10px Segoe UI';cb.fillStyle='#00d2ff';cb.textAlign='right';cb.textBaseline='top';
  cb.fillText('Buffer: '+(len/200).toFixed(1)+'s / 30.0s',W-12,12);

  if (cached_filt_block && cached_filt_block.length) {
    const fMin=Math.min(...cached_filt_block), fMax=Math.max(...cached_filt_block);
    let fR=fMax-fMin; if(fR<=0)fR=1;
    cf.beginPath();cf.strokeStyle='#00ff66';cf.lineWidth=2;
    cf.shadowColor='rgba(0,255,102,0.3)';cf.shadowBlur=6;
    for(let i=0;i<cached_filt_block.length;i++){
      const x=(i/(cached_filt_block.length-1))*W, y=pH-pad-((cached_filt_block[i]-fMin)/fR)*dH;
      i===0?cf.moveTo(x,y):cf.lineTo(x,y);
    }
    cf.stroke();cf.shadowBlur=0;
    cf.font='10px Segoe UI';cf.fillStyle='#00ff66';cf.textAlign='right';cf.textBaseline='top';
    cf.fillText('Buffer: '+(len/200).toFixed(1)+'s / 30.0s',W-12,12);
  } else {
    cf.font='14px Segoe UI';cf.fillStyle='#7a7a85';cf.textAlign='center';
    cf.fillText('Processing zero-phase filter…',W/2,H/2);
  }
}

// ── Animation loop ───────────────────────────────────────────────────────────
let lastFrameTime=performance.now(), samplesAcc=0;
const sampleRate=200;

function animationLoop(ts) {
  requestAnimationFrame(animationLoop);
  if (paused){ lastFrameTime=ts; if(currentTab==='tab1')renderFrame(); return; }
  let elapsed=ts-lastFrameTime; lastFrameTime=ts;
  if(elapsed>100)elapsed=100;
  if(rawQueue.length>2000){
    rawQueue=rawQueue.slice(-200);filtQueue=filtQueue.slice(-200);filtQueue2=filtQueue2.slice(-200);
  }
  samplesAcc+=(elapsed/1000)*sampleRate;
  let n=Math.floor(samplesAcc); samplesAcc-=n;
  for(let k=0;k<n;k++){
    if(rawQueue.length>0&&filtQueue.length>0&&filtQueue2.length>0){
      raw_sweep_data[write_idx]=rawQueue.shift();
      filtered_sweep_data[write_idx]=filtQueue.shift();
      filtered_sweep_data2[write_idx]=filtQueue2.shift();
      for(let g=1;g<=erase_gap;g++){
        raw_sweep_data[(write_idx+g)%N]=null;
        filtered_sweep_data[(write_idx+g)%N]=null;
        filtered_sweep_data2[(write_idx+g)%N]=null;
      }
      write_idx=(write_idx+1)%N;
    } else break;
  }
  if(currentTab==='tab1'){updateScales();renderFrame();}
}

// ── Render ───────────────────────────────────────────────────────────────────
function drawChannel(ctx, data, scaleY, strokeColor, shadowColor, dotRadius) {
  ctx.fillStyle='#040406'; ctx.fillRect(0,0,W,H);
  drawGridAndXAxis(ctx);

  // baseline for filtered channels
  if (shadowColor !== 'rgba(0,210,255,0.3)') {
    const yB=scaleY(0);
    ctx.beginPath();ctx.strokeStyle=strokeColor.replace(')',',0.08)').replace('rgb','rgba');
    ctx.lineWidth=1;ctx.setLineDash([6,6]);
    ctx.moveTo(0,yB);ctx.lineTo(W,yB);ctx.stroke();ctx.setLineDash([]);
  }

  ctx.beginPath();ctx.strokeStyle=strokeColor;ctx.lineWidth=2.5;
  ctx.shadowColor=shadowColor;ctx.shadowBlur=10;
  ctx.lineJoin='round';ctx.lineCap='round';
  let started=false;
  for(let i=0;i<N;i++){
    const v=data[i];
    if(v===null||v===undefined){if(started){ctx.stroke();ctx.beginPath();started=false;}continue;}
    const x=(i/N)*W,y=scaleY(v);
    if(!started){ctx.moveTo(x,y);started=true;}else ctx.lineTo(x,y);
  }
  if(started)ctx.stroke();
  ctx.shadowBlur=0;

  const hi=(write_idx-1+N)%N, hv=data[hi];
  if(hv!==null&&hv!==undefined){
    const hx=(hi/N)*W,hy=scaleY(hv);
    ctx.beginPath();ctx.arc(hx,hy,dotRadius+4,0,2*Math.PI);
    ctx.strokeStyle=strokeColor.replace(')',',0.6)').replace('rgb','rgba');
    ctx.lineWidth=1.5;ctx.stroke();
    ctx.beginPath();ctx.arc(hx,hy,dotRadius,0,2*Math.PI);
    ctx.fillStyle='#ffffff';ctx.shadowColor=strokeColor;ctx.shadowBlur=12;ctx.fill();
    ctx.shadowBlur=0;
  }
}

function renderFrame() {
  drawChannel(ctx_raw,   raw_sweep_data,      scaleY_raw,   '#00d2ff','rgba(0,210,255,0.3)', 3);
  drawChannel(ctx_filt,  filtered_sweep_data,  scaleY_filt,  '#00ff66','rgba(0,255,102,0.45)',4);
  drawChannel(ctx_filt2, filtered_sweep_data2, scaleY_filt2, '#bf5af2','rgba(191,90,242,0.45)',4);
}

// ── Pause button ─────────────────────────────────────────────────────────────
document.getElementById('btn-pause').addEventListener('click', function(){
  paused=!paused;
  this.textContent=paused?'Resume':'Pause';
  this.className=paused?'active':'';
});

resize();
requestAnimationFrame(animationLoop);

})();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/")
async def index():
    return HTMLResponse(HTML_PAGE)


@app.websocket("/ws")
async def ws_browser(websocket: WebSocket):
    await websocket.accept()
    browser_clients.add(websocket)
    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        browser_clients.discard(websocket)


class IncomingPacket(BaseModel):
    ts: str = ""
    pkt: int = 0
    spo2: Optional[int] = None
    spo2r: Optional[int] = None
    hr: Optional[int] = None
    hrr: Optional[int] = None
    pi: Optional[int] = None
    pir: Optional[int] = None
    ax: int = 0
    ay: int = 0
    az: int = 0
    bat: int = 0
    status: List[str] = []
    samples: List[int] = []
    filtered_samples: List[float] = []
    filtered_samples2: List[float] = []
    raw_block: List[float] = []
    filt_block: Optional[List[float]] = None


@app.post("/data")
async def receive_data(pkt: IncomingPacket):
    d = pkt.model_dump()
    _json_recorder.save_dict(d)
    broadcast(d)
    return {"ok": True, "relayed": len(browser_clients), "saved": _json_recorder.records_written}


# ---------------------------------------------------------------------------
# BLE control endpoints
# ---------------------------------------------------------------------------

class BLEConnectRequest(BaseModel):
    address: Optional[str] = None   # override target; falls back to SETTINGS.device_address


@app.post("/ble/connect")
async def ble_connect(req: BLEConnectRequest = BLEConnectRequest()):
    """
    Start scanning for a BerryMed watch and begin streaming.
    Pass {"address": "XX:XX:XX:XX:XX:XX"} to connect to a specific device
    found via /ble/scan; omit to use the address in config.py.
    """
    global _ble_task, _ble_target_address
    if _ble_task and not _ble_task.done():
        return {"ok": False, "reason": "BLE runner already active", "status": _ble_status}

    _ble_target_address = (req.address or SETTINGS.device_address).strip().upper()
    _data_parser.flush()
    _ble_status.update({
        "state":            "scanning",
        "error":            None,
        "packets_received": 0,
        "packets_saved":    0,
    })
    _ble_task = asyncio.create_task(_ble_runner())
    logger.info("[BLE] Connect requested → target %s", _ble_target_address)
    return {"ok": True, "target": _ble_target_address, "status": _ble_status}


@app.get("/ble/scan")
async def ble_scan(timeout: float = 8.0):
    """
    Perform a one-shot BLE scan. Cancels any in-progress (non-connected)
    BLE runner first so Windows doesn't conflict with two simultaneous scans.
    Discovered BLEDevice objects are cached; /ble/connect reuses them directly.
    """
    global _ble_task, _scan_device_cache

    # Stop the runner if it's scanning/connecting (not yet connected) so we
    # don't run two BLE scanners at the same time on the Windows BLE stack.
    if _ble_task and not _ble_task.done():
        if _ble_status.get("state") not in ("connected",):
            logger.info("[BLE] Cancelling runner for ad-hoc scan …")
            _ble_task.cancel()
            try:
                await _ble_task
            except asyncio.CancelledError:
                pass
            _ble_task = None
            _ble_status["state"] = "idle"

    timeout = max(3.0, min(timeout, 20.0))
    logger.info("[BLE] Ad-hoc scan %.0fs …", timeout)

    try:
        raw_devices = await BleakScanner.discover(timeout=timeout, return_adv=False)
    except Exception as exc:
        logger.error("[BLE] Scan error: %s", exc)
        return {"ok": False, "error": str(exc), "devices": []}

    BERRY_KEYWORDS = {"berrymed", "berry", "lifesigns", "niso", "o2m", "spo2"}

    # Cache every discovered device by address so _ble_runner can skip re-scan
    _scan_device_cache = {
        d.address.upper(): d for d in raw_devices
    }

    devices = []
    for d in raw_devices:
        name = d.name or ""
        is_berry = any(kw in name.lower() for kw in BERRY_KEYWORDS)
        devices.append({
            "address":    d.address,
            "name":       name or "(unnamed)",
            "is_berrymed": is_berry,
        })

    devices.sort(key=lambda x: (not x["is_berrymed"], x["name"].lower()))
    logger.info("[BLE] Scan complete — %d devices, %d BerryMed",
                len(devices), sum(1 for d in devices if d["is_berrymed"]))

    return {
        "ok":             True,
        "count":          len(devices),
        "current_target": _ble_target_address,
        "devices":        devices,
    }


@app.post("/ble/disconnect")
async def ble_disconnect():
    """Stop streaming and disconnect from the watch."""
    global _ble_task
    if _ble_task and not _ble_task.done():
        _ble_task.cancel()
        try:
            await _ble_task
        except asyncio.CancelledError:
            pass
    await _ble_manager.disconnect()
    _ble_status["state"] = "idle"
    logger.info("[BLE] Disconnected by user request")
    return {"ok": True, "status": _ble_status}


@app.get("/ble/status")
async def ble_status_endpoint():
    """Return current BLE connection state and packet counters."""
    return {
        **_ble_status,
        "parser_stats":    _data_parser.stats,
        "json_records":    _json_recorder.records_written,
        "browser_clients": len(browser_clients),
    }


# ---------------------------------------------------------------------------
# Standalone simulator (for testing without BLE hardware)
# ---------------------------------------------------------------------------

_sim_processor = PPGProcessor(fs=200.0)


async def _run_simulator():
    t = 0.0
    while True:
        await asyncio.sleep(0.24)
        t += 0.24
        samples = []
        base = math.sin(t * 2.0 * math.pi * 1.5) * 5000
        for i in range(NUM_ADC_SAMPLES):
            phase = (i / NUM_ADC_SAMPLES) * 2.0 * math.pi
            val = int(base + math.sin((t + phase) * 2.0 * math.pi * 0.2) * 8000 + random.gauss(0, 200))
            samples.append(val)

        proc = _sim_processor.process(samples)

        # Simulated accelerometer (gentle noise, gravity on Z)
        sim_ax = max(-128, min(127, int(random.gauss(0, 3))))
        sim_ay = max(-128, min(127, int(random.gauss(0, 3))))
        sim_az = max(-128, min(127, int(random.gauss(63, 5))))

        pkt_dict = {
            "ts":   datetime.now().isoformat(timespec="milliseconds"),
            "pkt":  int(t * 4.17) % 256,
            "spo2": random.randint(95, 100), "spo2r": random.randint(95, 100),
            "hr":   random.randint(60, 80),  "hrr":   random.randint(60, 80),
            "pi":   random.randint(5, 30),   "pir":   random.randint(5, 30),
            "bat":  random.randint(60, 100),
            "status": [],
            "ax": sim_ax, "ay": sim_ay, "az": sim_az,
            "samples":           samples,
            "filtered_samples":  proc["filtered1"],
            "filtered_samples2": proc["filtered2"],
            "raw_block":         proc["raw_block"],
            "filt_block":        proc["filt_block"],
        }
        _json_recorder.save_dict(pkt_dict)
        broadcast(pkt_dict)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    global _simulate_mode
    p = argparse.ArgumentParser(description="PlethAnalysis Web Server")
    p.add_argument("--simulate", action="store_true",
                   help="Use synthetic data (no BLE hardware required)")
    p.add_argument("--connect",  action="store_true",
                   help="Auto-connect to BerryMed watch on startup")
    p.add_argument("--port", type=int, default=8000, help="HTTP port (default 8000)")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    args = p.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format=SETTINGS.log_format)

    _simulate_mode = args.simulate

    if args.connect and not args.simulate:
        @app.on_event("startup")
        async def _auto_connect():
            global _ble_task
            _ble_task = asyncio.create_task(_ble_runner())
            logger.info("[BLE] Auto-connect started (--connect flag)")

    mode = "simulated" if args.simulate else ("BLE auto-connect" if args.connect else "web-only")
    logger.info("PlethAnalysis server  http://localhost:%d  [%s]", args.port, mode)
    logger.info("  Browser   → http://localhost:%d", args.port)
    logger.info("  BLE status→ http://localhost:%d/ble/status", args.port)
    uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
