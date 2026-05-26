#!/usr/bin/env python3
"""
PlethAnalysis Web Server — pure web module.

Serves the real-time waveform HTML page and accepts data
via WebSocket (/ws) or HTTP POST (/data).

Standalone usage (simulated data for testing):
    python server.py --simulate
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
    "state":   "idle",      # idle | scanning | connecting | connected | error | stopped
    "address": None,
    "error":   None,
    "packets_received": 0,
    "packets_saved":    0,
}
_ble_task: Optional[asyncio.Task] = None

# ---------------------------------------------------------------------------
# Connected browser clients (shared with main.py)
# ---------------------------------------------------------------------------

browser_clients: Set[WebSocket] = set()

# ---------------------------------------------------------------------------
# Broadcast helper — push a JSON packet to all browsers
# ---------------------------------------------------------------------------


def broadcast(packet_dict: dict) -> None:
    """Called from main.py or anywhere in-process to push data to all browsers."""
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

    # ── Signal processing (filters + 30 s block) ──────────────────────────
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
        # ── Accelerometer (Lifesigns v1.1 Axis-X/Y/Z, signed int8) ────────
        "ax": pkt.axis_x,
        "ay": pkt.axis_y,
        "az": pkt.axis_z,
        "bat":    pkt.battery,
        "status": pkt.status_str,
        # ── Waveform arrays ────────────────────────────────────────────────
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
    Persistent async task that:
      1. Scans for the BerryMed device (by address from SETTINGS)
      2. Connects and requests original-waveform mode (0xF4)
      3. Streams packets → _on_ble_data callback
      4. On disconnect, waits reconnect_delay seconds and retries
         up to max_reconnect_attempts times
    """
    attempt = 0
    while attempt < SETTINGS.max_reconnect_attempts:
        try:
            _ble_status.update({"state": "scanning", "error": None})
            logger.info("[BLE] Scanning for %s …", SETTINGS.device_address)
            await _ble_manager.scan()

            _ble_status["state"] = "connecting"
            logger.info("[BLE] Connecting …")
            await _ble_manager.connect()

            # Request original (unfiltered) ADC waveform from the device
            cmd = _data_parser.send_command(SETTINGS.cmd_original_wave)
            await _ble_manager.send_command(cmd)

            _ble_manager.on_data(_on_ble_data)
            await _ble_manager.start_stream()

            _ble_status.update({
                "state":   "connected",
                "address": SETTINGS.device_address,
            })
            logger.info("[BLE] Connected — streaming data")
            attempt = 0  # reset counter on successful connection

            # Stay alive until the watch disconnects
            while _ble_manager.connected:
                await asyncio.sleep(0.5)

            logger.warning("[BLE] Watch disconnected")
            _ble_status["state"] = "disconnected"

        except (DiscoveryTimeout, ConnectionFailed) as exc:
            attempt += 1
            msg = str(exc)
            _ble_status.update({"state": "error", "error": msg})
            logger.warning("[BLE] Attempt %d/%d failed: %s", attempt,
                           SETTINGS.max_reconnect_attempts, msg)

        except asyncio.CancelledError:
            logger.info("[BLE] Runner cancelled")
            break

        except Exception as exc:
            attempt += 1
            _ble_status.update({"state": "error", "error": str(exc)})
            logger.error("[BLE] Unexpected error: %s", exc, exc_info=True)

        if attempt < SETTINGS.max_reconnect_attempts:
            logger.info("[BLE] Retrying in %ds …", SETTINGS.reconnect_delay)
            await asyncio.sleep(SETTINGS.reconnect_delay)

    _ble_status["state"] = "stopped"
    logger.info("[BLE] Runner stopped (attempts exhausted or cancelled)")


@asynccontextmanager
async def lifespan(app: FastAPI):
    bg_task = None
    if _simulate_mode:
        bg_task = asyncio.create_task(_run_simulator())
    logger.info("Web server ready")
    yield
    # ── clean up simulator ─────────────────────────────────────────────────
    if bg_task:
        bg_task.cancel()
        try:
            await bg_task
        except asyncio.CancelledError:
            pass
    # ── clean up BLE runner ────────────────────────────────────────────────
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
.dash-value.hr { color: #00ff66; text-shadow: 0 0 10px rgba(0, 255, 102, 0.2); }
.dash-value.pi  { color: #bf5af2; text-shadow: 0 0 10px rgba(191, 90, 242, 0.2); }
.dash-value.bat { color: #ff9f0a; text-shadow: 0 0 10px rgba(255, 159, 10, 0.2); }

.dash-unit { font-size: 12px; color: #5a5a65; font-weight: 500; margin-left: 2px; }

#tab-bar { display: flex; gap: 8px; margin-left: 16px; }
.tab-btn { background: rgba(255, 255, 255, 0.02); color: #7a7a85; border: 1px solid rgba(255, 255, 255, 0.05);
  padding: 8px 16px; border-radius: 8px; font-size: 11px; font-weight: 700; text-transform: uppercase;
  letter-spacing: 1px; cursor: pointer; transition: all 0.2s ease; font-family: inherit; }
.tab-btn:hover { background: rgba(255, 255, 255, 0.05); color: #f0f0f5; border-color: rgba(255, 255, 255, 0.1); }
.tab-btn.active { background: rgba(0, 210, 255, 0.1); color: #00d2ff; border-color: #00d2ff;
  box-shadow: 0 0 12px rgba(0, 210, 255, 0.15); }

.dash-status { display: flex; align-items: center; gap: 8px; margin-left: auto;
  padding: 8px 16px; background: rgba(255, 255, 255, 0.015); border: 1px solid rgba(255, 255, 255, 0.04);
  border-radius: 8px; font-size: 13px; font-weight: 500; }

.status-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.status-dot.ok { background: #00ff66; box-shadow: 0 0 8px #00ff66cc; }
.status-dot.warn { background: #ff453a; box-shadow: 0 0 8px #ff453acc; }
.status-dot.disconnected { background: #3a3a3c; box-shadow: none; }

#plot-wrap { flex: 1; display: flex; flex-direction: column; gap: 6px; padding: 6px; background: #020203; overflow: hidden; position: relative; }

.tab-content { display: flex; flex-direction: column; gap: 6px; flex: 1; height: 100%; overflow: hidden; }

.plot-pane { flex: 1; position: relative; background: #040406; border: 1px solid #14141a; border-radius: 8px; overflow: hidden; }
.plot-pane canvas { display: block; width: 100%; height: 100%; }

.pane-title { position: absolute; top: 8px; left: 12px; font-size: 10px; font-weight: 700;
  text-transform: uppercase; letter-spacing: 1.2px; color: #7a7a85; z-index: 10;
  pointer-events: none; background: rgba(4, 4, 6, 0.75); padding: 3px 8px; border-radius: 4px;
  border: 1px solid rgba(255, 255, 255, 0.03); backdrop-filter: blur(4px); }

#controls { position: absolute; top: 16px; right: 24px; display: flex; gap: 8px; z-index: 20; }
#controls button { background: rgba(15, 15, 20, 0.85); color: #8e8e93; border: 1px solid #2c2c35;
  padding: 6px 16px; border-radius: 6px; font-size: 12px; font-weight: 600; cursor: pointer;
  font-family: inherit; letter-spacing: 0.5px; backdrop-filter: blur(8px);
  transition: all 0.2s ease; }

#controls button:hover { background: #1c1c24; color: #f0f0f5; border-color: #48485a; }
#controls button.active { background: rgba(0, 255, 102, 0.1); color: #00ff66; border-color: #00ff66; }
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
    
    <!-- Tab Controls inside Dashboard Header -->
    <div id="tab-bar">
      <button class="tab-btn active" id="btn-tab1" onclick="setTab('tab1')">Live Sweep Monitor</button>
      <button class="tab-btn" id="btn-tab2" onclick="setTab('tab2')">30s Block Processed View</button>
    </div>

    <div class="dash-status" id="dash-status">
      <span class="status-dot disconnected" id="status-dot"></span>
      <span id="status-text">Waiting for data</span>
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
        <div class="pane-title">Filtered Waveform (PPG Butterworth: 2nd Order, 0.5Hz - 8.0Hz)</div>
        <canvas id="waveform-filtered"></canvas>
      </div>
      <div class="plot-pane">
        <div class="pane-title">Filtered Waveform (PPG Butterworth: 4th Order, 0.5Hz - 40.0Hz)</div>
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
        <div class="pane-title">30-Second Block-Filtered (Zero-Phase Bandpass, 0.5Hz - 40.0Hz)</div>
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

const canvasRaw = document.getElementById('waveform-raw');
const canvasFilt = document.getElementById('waveform-filtered');
const canvasFilt2 = document.getElementById('waveform-filtered2');
const canvasBlockRaw = document.getElementById('block-raw');
const canvasBlockFilt = document.getElementById('block-filtered');

const ctx_raw = canvasRaw.getContext('2d');
const ctx_filt = canvasFilt.getContext('2d');
const ctx_filt2 = canvasFilt2.getContext('2d');

let W = 0, H = 0; // Dimensions for individual canvas pane
let currentTab = 'tab1';

// Slide buffers for 30s block view
let cached_raw_block = [];
let cached_filt_block = [];

function resize() {
  const firstPane = document.querySelector('.plot-pane');
  if (!firstPane) return;
  const rect = firstPane.getBoundingClientRect();
  W = rect.width;
  H = rect.height;
  if (W > 0 && H > 0) {
    canvasRaw.width = W;
    canvasRaw.height = H;
    canvasFilt.width = W;
    canvasFilt.height = H;
    canvasFilt2.width = W;
    canvasFilt2.height = H;
    
    if (canvasBlockRaw) {
      canvasBlockRaw.width = W;
      canvasBlockRaw.height = H;
    }
    if (canvasBlockFilt) {
      canvasBlockFilt.width = W;
      canvasBlockFilt.height = H;
    }
    
    if (currentTab === 'tab2') {
      drawBlockPlots();
    }
  }
}
window.addEventListener('resize', resize);

window.setTab = function(tabId) {
  currentTab = tabId;
  document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
  document.getElementById('btn-' + tabId).classList.add('active');
  
  if (tabId === 'tab1') {
    document.getElementById('tab1-content').style.display = 'flex';
    document.getElementById('tab2-content').style.display = 'none';
  } else {
    document.getElementById('tab1-content').style.display = 'none';
    document.getElementById('tab2-content').style.display = 'flex';
    drawBlockPlots();
  }
  resize();
};

const SAMPLES_PER_PKT = 48;
const PKT_INTERVAL_MS = 240;
const N = 960; // 4.8 seconds total sweep
const erase_gap = 40; 

const raw_sweep_data = new Array(N).fill(null);
const filtered_sweep_data = new Array(N).fill(null);
const filtered_sweep_data2 = new Array(N).fill(null);
let write_idx = 0;

let rawQueue = [];
let filtQueue = [];
let filtQueue2 = [];

let paused = false;
let lastPacket = null;

const wsProto = location.protocol === 'https:' ? 'wss:' : 'ws:';
const wsUrl = wsProto + '//' + location.host + '/ws';
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
      lastPacket = pkt;
      if (!paused) {
        updateDashboard(pkt);
        const samples = pkt.samples;
        const filtered_samples = pkt.filtered_samples;
        const filtered_samples2 = pkt.filtered_samples2;
        if (samples && samples.length) {
          for (let i = 0; i < samples.length; i++) {
            rawQueue.push(samples[i]);
            filtQueue.push(filtered_samples ? filtered_samples[i] : samples[i]);
            filtQueue2.push(filtered_samples2 ? filtered_samples2[i] : samples[i]);
          }
        }
        
        // Cache 30-second block-processed arrays if sent
        if (pkt.raw_block && pkt.raw_block.length) {
          cached_raw_block = pkt.raw_block;
          cached_filt_block = pkt.filt_block;
          if (currentTab === 'tab2') {
            drawBlockPlots();
          }
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
  const setNum = (id, val) => {
    const el = document.getElementById(id);
    if (el) el.textContent = val != null ? val : '--';
  };
  setNum('v-spo2', pkt.spo2);
  setNum('v-hr', pkt.hr);
  setNum('v-pi', pkt.pi);
  setNum('v-bat', pkt.bat);
}

// Global variables for tracking independent limits for auto-scaling
let raw_smoothed_min = 20000, raw_smoothed_max = 45000;
let filt_smoothed_min = -2000, filt_smoothed_max = 2000;
let filt_smoothed_min2 = -2000, filt_smoothed_max2 = 2000;

function updateScales() {
  let rawMin = Infinity, rawMax = -Infinity;
  let filtMin = Infinity, filtMax = -Infinity;
  let filtMin2 = Infinity, filtMax2 = -Infinity;
  
  let count = 0;
  for (let i = 0; i < N; i++) {
    let r = raw_sweep_data[i];
    let f1 = filtered_sweep_data[i];
    let f2 = filtered_sweep_data2[i];
    if (r !== null) {
      if (r < rawMin) rawMin = r;
      if (r > rawMax) rawMax = r;
      count++;
    }
    if (f1 !== null) {
      if (f1 < filtMin) filtMin = f1;
      if (f1 > filtMax) filtMax = f1;
    }
    if (f2 !== null) {
      if (f2 < filtMin2) filtMin2 = f2;
      if (f2 > filtMax2) filtMax2 = f2;
    }
  }
  
  if (count < 10) return; // Wait for enough data points
  
  // Apply a 10% safety margin around min and max values to keep drawing safe from edges
  let padRaw = (rawMax - rawMin) * 0.1 || 500;
  rawMin -= padRaw; rawMax += padRaw;
  
  let padFilt = (filtMax - filtMin) * 0.1 || 100;
  filtMin -= padFilt; filtMax += padFilt;

  let padFilt2 = (filtMax2 - filtMin2) * 0.1 || 100;
  filtMin2 -= padFilt2; filtMax2 += padFilt2;
  
  // Smoothly decay/adapt limits using an Exponential Moving Average (EMA)
  raw_smoothed_min += (rawMin - raw_smoothed_min) * 0.05;
  raw_smoothed_max += (rawMax - raw_smoothed_max) * 0.05;
  
  filt_smoothed_min += (filtMin - filt_smoothed_min) * 0.05;
  filt_smoothed_max += (filtMax - filt_smoothed_max) * 0.05;

  filt_smoothed_min2 += (filtMin2 - filt_smoothed_min2) * 0.05;
  filt_smoothed_max2 += (filtMax2 - filt_smoothed_max2) * 0.05;
}

function scaleY_raw(val) {
  let range = raw_smoothed_max - raw_smoothed_min;
  if (range <= 0) range = 1;
  const plotH = H - 20; // 20px reserved at the bottom
  const padding = plotH * 0.15;
  const drawHeight = plotH * 0.70;
  return plotH - padding - ((val - raw_smoothed_min) / range) * drawHeight;
}

function scaleY_filt(val) {
  let maxAbs = Math.max(Math.abs(filt_smoothed_min), Math.abs(filt_smoothed_max));
  if (maxAbs <= 0) maxAbs = 100; // Safe default limit
  const plotH = H - 20;
  const padding = plotH * 0.15;
  const drawHeight = plotH * 0.70;
  return plotH - padding - ((val - (-maxAbs)) / (2 * maxAbs)) * drawHeight;
}

function scaleY_filt2(val) {
  let maxAbs = Math.max(Math.abs(filt_smoothed_min2), Math.abs(filt_smoothed_max2));
  if (maxAbs <= 0) maxAbs = 100; // Safe default limit
  const plotH = H - 20;
  const padding = plotH * 0.15;
  const drawHeight = plotH * 0.70;
  return plotH - padding - ((val - (-maxAbs)) / (2 * maxAbs)) * drawHeight;
}

function drawGridAndXAxis(ctx) {
  const plotH = H - 20;
  
  // Draw X axis border line
  ctx.strokeStyle = '#14141a';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(0, plotH);
  ctx.lineTo(W, plotH);
  ctx.stroke();
  
  // Draw grid lines at 240ms intervals (every 48 samples)
  const numSections = 20; // 960 / 48 = 20
  const secWidth = W / numSections;
  
  ctx.font = "9px 'Segoe UI', system-ui, -apple-system, sans-serif";
  ctx.fillStyle = '#5a5a65';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'top';
  
  for (let i = 0; i <= numSections; i++) {
    let x = i * secWidth;
    
    // Vertical grid line (exclude boundaries)
    if (i > 0 && i < numSections) {
      ctx.beginPath();
      ctx.moveTo(x, 0);
      ctx.lineTo(x, plotH);
      if (i % 2 === 0) {
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.03)'; // Major grid every 480ms
      } else {
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.01)'; // Minor grid every 240ms
      }
      ctx.stroke();
    }
    
    // Draw timeline label every 2 sections (480ms = 0.48s)
    if (i % 2 === 0) {
      let seconds = (i * 0.24).toFixed(2);
      ctx.fillText(seconds + 's', x, plotH + 5);
    }
  }
  
  // Draw horizontal grid lines inside the plot area
  ctx.strokeStyle = 'rgba(255, 255, 255, 0.01)';
  const numH = 6;
  for (let i = 1; i < numH; i++) {
    let y = (i / numH) * plotH;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(W, y);
    ctx.stroke();
  }
}

// 30s Static Block Rendering
function drawBlockPlots() {
  if (!canvasBlockRaw || !canvasBlockFilt) return;
  const ctx_b_raw = canvasBlockRaw.getContext('2d');
  const ctx_b_filt = canvasBlockFilt.getContext('2d');
  
  ctx_b_raw.fillStyle = '#040406';
  ctx_b_raw.fillRect(0, 0, W, H);
  
  ctx_b_filt.fillStyle = '#040406';
  ctx_b_filt.fillRect(0, 0, W, H);
  
  function draw30sGrid(ctx) {
    const plotH = H - 20;
    ctx.strokeStyle = '#14141a';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(0, plotH);
    ctx.lineTo(W, plotH);
    ctx.stroke();
    
    const numSections = 6; // 30 seconds / 5s grid line
    const secWidth = W / numSections;
    
    ctx.font = "9px 'Segoe UI', system-ui, -apple-system, sans-serif";
    ctx.fillStyle = '#5a5a65';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    
    for (let i = 0; i <= numSections; i++) {
      let x = i * secWidth;
      if (i > 0 && i < numSections) {
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, plotH);
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.03)';
        ctx.stroke();
      }
      ctx.fillText((i * 5) + 's', x, plotH + 5);
    }
    
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.01)';
    const numH = 6;
    for (let i = 1; i < numH; i++) {
      let y = (i / numH) * plotH;
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(W, y);
      ctx.stroke();
    }
  }
  
  draw30sGrid(ctx_b_raw);
  draw30sGrid(ctx_b_filt);
  
  if (!cached_raw_block || cached_raw_block.length < 400) {
    let secs = cached_raw_block ? (cached_raw_block.length / 200.0).toFixed(1) : "0.0";
    ctx_b_raw.font = "14px 'Segoe UI', system-ui, -apple-system, sans-serif";
    ctx_b_raw.fillStyle = '#00d2ff';
    ctx_b_raw.textAlign = 'center';
    ctx_b_raw.fillText("Accumulating Initial Buffer... (" + secs + "s / 2.0s)", W/2, H/2);
    
    ctx_b_filt.font = "14px 'Segoe UI', system-ui, -apple-system, sans-serif";
    ctx_b_filt.fillStyle = '#7a7a85';
    ctx_b_filt.textAlign = 'center';
    ctx_b_filt.fillText("Waiting for block processing triggers... (need 2.0 seconds minimum)", W/2, H/2);
    return;
  }
  
  const len = cached_raw_block.length;
  const plotH = H - 20;
  const padding = plotH * 0.10;
  const drawH = plotH * 0.80;
  
  // Plot 30s Raw Block
  const rMin = Math.min(...cached_raw_block);
  const rMax = Math.max(...cached_raw_block);
  let rRange = rMax - rMin;
  if (rRange <= 0) rRange = 1;
  
  ctx_b_raw.beginPath();
  ctx_b_raw.strokeStyle = '#00d2ff';
  ctx_b_raw.lineWidth = 1.5;
  ctx_b_raw.shadowColor = 'rgba(0, 210, 255, 0.15)';
  ctx_b_raw.shadowBlur = 4;
  
  for (let i = 0; i < len; i++) {
    let x = (i / (len - 1)) * W;
    let y = plotH - padding - ((cached_raw_block[i] - rMin) / rRange) * drawH;
    if (i === 0) ctx_b_raw.moveTo(x, y);
    else ctx_b_raw.lineTo(x, y);
  }
  ctx_b_raw.stroke();
  ctx_b_raw.shadowBlur = 0;
  
  // Draw raw progress badge
  let secs = (len / 200.0).toFixed(1);
  ctx_b_raw.font = "10px 'Segoe UI', system-ui, -apple-system, sans-serif";
  ctx_b_raw.fillStyle = '#00d2ff';
  ctx_b_raw.textAlign = 'right';
  ctx_b_raw.textBaseline = 'top';
  ctx_b_raw.fillText("Buffer Progress: " + secs + "s / 30.0s", W - 12, 12);

  // Plot 30s Filtered Block
  if (cached_filt_block && cached_filt_block.length) {
    const fMin = Math.min(...cached_filt_block);
    const fMax = Math.max(...cached_filt_block);
    let fRange = fMax - fMin;
    if (fRange <= 0) fRange = 1;
    
    ctx_b_filt.beginPath();
    ctx_b_filt.strokeStyle = '#00ff66';
    ctx_b_filt.lineWidth = 2.0;
    ctx_b_filt.shadowColor = 'rgba(0, 255, 102, 0.3)';
    ctx_b_filt.shadowBlur = 6;
    
    for (let i = 0; i < cached_filt_block.length; i++) {
      let x = (i / (cached_filt_block.length - 1)) * W;
      let y = plotH - padding - ((cached_filt_block[i] - fMin) / fRange) * drawH;
      if (i === 0) ctx_b_filt.moveTo(x, y);
      else ctx_b_filt.lineTo(x, y);
    }
    ctx_b_filt.stroke();
    ctx_b_filt.shadowBlur = 0;
    
    // Draw filtered progress badge
    ctx_b_filt.font = "10px 'Segoe UI', system-ui, -apple-system, sans-serif";
    ctx_b_filt.fillStyle = '#00ff66';
    ctx_b_filt.textAlign = 'right';
    ctx_b_filt.textBaseline = 'top';
    ctx_b_filt.fillText("Buffer Progress: " + secs + "s / 30.0s", W - 12, 12);
  } else {
    ctx_b_filt.font = "14px 'Segoe UI', system-ui, -apple-system, sans-serif";
    ctx_b_filt.fillStyle = '#7a7a85';
    ctx_b_filt.textAlign = 'center';
    ctx_b_filt.fillText("Processing zero-phase FIR/IIR filter on 30s block...", W/2, H/2);
  }
}

let lastFrameTime = performance.now();
const sampleRate = 200; 
let samplesAccumulator = 0;

function animationLoop(timestamp) {
  requestAnimationFrame(animationLoop);
  
  if (paused) {
    lastFrameTime = timestamp;
    if (currentTab === 'tab1') {
      renderFrame();
    }
    return;
  }
  
  let elapsed = timestamp - lastFrameTime;
  lastFrameTime = timestamp;
  
  if (elapsed > 100) elapsed = 100;
  
  if (rawQueue.length > 2000) {
    rawQueue = rawQueue.slice(rawQueue.length - 200);
    filtQueue = filtQueue.slice(filtQueue.length - 200);
    filtQueue2 = filtQueue2.slice(filtQueue2.length - 200);
  }
  
  samplesAccumulator += (elapsed / 1000) * sampleRate;
  let samplesCount = Math.floor(samplesAccumulator);
  samplesAccumulator -= samplesCount;
  
  for (let k = 0; k < samplesCount; k++) {
    if (rawQueue.length > 0 && filtQueue.length > 0 && filtQueue2.length > 0) {
      let rawSample = rawQueue.shift();
      let filtSample = filtQueue.shift();
      let filtSample2 = filtQueue2.shift();
      
      raw_sweep_data[write_idx] = rawSample;
      filtered_sweep_data[write_idx] = filtSample;
      filtered_sweep_data2[write_idx] = filtSample2;
      
      for (let g = 1; g <= erase_gap; g++) {
        raw_sweep_data[(write_idx + g) % N] = null;
        filtered_sweep_data[(write_idx + g) % N] = null;
        filtered_sweep_data2[(write_idx + g) % N] = null;
      }
      
      write_idx = (write_idx + 1) % N;
    } else {
      break;
    }
  }
  
  if (currentTab === 'tab1') {
    updateScales();
    renderFrame();
  }
}

function renderFrame() {
  // --- Draw Top Pane: Unfiltered (Raw) Waveform ---
  ctx_raw.fillStyle = '#040406';
  ctx_raw.fillRect(0, 0, W, H);
  
  drawGridAndXAxis(ctx_raw);
  
  ctx_raw.beginPath();
  ctx_raw.strokeStyle = '#00d2ff'; 
  ctx_raw.lineWidth = 2;
  ctx_raw.shadowColor = 'rgba(0, 210, 255, 0.3)';
  ctx_raw.shadowBlur = 8;
  ctx_raw.lineJoin = 'round';
  ctx_raw.lineCap = 'round';
  
  let startedRaw = false;
  for (let i = 0; i < N; i++) {
    let val = raw_sweep_data[i];
    if (val === null || val === undefined) {
      if (startedRaw) {
        ctx_raw.stroke();
        ctx_raw.beginPath();
        startedRaw = false;
      }
      continue;
    }
    
    let x = (i / N) * W;
    let y = scaleY_raw(val);
    if (!startedRaw) {
      ctx_raw.moveTo(x, y);
      startedRaw = true;
    } else {
      ctx_raw.lineTo(x, y);
    }
  }
  if (startedRaw) {
    ctx_raw.stroke();
  }
  ctx_raw.shadowBlur = 0;
  
  let headIdx = (write_idx - 1 + N) % N;
  let headValRaw = raw_sweep_data[headIdx];
  if (headValRaw !== null && headValRaw !== undefined) {
    let headX = (headIdx / N) * W;
    let headY = scaleY_raw(headValRaw);
    
    ctx_raw.beginPath();
    ctx_raw.arc(headX, headY, 6, 0, 2 * Math.PI);
    ctx_raw.strokeStyle = 'rgba(0, 210, 255, 0.6)';
    ctx_raw.lineWidth = 1.5;
    ctx_raw.stroke();
    
    ctx_raw.beginPath();
    ctx_raw.arc(headX, headY, 3, 0, 2 * Math.PI);
    ctx_raw.fillStyle = '#ffffff';
    ctx_raw.shadowColor = '#00d2ff';
    ctx_raw.shadowBlur = 10;
    ctx_raw.fill();
    ctx_raw.shadowBlur = 0;
  }
  
  // --- Draw Middle Pane: Filtered Waveform (0.5Hz - 8Hz) ---
  ctx_filt.fillStyle = '#040406';
  ctx_filt.fillRect(0, 0, W, H);
  
  drawGridAndXAxis(ctx_filt);
  
  let yBaseline = scaleY_filt(0);
  ctx_filt.beginPath();
  ctx_filt.strokeStyle = 'rgba(0, 255, 102, 0.08)';
  ctx_filt.lineWidth = 1;
  ctx_filt.setLineDash([6, 6]);
  ctx_filt.moveTo(0, yBaseline);
  ctx_filt.lineTo(W, yBaseline);
  ctx_filt.stroke();
  ctx_filt.setLineDash([]); 
  
  ctx_filt.beginPath();
  ctx_filt.strokeStyle = '#00ff66'; 
  ctx_filt.lineWidth = 2.5;
  ctx_filt.shadowColor = 'rgba(0, 255, 102, 0.45)';
  ctx_filt.shadowBlur = 10;
  ctx_filt.lineJoin = 'round';
  ctx_filt.lineCap = 'round';
  
  let startedFilt = false;
  for (let i = 0; i < N; i++) {
    let val = filtered_sweep_data[i];
    if (val === null || val === undefined) {
      if (startedFilt) {
        ctx_filt.stroke();
        ctx_filt.beginPath();
        startedFilt = false;
      }
      continue;
    }
    
    let x = (i / N) * W;
    let y = scaleY_filt(val);
    if (!startedFilt) {
      ctx_filt.moveTo(x, y);
      startedFilt = true;
    } else {
      ctx_filt.lineTo(x, y);
    }
  }
  if (startedFilt) {
    ctx_filt.stroke();
  }
  ctx_filt.shadowBlur = 0;
  
  let headValFilt = filtered_sweep_data[headIdx];
  if (headValFilt !== null && headValFilt !== undefined) {
    let headX = (headIdx / N) * W;
    let headY = scaleY_filt(headValFilt);
    
    ctx_filt.beginPath();
    ctx_filt.arc(headX, headY, 8, 0, 2 * Math.PI);
    ctx_filt.strokeStyle = 'rgba(0, 255, 102, 0.6)';
    ctx_filt.lineWidth = 1.5;
    ctx_filt.stroke();
    
    ctx_filt.beginPath();
    ctx_filt.arc(headX, headY, 4, 0, 2 * Math.PI);
    ctx_filt.fillStyle = '#ffffff';
    ctx_filt.shadowColor = '#00ff66';
    ctx_filt.shadowBlur = 12;
    ctx_filt.fill();
    ctx_filt.shadowBlur = 0;
  }

  // --- Draw Bottom Pane: Filtered Waveform 2 (0.5Hz - 40Hz) ---
  ctx_filt2.fillStyle = '#040406';
  ctx_filt2.fillRect(0, 0, W, H);
  
  drawGridAndXAxis(ctx_filt2);
  
  let yBaseline2 = scaleY_filt2(0);
  ctx_filt2.beginPath();
  ctx_filt2.strokeStyle = 'rgba(191, 90, 242, 0.08)';
  ctx_filt2.lineWidth = 1;
  ctx_filt2.setLineDash([6, 6]);
  ctx_filt2.moveTo(0, yBaseline2);
  ctx_filt2.lineTo(W, yBaseline2);
  ctx_filt2.stroke();
  ctx_filt2.setLineDash([]); 
  
  ctx_filt2.beginPath();
  ctx_filt2.strokeStyle = '#bf5af2'; 
  ctx_filt2.lineWidth = 2.5;
  ctx_filt2.shadowColor = 'rgba(191, 90, 242, 0.45)';
  ctx_filt2.shadowBlur = 10;
  ctx_filt2.lineJoin = 'round';
  ctx_filt2.lineCap = 'round';
  
  let startedFilt2 = false;
  for (let i = 0; i < N; i++) {
    let val = filtered_sweep_data2[i];
    if (val === null || val === undefined) {
      if (startedFilt2) {
        ctx_filt2.stroke();
        ctx_filt2.beginPath();
        startedFilt2 = false;
      }
      continue;
    }
    
    let x = (i / N) * W;
    let y = scaleY_filt2(val);
    if (!startedFilt2) {
      ctx_filt2.moveTo(x, y);
      startedFilt2 = true;
    } else {
      ctx_filt2.lineTo(x, y);
    }
  }
  if (startedFilt2) {
    ctx_filt2.stroke();
  }
  ctx_filt2.shadowBlur = 0;
  
  let headValFilt2 = filtered_sweep_data2[headIdx];
  if (headValFilt2 !== null && headValFilt2 !== undefined) {
    let headX = (headIdx / N) * W;
    let headY = scaleY_filt2(headValFilt2);
    
    ctx_filt2.beginPath();
    ctx_filt2.arc(headX, headY, 8, 0, 2 * Math.PI);
    ctx_filt2.strokeStyle = 'rgba(191, 90, 242, 0.6)';
    ctx_filt2.lineWidth = 1.5;
    ctx_filt2.stroke();
    
    ctx_filt2.beginPath();
    ctx_filt2.arc(headX, headY, 4, 0, 2 * Math.PI);
    ctx_filt2.fillStyle = '#ffffff';
    ctx_filt2.shadowColor = '#bf5af2';
    ctx_filt2.shadowBlur = 12;
    ctx_filt2.fill();
    ctx_filt2.shadowBlur = 0;
  }
}

// Pause functionality
const btnPause = document.getElementById('btn-pause');
btnPause.addEventListener('click', () => {
  paused = !paused;
  btnPause.textContent = paused ? 'Resume' : 'Pause';
  btnPause.className = paused ? 'active' : '';
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
                # Wait for any message from the browser with a long timeout.
                # The browser never actively sends data, so this just keeps the
                # coroutine alive. If the browser disconnects cleanly, we get
                # WebSocketDisconnect; if it drops, asyncio.TimeoutError loops.
                await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
            except asyncio.TimeoutError:
                # No message from the browser in 60s — that's normal, keep going
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
    # Save to Lifesigns v1.1 JSON-Lines file (includes accelerometer x/y/z)
    _json_recorder.save_dict(d)
    broadcast(d)
    return {"ok": True, "relayed": len(browser_clients), "saved": _json_recorder.records_written}


# ---------------------------------------------------------------------------
# BLE control endpoints
# ---------------------------------------------------------------------------

@app.post("/ble/connect")
async def ble_connect():
    """
    Start scanning for the BerryMed watch and begin streaming.
    Uses SETTINGS.device_address (config.py).
    Safe to call again — if already running, returns current state.
    """
    global _ble_task
    if _ble_task and not _ble_task.done():
        return {"ok": False, "reason": "BLE runner already active", "status": _ble_status}

    # Reset parser + processor state for a fresh session
    _data_parser.flush()
    _ble_status.update({
        "state": "scanning",
        "error": None,
        "packets_received": 0,
        "packets_saved":    0,
    })
    _ble_task = asyncio.create_task(_ble_runner())
    logger.info("[BLE] Connect requested — runner task started")
    return {"ok": True, "status": _ble_status}


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
        "parser_stats": _data_parser.stats,
        "json_records": _json_recorder.records_written,
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
            val = int(
                base
                + math.sin((t + phase) * 2.0 * math.pi * 0.2) * 8000
                + random.gauss(0, 200)
            )
            samples.append(val)

        proc_results = _sim_processor.process(samples)

        # Simulated accelerometer values (gentle motion noise around gravity on Z)
        sim_ax = int(random.gauss(0, 3))
        sim_ay = int(random.gauss(0, 3))
        sim_az = int(random.gauss(63, 5))   # ~0.5 g on Z axis

        pkt_dict = {
            "ts":  datetime.now().isoformat(timespec="milliseconds"),
            "pkt": int(t * 4.17) % 256,
            "spo2":  random.randint(95, 100),
            "spo2r": random.randint(95, 100),
            "hr":    random.randint(60, 80),
            "hrr":   random.randint(60, 80),
            "pi":    random.randint(5, 30),
            "pir":   random.randint(5, 30),
            "bat":   random.randint(60, 100),
            "status": [],
            # ── Accelerometer (Lifesigns v1.1: Axis-X/Y/Z, signed int8) ───
            "ax": max(-128, min(127, sim_ax)),
            "ay": max(-128, min(127, sim_ay)),
            "az": max(-128, min(127, sim_az)),
            "samples":          samples,
            "filtered_samples":  proc_results["filtered1"],
            "filtered_samples2": proc_results["filtered2"],
            "raw_block":         proc_results["raw_block"],
            "filt_block":        proc_results["filt_block"],
        }

        # Save to Lifesigns v1.1 JSON-Lines file (includes accelerometer x/y/z)
        _json_recorder.save_dict(pkt_dict)
        broadcast(pkt_dict)


# ---------------------------------------------------------------------------
# CLI for standalone mode
# ---------------------------------------------------------------------------


def main():
    global _simulate_mode
    p = argparse.ArgumentParser(description="PlethAnalysis Web Server")
    p.add_argument("--simulate", action="store_true",
                   help="Use synthetic data (no BLE hardware required)")
    p.add_argument("--connect", action="store_true",
                   help="Auto-connect to BerryMed watch on startup")
    p.add_argument("--port", type=int, default=8000, help="HTTP port (default 8000)")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    args = p.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format=SETTINGS.log_format)

    _simulate_mode = args.simulate

    if args.connect and not args.simulate:
        # Schedule BLE connect after the event loop starts
        @app.on_event("startup")
        async def _auto_connect():
            global _ble_task
            _ble_task = asyncio.create_task(_ble_runner())
            logger.info("[BLE] Auto-connect started (--connect flag)")

    mode = "simulated" if args.simulate else ("BLE auto-connect" if args.connect else "web-only")
    logger.info("PlethAnalysis server on http://localhost:%d  [%s]", args.port, mode)
    logger.info("  Open browser → http://localhost:%d", args.port)
    logger.info("  BLE connect  → POST http://localhost:%d/ble/connect", args.port)
    logger.info("  BLE status   → GET  http://localhost:%d/ble/status", args.port)
    uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
