#!/usr/bin/env python3
"""
<<<<<<< HEAD
PlethAnalysis Web Server.

Serves the real-time waveform page, manages BLE connection to the
BerryMed watch, processes PPG signals, and saves data to JSON.

Usage:
    python server.py                # web-only (use Connect button in browser)
    python server.py --connect      # auto-connect to watch on startup
    python server.py --simulate     # synthetic data, no hardware needed
=======
PlethAnalysis Web Server — pure web module.

Serves the real-time waveform HTML page and accepts data
via WebSocket (/ws) or HTTP POST (/data).

Standalone usage (simulated data for testing):
    python server.py --simulate
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
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
<<<<<<< HEAD
from dataparse import DataParser, NUM_ADC_SAMPLES
from datasave import JSONRecorder
from bleak import BleakScanner
from pair import BLEManager, ConnectionFailed, DiscoveryTimeout
=======
from dataparse import NUM_ADC_SAMPLES
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
from singalProcess import PPGProcessor

logger = logging.getLogger("server")

# ---------------------------------------------------------------------------
<<<<<<< HEAD
# JSON recorder — Lifesigns v1.1 format (one record per packet, JSONL file)
# ---------------------------------------------------------------------------

# Recorder is None until the first connection — a fresh file is opened each
# time the watch connects and closed when it disconnects / runner stops.
_json_recorder: Optional[JSONRecorder] = None


def _open_session_recorder(device_name: str = "BerryMed") -> JSONRecorder:
    """Create a new JSONL file named by session start time."""
    from pathlib import Path
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe = device_name.replace(" ", "_").replace(":", "")[:20]
    path = Path(__file__).parent / f"lifesigns_{safe}_{ts}.json"
    logger.info("[REC] New session file: %s", path)
    return JSONRecorder(path)

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
=======
# Connected browser clients (shared with main.py)
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
# ---------------------------------------------------------------------------

browser_clients: Set[WebSocket] = set()

# ---------------------------------------------------------------------------
# Broadcast helper — push a JSON packet to all browsers
# ---------------------------------------------------------------------------


def broadcast(packet_dict: dict) -> None:
<<<<<<< HEAD
=======
    """Called from main.py or anywhere in-process to push data to all browsers."""
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
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


<<<<<<< HEAD
def broadcast_ble_state(state: str, **extra) -> None:
    """Push a BLE state-change event to all browsers immediately."""
    broadcast({"_ble_event": True, "state": state, **extra})


=======
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
async def _send_ws(ws: WebSocket, payload: str) -> None:
    try:
        await ws.send_text(payload)
    except Exception:
        browser_clients.discard(ws)

<<<<<<< HEAD

=======
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

_simulate_mode = False


<<<<<<< HEAD
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
    if _json_recorder is not None:
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
      4. On disconnect, closes the session file and goes back to scanning
    Cancelled only by /ble/disconnect or server shutdown.
    """
    global _json_recorder
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

            # Tell the watch to stay awake for at least 1 minute (0xF7).
            # Without this, the BerryMed stops advertising ~30 s after it
            # loses skin contact, making reconnects impossible.
            wake_cmd = _data_parser.send_command(SETTINGS.cmd_wake_1min)
            await _ble_manager.send_command(wake_cmd)
            logger.info("[BLE] Wake-keep (0xF7) sent — watch will stay awake 1 min")

            _ble_manager.on_data(_on_ble_data)
            await _ble_manager.start_stream()

            # ── Open a fresh JSONL file for this session ───────────────────
            if _json_recorder is not None:
                _json_recorder.close()
            _json_recorder = _open_session_recorder(dev_name)

            _ble_status.update({
                "state":            "connected",
                "address":          _ble_target_address,
                "device_name":      dev_name,
                "packets_received": 0,
                "packets_saved":    0,
                "session_file":     str(_json_recorder.path),
            })
            broadcast_ble_state("connected",
                                device_name=dev_name, address=_ble_target_address,
                                session_file=str(_json_recorder.path))
            logger.info("[BLE] Connected — streaming data → %s", _json_recorder.path)

            # ── STREAM — keep alive until watch drops ─────────────────────
            while _ble_manager.connected:
                await asyncio.sleep(0.5)

            # ── Session ended — close this file ───────────────────────────
            if _json_recorder is not None:
                logger.info("[REC] Session ended — %d records saved to %s",
                            _json_recorder.records_written, _json_recorder.path)
                _json_recorder.close()
                _json_recorder = None

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
            if _json_recorder is not None:
                logger.info("[REC] Runner cancelled — closing session file (%d records)",
                            _json_recorder.records_written)
                _json_recorder.close()
                _json_recorder = None
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


=======
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
@asynccontextmanager
async def lifespan(app: FastAPI):
    bg_task = None
    if _simulate_mode:
        bg_task = asyncio.create_task(_run_simulator())
    logger.info("Web server ready")
    yield
<<<<<<< HEAD
    # clean up simulator
=======
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
    if bg_task:
        bg_task.cancel()
        try:
            await bg_task
        except asyncio.CancelledError:
            pass
<<<<<<< HEAD
    # clean up BLE runner
    global _ble_task
    if _ble_task and not _ble_task.done():
        _ble_task.cancel()
        try:
            await _ble_task
        except asyncio.CancelledError:
            pass
    await _ble_manager.disconnect()
    if _json_recorder is not None:
        _json_recorder.close()
=======
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
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
<<<<<<< HEAD
  justify-content: center; min-width: 120px; padding: 6px 16px;
=======
  justify-content: center; min-width: 120px; padding: 6px 16px; 
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
  background: rgba(255, 255, 255, 0.015); border: 1px solid rgba(255, 255, 255, 0.04);
  border-radius: 8px; transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1); }

.dash-item:hover { background: rgba(255, 255, 255, 0.035); border-color: rgba(255, 255, 255, 0.08);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.5); transform: translateY(-1px); }

.dash-label { font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 1.5px;
  color: #7a7a85; margin-bottom: 2px; }

.dash-value { font-size: 26px; font-weight: 700; font-variant-numeric: tabular-nums;
  letter-spacing: -0.5px; line-height: 1; }

.dash-value.spo2 { color: #00d2ff; text-shadow: 0 0 10px rgba(0, 210, 255, 0.2); }
<<<<<<< HEAD
.dash-value.hr   { color: #00ff66; text-shadow: 0 0 10px rgba(0, 255, 102, 0.2); }
.dash-value.pi   { color: #bf5af2; text-shadow: 0 0 10px rgba(191, 90, 242, 0.2); }
.dash-value.bat  { color: #ff9f0a; text-shadow: 0 0 10px rgba(255, 159, 10, 0.2); }
=======
.dash-value.hr { color: #00ff66; text-shadow: 0 0 10px rgba(0, 255, 102, 0.2); }
.dash-value.pi  { color: #bf5af2; text-shadow: 0 0 10px rgba(191, 90, 242, 0.2); }
.dash-value.bat { color: #ff9f0a; text-shadow: 0 0 10px rgba(255, 159, 10, 0.2); }
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd

.dash-unit { font-size: 12px; color: #5a5a65; font-weight: 500; margin-left: 2px; }

#tab-bar { display: flex; gap: 8px; margin-left: 16px; }
.tab-btn { background: rgba(255, 255, 255, 0.02); color: #7a7a85; border: 1px solid rgba(255, 255, 255, 0.05);
  padding: 8px 16px; border-radius: 8px; font-size: 11px; font-weight: 700; text-transform: uppercase;
  letter-spacing: 1px; cursor: pointer; transition: all 0.2s ease; font-family: inherit; }
.tab-btn:hover { background: rgba(255, 255, 255, 0.05); color: #f0f0f5; border-color: rgba(255, 255, 255, 0.1); }
.tab-btn.active { background: rgba(0, 210, 255, 0.1); color: #00d2ff; border-color: #00d2ff;
  box-shadow: 0 0 12px rgba(0, 210, 255, 0.15); }

<<<<<<< HEAD
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

=======
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
.dash-status { display: flex; align-items: center; gap: 8px; margin-left: auto;
  padding: 8px 16px; background: rgba(255, 255, 255, 0.015); border: 1px solid rgba(255, 255, 255, 0.04);
  border-radius: 8px; font-size: 13px; font-weight: 500; }

.status-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
<<<<<<< HEAD
.status-dot.ok           { background: #00ff66; box-shadow: 0 0 8px #00ff66cc; }
.status-dot.warn         { background: #ff453a; box-shadow: 0 0 8px #ff453acc; }
.status-dot.disconnected { background: #3a3a3c; box-shadow: none; }

#plot-wrap { flex: 1; display: flex; flex-direction: column; gap: 6px; padding: 6px;
  background: #020203; overflow: hidden; position: relative; }

.tab-content { display: flex; flex-direction: column; gap: 6px; flex: 1; height: 100%; overflow: hidden; }

.plot-pane { flex: 1; position: relative; background: #040406; border: 1px solid #14141a;
  border-radius: 8px; overflow: hidden; }
=======
.status-dot.ok { background: #00ff66; box-shadow: 0 0 8px #00ff66cc; }
.status-dot.warn { background: #ff453a; box-shadow: 0 0 8px #ff453acc; }
.status-dot.disconnected { background: #3a3a3c; box-shadow: none; }

#plot-wrap { flex: 1; display: flex; flex-direction: column; gap: 6px; padding: 6px; background: #020203; overflow: hidden; position: relative; }

.tab-content { display: flex; flex-direction: column; gap: 6px; flex: 1; height: 100%; overflow: hidden; }

.plot-pane { flex: 1; position: relative; background: #040406; border: 1px solid #14141a; border-radius: 8px; overflow: hidden; }
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
.plot-pane canvas { display: block; width: 100%; height: 100%; }

.pane-title { position: absolute; top: 8px; left: 12px; font-size: 10px; font-weight: 700;
  text-transform: uppercase; letter-spacing: 1.2px; color: #7a7a85; z-index: 10;
  pointer-events: none; background: rgba(4, 4, 6, 0.75); padding: 3px 8px; border-radius: 4px;
  border: 1px solid rgba(255, 255, 255, 0.03); backdrop-filter: blur(4px); }

#controls { position: absolute; top: 16px; right: 24px; display: flex; gap: 8px; z-index: 20; }
#controls button { background: rgba(15, 15, 20, 0.85); color: #8e8e93; border: 1px solid #2c2c35;
  padding: 6px 16px; border-radius: 6px; font-size: 12px; font-weight: 600; cursor: pointer;
<<<<<<< HEAD
  font-family: inherit; letter-spacing: 0.5px; backdrop-filter: blur(8px); transition: all 0.2s ease; }
#controls button:hover  { background: #1c1c24; color: #f0f0f5; border-color: #48485a; }
#controls button.active { background: rgba(0, 255, 102, 0.1); color: #00ff66; border-color: #00ff66; }

/* (device-picker modal removed — Connect Watch auto-discovers any Lifesigns device) */
=======
  font-family: inherit; letter-spacing: 0.5px; backdrop-filter: blur(8px);
  transition: all 0.2s ease; }

#controls button:hover { background: #1c1c24; color: #f0f0f5; border-color: #48485a; }
#controls button.active { background: rgba(0, 255, 102, 0.1); color: #00ff66; border-color: #00ff66; }
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
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
<<<<<<< HEAD

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

=======
    
    <!-- Tab Controls inside Dashboard Header -->
    <div id="tab-bar">
      <button class="tab-btn active" id="btn-tab1" onclick="setTab('tab1')">Live Sweep Monitor</button>
      <button class="tab-btn" id="btn-tab2" onclick="setTab('tab2')">30s Block Processed View</button>
    </div>

>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
    <div class="dash-status" id="dash-status">
      <span class="status-dot disconnected" id="status-dot"></span>
      <span id="status-text">Waiting for data</span>
    </div>
  </div>
<<<<<<< HEAD

  <!-- No device-picker modal needed — Connect Watch button auto-discovers any Lifesigns device -->

=======
  
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
  <div id="plot-wrap">
    <!-- TAB 1: LIVE SWEEP MONITOR -->
    <div id="tab1-content" class="tab-content">
      <div class="plot-pane">
        <div class="pane-title">Unfiltered Waveform (Raw)</div>
        <canvas id="waveform-raw"></canvas>
      </div>
      <div class="plot-pane">
<<<<<<< HEAD
        <div class="pane-title">Filtered Waveform (PPG Butterworth: 2nd Order, 0.5Hz – 8.0Hz)</div>
        <canvas id="waveform-filtered"></canvas>
      </div>
      <div class="plot-pane">
        <div class="pane-title">Filtered Waveform (PPG Butterworth: 4th Order, 0.5Hz – 40.0Hz)</div>
=======
        <div class="pane-title">Filtered Waveform (PPG Butterworth: 2nd Order, 0.5Hz - 8.0Hz)</div>
        <canvas id="waveform-filtered"></canvas>
      </div>
      <div class="plot-pane">
        <div class="pane-title">Filtered Waveform (PPG Butterworth: 4th Order, 0.5Hz - 40.0Hz)</div>
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
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
<<<<<<< HEAD
        <div class="pane-title">30-Second Block-Filtered (Zero-Phase Bandpass, 0.5Hz – 40.0Hz)</div>
=======
        <div class="pane-title">30-Second Block-Filtered (Zero-Phase Bandpass, 0.5Hz - 40.0Hz)</div>
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
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

<<<<<<< HEAD
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
=======
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
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
let cached_raw_block = [];
let cached_filt_block = [];

function resize() {
  const firstPane = document.querySelector('.plot-pane');
  if (!firstPane) return;
  const rect = firstPane.getBoundingClientRect();
<<<<<<< HEAD
  W = rect.width; H = rect.height;
  if (W > 0 && H > 0) {
    canvasRaw.width   = W; canvasRaw.height   = H;
    canvasFilt.width  = W; canvasFilt.height  = H;
    canvasFilt2.width = W; canvasFilt2.height = H;
    if (canvasBlockRaw)  { canvasBlockRaw.width  = W; canvasBlockRaw.height  = H; }
    if (canvasBlockFilt) { canvasBlockFilt.width = W; canvasBlockFilt.height = H; }
    if (currentTab === 'tab2') drawBlockPlots();
=======
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
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
  }
}
window.addEventListener('resize', resize);

window.setTab = function(tabId) {
  currentTab = tabId;
<<<<<<< HEAD
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
=======
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
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
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
<<<<<<< HEAD

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
=======
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
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
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
<<<<<<< HEAD
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
  // Persist until the next successful state change clears it
  const el = document.getElementById('status-text');
  if (el) { el.textContent = '⚠ ' + msg; el.style.color = '#ff453a'; }
  // Also show in the BLE counter area so it's visible alongside the button
  const pktsEl = document.getElementById('ble-pkts');
  if (pktsEl) { pktsEl.textContent = 'Error'; pktsEl.style.color = '#ff453a'; }
}

const BLE_UI = {
  idle:        { label: 'Connect Watch',  cls: '',               disabled: false },
  scanning:    { label: '⏹ Cancel Scan', cls: 'state-busy',     disabled: false },
  connecting:  { label: '⏹ Cancel',      cls: 'state-busy',     disabled: false },
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

  // Clear any error colouring on transitions to non-error states
  if (state !== 'error') {
    const stEl = document.getElementById('status-text');
    if (stEl && stEl.style.color === 'rgb(255, 69, 58)') {
      stEl.style.color = '';
    }
    const pktsEl2 = document.getElementById('ble-pkts');
    if (pktsEl2 && pktsEl2.style.color) pktsEl2.style.color = '';
  }

  const pktsEl = document.getElementById('ble-pkts');
  if (state === 'connected' && pkts != null) {
    pktsEl.textContent = (devName ? devName + ' · ' : '') + pkts + ' pkts';
  } else if (state !== 'error') {
    pktsEl.textContent = state === 'scanning' ? 'Scanning…' :
                         state === 'connecting' ? 'Connecting…' : '';
  }
}

// ── BLE button — one click connects, one click cancels/disconnects ───────────
window.bleButtonClick = async function() {
  if (_bleState === 'connected') {
    await fetch('/ble/disconnect', { method: 'POST' });
    applyBleUI('idle', 0);
    return;
  }
  if (_bleState === 'scanning' || _bleState === 'connecting') {
    // User wants to cancel — stop and go back to idle
    await fetch('/ble/disconnect', { method: 'POST' });
    applyBleUI('idle', 0);
    return;
  }
  // idle / error / stopped → start auto-scan (no address = find any Lifesigns device)
  applyBleUI('scanning', 0);
  try {
    const r = await fetch('/ble/connect', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({}),   // empty = auto-discover any BerryMed/Lifesigns watch
    });
    const d = await r.json();
    if (!d.ok) {
      showBleError(d.reason || 'Could not start BLE runner');
      applyBleUI('error', 0);
    }
    // State changes come in via WebSocket (_ble_event) from here on
  } catch(e) {
    showBleError('Network error: ' + e);
    applyBleUI('error', 0);
  }
};

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
=======
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
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
let raw_smoothed_min = 20000, raw_smoothed_max = 45000;
let filt_smoothed_min = -2000, filt_smoothed_max = 2000;
let filt_smoothed_min2 = -2000, filt_smoothed_max2 = 2000;

function updateScales() {
<<<<<<< HEAD
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
=======
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
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
  }
}

function renderFrame() {
<<<<<<< HEAD
  drawChannel(ctx_raw,   raw_sweep_data,      scaleY_raw,   '#00d2ff','rgba(0,210,255,0.3)', 3);
  drawChannel(ctx_filt,  filtered_sweep_data,  scaleY_filt,  '#00ff66','rgba(0,255,102,0.45)',4);
  drawChannel(ctx_filt2, filtered_sweep_data2, scaleY_filt2, '#bf5af2','rgba(191,90,242,0.45)',4);
}

// ── Pause button ─────────────────────────────────────────────────────────────
document.getElementById('btn-pause').addEventListener('click', function(){
  paused=!paused;
  this.textContent=paused?'Resume':'Pause';
  this.className=paused?'active':'';
=======
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
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
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
<<<<<<< HEAD
                await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
            except asyncio.TimeoutError:
=======
                # Wait for any message from the browser with a long timeout.
                # The browser never actively sends data, so this just keeps the
                # coroutine alive. If the browser disconnects cleanly, we get
                # WebSocketDisconnect; if it drops, asyncio.TimeoutError loops.
                await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
            except asyncio.TimeoutError:
                # No message from the browser in 60s — that's normal, keep going
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
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
<<<<<<< HEAD
    d = pkt.model_dump()
    if _json_recorder is not None:
        _json_recorder.save_dict(d)
    broadcast(d)
    saved = _json_recorder.records_written if _json_recorder else 0
    return {"ok": True, "relayed": len(browser_clients), "saved": saved}


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

    If a runner is already CONNECTED, refuses (disconnect first).
    If a runner is scanning/connecting/error, cancels it and restarts with
    the new target — this is the normal path after a modal scan pick.
    """
    global _ble_task, _ble_target_address

    if _ble_task and not _ble_task.done():
        if _ble_status.get("state") == "connected":
            # Already streaming — don't interrupt
            return {"ok": False, "reason": "Already connected — disconnect first", "status": _ble_status}

        # Runner is scanning/connecting/error for old target — cancel it so we
        # can restart cleanly with the device the user just picked from the modal.
        logger.info("[BLE] Cancelling runner (state=%s) to honour new connect request",
                    _ble_status.get("state"))
        _ble_task.cancel()
        try:
            await _ble_task
        except asyncio.CancelledError:
            pass
        _ble_task = None

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

    # Lifesigns Protocol v1.1 — Comm Service UUID used to identify BerryMed devices
    # even when they advertise without a readable name (common on BerryMed hardware)
    LIFESIGNS_SVC_UUID = "49535343-fe7d-4ae5-8fa9-9fafd205e455"
    BERRY_KEYWORDS = {"berrymed", "berry", "lifesigns", "niso", "o2m", "spo2"}

    try:
        # return_adv=True gives us (BLEDevice, AdvertisementData) so we can
        # check service UUIDs broadcast in the advertisement — the v1.1 protocol
        # way to identify the device even if name is blank.
        scan_results = await BleakScanner.discover(timeout=timeout, return_adv=True)
    except Exception as exc:
        logger.error("[BLE] Scan error: %s", exc)
        return {"ok": False, "error": str(exc), "devices": []}

    # Cache every discovered BLEDevice by address so _ble_runner can skip re-scan.
    # Use dev.address (not the dict key) so the key format exactly matches what
    # we send to the browser and what comes back in /ble/connect.
    _scan_device_cache = {
        dev.address.upper(): dev for _addr, (dev, _adv) in scan_results.items()
    }

    # BerryMed OUI prefixes — all known BerryMed watch MAC ranges
    BERRY_OUI = {"00:A0:50", "AC:67:B2", "A4:C1:38"}

    devices = []
    for addr, (dev, adv) in scan_results.items():
        name = dev.name or ""
        # Priority 1 — Lifesigns service UUID (protocol-correct, works even if name is blank)
        svc_uuids = [s.lower() for s in (adv.service_uuids or [])]
        by_uuid = LIFESIGNS_SVC_UUID in svc_uuids
        # Priority 2 — known BerryMed OUI prefix in MAC address
        by_oui  = dev.address.upper()[:8] in BERRY_OUI
        # Priority 3 — device name keyword
        by_name = any(kw in name.lower() for kw in BERRY_KEYWORDS)
        is_berry = by_uuid or by_oui or by_name
        if is_berry:
            how = ("uuid" if by_uuid else "") + (" oui" if by_oui else "") + (" name" if by_name else "")
            logger.info("[BLE] BerryMed match [%s]: %s [%s]", how.strip(), name or "(unnamed)", dev.address)
        devices.append({
            "address":    dev.address,
            "name":       name or f"BerryMed {dev.address[-5:]}" if is_berry else "(unnamed)",
            "is_berrymed": is_berry,
        })

    devices.sort(key=lambda x: (not x["is_berrymed"], x["name"].lower()))
    logger.info("[BLE] Scan complete — %d devices, %d BerryMed/Lifesigns",
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
        "target_address":  _ble_target_address,
        "scan_cache_keys": list(_scan_device_cache.keys()),
        "parser_stats":    _data_parser.stats,
        "json_records":    _json_recorder.records_written,
        "browser_clients": len(browser_clients),
    }


@app.get("/ble/debug")
async def ble_debug():
    """
    One-shot diagnostic endpoint — run while the issue is happening.
    Open http://localhost:8000/ble/debug in the browser and paste the
    JSON into the issue report.
    """
    import platform, sys
    return {
        "ble_status":       {**_ble_status},
        "target_address":   _ble_target_address,
        "scan_cache_keys":  list(_scan_device_cache.keys()),
        "runner_alive":     bool(_ble_task and not _ble_task.done()),
        "manager_connected": _ble_manager.connected,
        "manager_device":   str(getattr(_ble_manager, "_device", None)),
        "parser_stats":     _data_parser.stats,
        "json_records":     _json_recorder.records_written,
        "python":           sys.version,
        "platform":         platform.platform(),
    }
=======
    broadcast(pkt.model_dump())
    return {"ok": True, "relayed": len(browser_clients)}
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd


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
<<<<<<< HEAD
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
        if _json_recorder is not None:
            _json_recorder.save_dict(pkt_dict)
        broadcast(pkt_dict)


# ---------------------------------------------------------------------------
# CLI
=======
            val = int(
                base
                + math.sin((t + phase) * 2.0 * math.pi * 0.2) * 8000
                + random.gauss(0, 200)
            )
            samples.append(val)

        proc_results = _sim_processor.process(samples)

        broadcast({
            "ts": datetime.now().isoformat(timespec="milliseconds"),
            "pkt": int(t * 4.17) % 256,
            "spo2": random.randint(95, 100),
            "hr": random.randint(60, 80),
            "pi": random.randint(5, 30),
            "bat": random.randint(60, 100),
            "status": ["ok"],
            "samples": samples,
            "filtered_samples": proc_results["filtered1"],
            "filtered_samples2": proc_results["filtered2"],
            "raw_block": proc_results["raw_block"],
            "filt_block": proc_results["filt_block"],
        })


# ---------------------------------------------------------------------------
# CLI for standalone mode
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
# ---------------------------------------------------------------------------


def main():
    global _simulate_mode
    p = argparse.ArgumentParser(description="PlethAnalysis Web Server")
<<<<<<< HEAD
    p.add_argument("--simulate", action="store_true",
                   help="Use synthetic data (no BLE hardware required)")
    p.add_argument("--connect",  action="store_true",
                   help="Auto-connect to BerryMed watch on startup")
    p.add_argument("--port", type=int, default=8000, help="HTTP port (default 8000)")
=======
    p.add_argument("--simulate", action="store_true", help="Synthetic data for testing")
    p.add_argument("--port", type=int, default=8000, help="HTTP port")
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    args = p.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format=SETTINGS.log_format)

    _simulate_mode = args.simulate
<<<<<<< HEAD

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
=======
    logger.info("Standalone web server on http://localhost:%d  (mode=%s)", args.port, "simulated" if _simulate_mode else "web-only")
    uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="info")
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd


if __name__ == "__main__":
    main()
