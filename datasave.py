"""
datasave.py — Lifesigns Protocol v1.1 JSON Recorder

Converts a parsed PlethPacket (or an IncomingPacket dict from the web
endpoint) into a structured JSON record whose field names mirror the
Lifesigns Protocol v1.1 specification exactly, then appends it to a
JSON-Lines file (one complete JSON object per line).

Packet field mapping (v1.1 spec → JSON key):
  Byte2  PktIndex          → pktIndex
  Byte3  Status            → status.raw / status.flags
  Byte4  SpO2Sat           → spo2.averaged
  Byte5  SpO2SatReal       → spo2.realtime
  Byte6  PulseRate         → pulseRate.averaged
  Byte7  PulseRateReal     → pulseRate.realtime
  Byte8  PerfuIndex        → perfusionIndex.averaged
  Byte9  PerfuIndexReal    → perfusionIndex.realtime
  Byte10–201 ADCSample[0–47] → adcSamples  (signed int32 × 48)
  Byte202 Axis-X           → accelerometer.x  (signed int8, –128 … 127)
  Byte203 Axis-Y           → accelerometer.y
  Byte204 Axis-Z           → accelerometer.z
  Byte205 Battery          → battery.level
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Default output path (next to this file)
# --------------------------------------------------------------------------
DEFAULT_JSON_PATH = Path(__file__).parent / "lifesigns_v1_1_data.jsonl"


# --------------------------------------------------------------------------
# Status flag names (matches dataparse.py StatusFlag bit positions)
# --------------------------------------------------------------------------
_STATUS_FLAGS = {
    0x01: "SpO2SensorOff",
    0x02: "NoFinger",
    0x04: "NoPulseSignal",
    0x08: "PulseBeatTip",
}


def _decode_status_flags(raw: int) -> list[str]:
    return [name for bit, name in _STATUS_FLAGS.items() if raw & bit]


# --------------------------------------------------------------------------
# Record builders
# --------------------------------------------------------------------------

def build_record_from_packet(pkt) -> dict:
    """
    Build a v1.1-structured JSON record from a *PlethPacket* dataclass
    (as returned by DataParser.feed()).
    """
    status_raw = int(pkt.status)
    return {
        "timestamp": pkt.timestamp.isoformat(timespec="milliseconds"),
        "deviceID": "BerryMed",
        "protocolVersion": "Lifesigns_v1.1",
        "pktIndex": pkt.pkt_index,
        "status": {
            "raw": status_raw,
            "flags": _decode_status_flags(status_raw),
        },
        # ── SpO2 ──────────────────────────────────────────────────────────
        "spo2": {
            "averaged":  pkt.spo2,        # None when invalid (sentinel 127)
            "realtime":  pkt.spo2_real,
            "unit":      "%",
            "invalidSentinel": 127,
        },
        # ── Pulse Rate ────────────────────────────────────────────────────
        "pulseRate": {
            "averaged":  pkt.pulse_rate,   # None when invalid (sentinel 255)
            "realtime":  pkt.pulse_rate_real,
            "unit":      "bpm",
            "invalidSentinel": 255,
        },
        # ── Perfusion Index ───────────────────────────────────────────────
        "perfusionIndex": {
            "averaged":  pkt.perfu_index,  # None when invalid (sentinel 0)
            "realtime":  pkt.perfu_index_real,
            "unit":      "‰",
            "invalidSentinel": 0,
        },
        # ── ADC Waveform Samples (48 × signed int32) ──────────────────────
        "adcSamples": {
            "count":   len(pkt.adc_samples),
            "values":  pkt.adc_samples,
            "type":    "signed_int32",
            "rangeMin": -(2**31),
            "rangeMax":   2**31 - 1,
        },
        # ── Accelerometer (Triaxial, signed int8) ─────────────────────────
        "accelerometer": {
            "x":       pkt.axis_x,
            "y":       pkt.axis_y,
            "z":       pkt.axis_z,
            "type":    "signed_int8",
            "rangeMin": -128,
            "rangeMax":  127,
        },
        # ── Battery ───────────────────────────────────────────────────────
        "battery": {
            "level": pkt.battery,
            "unit":  "%",
        },
    }


def build_record_from_dict(d: dict) -> dict:
    """
    Build a v1.1-structured JSON record from the flat *IncomingPacket*
    dict that arrives at the server's POST /data endpoint.

    Expected keys (all optional — None/0 used as fallback):
        ts, pkt, status, spo2, spo2r, hr, hrr, pi, pir,
        ax, ay, az, bat, samples
    """
    raw_status = d.get("status_raw", 0)
    # The web model passes status as a list of flag strings; we re-encode
    # the raw int only if it was explicitly forwarded; otherwise decode from list.
    flags: list[str] = d.get("status", [])
    if isinstance(flags, int):
        raw_status = flags
        flags = _decode_status_flags(raw_status)

    ts = d.get("ts") or datetime.now().isoformat(timespec="milliseconds")

    return {
        "timestamp":       ts,
        "deviceID":        d.get("deviceID", "BerryMed"),
        "protocolVersion": "Lifesigns_v1.1",
        "pktIndex":        d.get("pkt", 0),
        "status": {
            "raw":   raw_status,
            "flags": flags,
        },
        # ── SpO2 ──────────────────────────────────────────────────────────
        "spo2": {
            "averaged":       d.get("spo2"),
            "realtime":       d.get("spo2r"),
            "unit":           "%",
            "invalidSentinel": 127,
        },
        # ── Pulse Rate ────────────────────────────────────────────────────
        "pulseRate": {
            "averaged":       d.get("hr"),
            "realtime":       d.get("hrr"),
            "unit":           "bpm",
            "invalidSentinel": 255,
        },
        # ── Perfusion Index ───────────────────────────────────────────────
        "perfusionIndex": {
            "averaged":       d.get("pi"),
            "realtime":       d.get("pir"),
            "unit":           "‰",
            "invalidSentinel": 0,
        },
        # ── ADC Waveform Samples (48 × signed int32) ──────────────────────
        "adcSamples": {
            "count":    len(d.get("samples", [])),
            "values":   d.get("samples", []),
            "type":     "signed_int32",
            "rangeMin": -(2**31),
            "rangeMax":   2**31 - 1,
        },
        # ── Accelerometer (Triaxial, signed int8) ─────────────────────────
        "accelerometer": {
            "x":       d.get("ax", 0),
            "y":       d.get("ay", 0),
            "z":       d.get("az", 0),
            "type":    "signed_int8",
            "rangeMin": -128,
            "rangeMax":  127,
        },
        # ── Battery ───────────────────────────────────────────────────────
        "battery": {
            "level": d.get("bat", 0),
            "unit":  "%",
        },
    }


# --------------------------------------------------------------------------
# Recorder  (proper JSON array writer)
# --------------------------------------------------------------------------

class JSONRecorder:
    """
    Writes a valid JSON array file — one Lifesigns v1.1 record per element.

    File structure:
        [
          { ...packet 0... },
          { ...packet 1... },
          ...
          { ...packet N... }
        ]

    The array is opened on construction and closed (terminated with `]`)
    when close() is called.  The file is readable as standard JSON by any
    tool (Python json.load, Excel Power Query, Postman, jq, etc.).

    Usage:
        recorder = JSONRecorder()                  # default path
        recorder = JSONRecorder("session.json")    # custom path

        recorder.save_packet(pkt)   # from BLE pipeline
        recorder.save_dict(d)       # from web /data endpoint
        recorder.close()            # finalises the JSON array
    """

    def __init__(self, path: Union[str, Path, None] = None) -> None:
        self.path = Path(path) if path else DEFAULT_JSON_PATH
        # Give default file a .json extension
        if self.path == DEFAULT_JSON_PATH:
            self.path = self.path.with_suffix(".json")
        elif self.path.suffix == ".jsonl":
            self.path = self.path.with_suffix(".json")
        self._fh = None
        self._count = 0
        self._open()

    # ── lifecycle ──────────────────────────────────────────────────────────

    def _open(self) -> None:
        try:
            self._fh = open(self.path, "w", encoding="utf-8")
            self._fh.write("[\n")   # open the JSON array
            self._fh.flush()
            logger.info("JSONRecorder: writing to %s", self.path)
        except OSError as exc:
            logger.error("JSONRecorder: cannot open %s — %s", self.path, exc)
            self._fh = None

    def close(self) -> None:
        if self._fh:
            try:
                # Close the JSON array cleanly
                if self._count == 0:
                    self._fh.write("]")          # empty array
                else:
                    self._fh.write("\n]")         # close after last record
                self._fh.flush()
            except OSError:
                pass
            self._fh.close()
            self._fh = None
            logger.info("JSONRecorder: closed (%d records) → %s",
                        self._count, self.path)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ── public save methods ────────────────────────────────────────────────

    def save_packet(self, pkt) -> bool:
        """Save from a PlethPacket dataclass (BLE path)."""
        return self._write(build_record_from_packet(pkt))

    def save_dict(self, d: dict) -> bool:
        """Save from a flat IncomingPacket dict (web /data path)."""
        return self._write(build_record_from_dict(d))

    # ── internal ───────────────────────────────────────────────────────────

    def _write(self, record: dict) -> bool:
        if self._fh is None:
            return False
        try:
            # Separate records with commas; no trailing comma on last record
            prefix = "  " if self._count == 0 else ",\n  "
            self._fh.write(prefix + json.dumps(record, ensure_ascii=False))
            self._fh.flush()
            self._count += 1
            return True
        except OSError as exc:
            logger.error("JSONRecorder: write error — %s", exc)
            return False

    @property
    def records_written(self) -> int:
        return self._count
