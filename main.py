#!/usr/bin/env python3
"""
PlethAnalysis — Master orchestrator.

Scans for a BERRY-MED BLE device, connects, streams data,
parses it per Lifesigns Protocol v1.1, and forwards structured records.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

from config import SETTINGS
from dataparse import DataParser, PlethPacket
from pair import BLEManager, ConnectionFailed, DiscoveryTimeout

logger = logging.getLogger("main")


class MainController:
    def __init__(self, output_file: Optional[Path] = None, cmd: Optional[int] = None) -> None:
        self._ble = BLEManager()
        self._parser = DataParser()
        self._output_file = output_file
        self._cmd = cmd
        self._running = False
        self._output_handle: Optional[object] = None

    async def run(self) -> int:
        self._running = True
        self._open_output()

        try:
            await self._ble.scan()
            await self._ble.connect()

            if self._cmd is not None:
                await self._ble.send_command(self._parser.send_command(self._cmd))

            self._ble.on_data(self._on_data_received)
            await self._ble.start_stream()

            logger.info("Listening for data. Press Ctrl+C to stop.")
            while self._running:
                await asyncio.sleep(0.5)

        except DiscoveryTimeout:
            logger.error("Device not found — check it is powered on and in range")
            return 1
        except ConnectionFailed as exc:
            logger.error("Connection failed: %s", exc)
            return 1
        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.info("Shutting down …")
        finally:
            await self._shutdown()

        self._log_stats()
        return 0

    def _on_data_received(self, data: bytes) -> None:
        packet = self._parser.feed(data)
        if packet is not None and packet.valid:
            self._emit(packet)

    def _open_output(self) -> None:
        if self._output_file:
            self._output_handle = open(self._output_file, "a")
            logger.info("Logging parsed data to %s", self._output_file)

    def _emit(self, pkt: PlethPacket) -> None:
        line = (
            f"{pkt.timestamp.isoformat(timespec='milliseconds')},"
            f"{pkt.pkt_index},{','.join(pkt.status_str) or 'ok'},"
            f"{pkt.spo2 or ''},{pkt.spo2_real or ''},"
            f"{pkt.pulse_rate or ''},{pkt.pulse_rate_real or ''},"
            f"{pkt.perfu_index or ''},{pkt.perfu_index_real or ''},"
            f"{pkt.axis_x},{pkt.axis_y},{pkt.axis_z},"
            f"{pkt.battery}"
        )
        print(line)
        if self._output_handle:
            self._output_handle.write(line + "\n")
            self._output_handle.flush()

    async def _shutdown(self) -> None:
        await self._ble.stop_stream()
        await self._ble.disconnect()
        if self._output_handle:
            self._output_handle.close()

    def _log_stats(self) -> None:
        stats = self._parser.stats
        logger.info(
            "Session complete — received=%d  parsed=%d  csum_bad=%d",
            stats["received"],
            stats["parsed"],
            stats["checksum_bad"],
        )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="PlethAnalysis — BERRY-MED BLE data collector")
    p.add_argument(
        "-o", "--output",
        type=Path,
        help="Append parsed CSV data to FILE",
        metavar="FILE",
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    p.add_argument(
        "--waveform",
        choices=["original", "filtered"],
        default="original",
        help="ADC waveform mode to send to device (default: original)",
    )
    return p


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else SETTINGS.log_level
    logging.basicConfig(level=level, format=SETTINGS.log_format)


def main() -> None:
    args = _build_parser().parse_args()
    _setup_logging(args.verbose)

    cmd_map = {"original": SETTINGS.cmd_original_wave, "filtered": SETTINGS.cmd_filtered_wave}
    cmd = cmd_map[args.waveform]

    controller = MainController(output_file=args.output, cmd=cmd)

    try:
        exit_code = asyncio.run(controller.run())
    except KeyboardInterrupt:
        exit_code = 0

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
