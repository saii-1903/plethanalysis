from __future__ import annotations

import asyncio
import logging
from typing import Callable, Optional

from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from bleak.exc import BleakError

from config import SETTINGS

logger = logging.getLogger(__name__)

DataCallback = Callable[[bytes], None]


class DiscoveryTimeout(Exception):
    pass


class ConnectionFailed(Exception):
    pass


class BLEManager:
    def __init__(self) -> None:
        self._client: Optional[BleakClient] = None
        self._device: Optional[BLEDevice] = None
        self._connected = False
        self._data_callback: Optional[DataCallback] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def connected(self) -> bool:
        return (
            self._connected
            and self._client is not None
            and self._client.is_connected
        )

    def on_data(self, callback: DataCallback) -> None:
        self._data_callback = callback

    async def scan(self, timeout: Optional[int] = None) -> BLEDevice:
        """
        Find the target device by MAC address (from SETTINGS).
        Raises DiscoveryTimeout if not found within the timeout.
        """
        addr = SETTINGS.device_address.strip().upper()
        t = timeout or SETTINGS.scan_timeout
        logger.info("Scanning for device %s (timeout=%ds) …", addr, t)

        device = await BleakScanner.find_device_by_address(addr, timeout=t)
        if device is None:
            raise DiscoveryTimeout(
                f"Device {addr!r} not seen within {t}s"
            )

        self._device = device
        logger.info("Found: %s [%s]", device.name, device.address)
        return device

    async def connect(self, timeout: Optional[int] = None) -> None:
        """
        Connect to the previously scanned device.
        Works with bleak 0.x and 1.x — no address_type kwarg.
        """
        if self._device is None:
            raise RuntimeError("Call scan() before connect()")

        # Tear down any stale client before trying again
        await self._safe_disconnect()

        t = timeout or SETTINGS.connect_timeout
        logger.info("Connecting to %s (timeout=%ds) …", self._device.address, t)

        try:
            self._client = BleakClient(self._device, timeout=t)
            await self._client.connect()
        except (BleakError, TimeoutError, asyncio.TimeoutError, OSError) as exc:
            self._client = None
            raise ConnectionFailed(
                f"Could not connect to {self._device.address}: {exc}"
            ) from exc

        self._connected = True
        logger.info("Connected to %s", self._device.address)

        # Log services for diagnostics
        for svc in self._client.services:
            logger.debug("  Service %s", svc.uuid)
            for ch in svc.characteristics:
                logger.debug("    Char %s  props=%s", ch.uuid, ch.properties)

        # Let the Windows BLE stack settle
        await asyncio.sleep(0.8)

    async def disconnect(self) -> None:
        await self._safe_disconnect()
        logger.info("Disconnected")

    async def start_stream(self) -> None:
        if not self.connected:
            raise RuntimeError("Not connected — call connect() first")
        logger.info("Subscribing to %s", SETTINGS.send_char_uuid)
        await self._client.start_notify(
            SETTINGS.send_char_uuid, self._handle_notification
        )
        logger.info("Stream started — listening for data")

    async def stop_stream(self) -> None:
        if self._client and self._client.is_connected:
            try:
                await self._client.stop_notify(SETTINGS.send_char_uuid)
            except Exception:
                pass
        logger.info("Stream stopped")

    async def send_command(self, cmd: bytes) -> None:
        if not self.connected:
            raise RuntimeError("Not connected")
        logger.info("Sending command: 0x%s", cmd.hex().upper())
        try:
            await self._client.write_gatt_char(
                SETTINGS.recv_char_uuid, cmd, response=True
            )
            logger.info("Command sent (with-response)")
        except Exception as e1:
            logger.warning("write with-response failed (%s) — retrying without", e1)
            try:
                await self._client.write_gatt_char(
                    SETTINGS.recv_char_uuid, cmd, response=False
                )
                logger.info("Command sent (without-response)")
            except Exception as e2:
                logger.error("Command write failed entirely: %s", e2)
                raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _handle_notification(self, _sender: int, data: bytes) -> None:
        if self._data_callback:
            self._data_callback(data)

    async def _safe_disconnect(self) -> None:
        """Silently tear down any existing client."""
        if self._client:
            try:
                if self._client.is_connected:
                    await self._client.disconnect()
            except Exception:
                pass
            self._client = None
        self._connected = False

    async def __aenter__(self) -> BLEManager:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.disconnect()
