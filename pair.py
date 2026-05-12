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
        return self._connected and self._client is not None and self._client.is_connected

    def on_data(self, callback: DataCallback) -> None:
        self._data_callback = callback

    async def scan(self, timeout: Optional[int] = None) -> BLEDevice:
        logger.info("Scanning for device '%s' ...", SETTINGS.device_name)
        device = await BleakScanner.find_device_by_name(
            SETTINGS.device_name, timeout=timeout or SETTINGS.scan_timeout
        )
        if device is None:
            raise DiscoveryTimeout(
                f"Device '{SETTINGS.device_name}' not found within {timeout or SETTINGS.scan_timeout}s"
            )
        self._device = device
        logger.info("Found device: %s [%s]", device.name, device.address)
        return device

    async def connect(self, timeout: Optional[int] = None) -> None:
        if self._device is None:
            raise RuntimeError("Call scan() before connect()")
        if self.connected:
            logger.warning("Already connected")
            return

        logger.info("Connecting to %s ...", self._device.address)
        self._client = BleakClient(self._device, timeout=timeout or SETTINGS.connect_timeout)
        try:
            await self._client.connect()
            self._connected = True
            logger.info("Connected to %s", self._device.address)
        except BleakError as exc:
            self._connected = False
            raise ConnectionFailed(str(exc)) from exc

    async def disconnect(self) -> None:
        if self._client and self._client.is_connected:
            await self._client.disconnect()
        self._connected = False
        logger.info("Disconnected")

    async def start_stream(self) -> None:
        if not self.connected:
            raise RuntimeError("Not connected")
        logger.info("Subscribing to notifications on %s", SETTINGS.send_char_uuid)
        await self._client.start_notify(SETTINGS.send_char_uuid, self._handle_notification)
        logger.info("Stream started – listening for data")

    async def stop_stream(self) -> None:
        if self._client and self._client.is_connected:
            await self._client.stop_notify(SETTINGS.send_char_uuid)
        logger.info("Stream stopped")

    async def send_command(self, cmd: bytes) -> None:
        if not self.connected:
            raise RuntimeError("Not connected")
        logger.info("Sending command: %s", cmd.hex())
        await self._client.write_gatt_char(SETTINGS.recv_char_uuid, cmd, response=True)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _handle_notification(self, _sender: int, data: bytes) -> None:
        if self._data_callback:
            self._data_callback(data)

    async def __aenter__(self) -> BLEManager:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.disconnect()
