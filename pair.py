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
        addr = SETTINGS.device_address.strip().upper()
        if addr:
            logger.info("Scanning for address '%s' ...", addr)
            device = await BleakScanner.find_device_by_address(
                addr, timeout=timeout or SETTINGS.scan_timeout
            )
            if device is None:
                raise DiscoveryTimeout(
                    f"Device at '{addr}' not found within {timeout or SETTINGS.scan_timeout}s"
                )
        else:
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

        addr = self._device.address
        timeout = timeout or SETTINGS.connect_timeout

        # Try both address types — Windows sometimes requires "random"
        for addr_type in (None, "public", "random"):
            if addr_type is None and self._already_tried("public", "random"):
                continue
            logger.info(
                "Connecting to %s (addr_type=%s) ...", addr, addr_type or "auto"
            )
            try:
                if addr_type:
                    self._client = BleakClient(
                        addr, timeout=timeout, address_type=addr_type
                    )
                else:
                    self._client = BleakClient(self._device, timeout=timeout)
                await self._client.connect()
                self._connected = True
                logger.info("Connected to %s", addr)
                
                # Log discovered services & characteristics for diagnostics
                logger.info("Discovering services and characteristics:")
                for service in self._client.services:
                    logger.info("Service: %s", service.uuid)
                    for char in service.characteristics:
                        logger.info("  -> Characteristic: %s (properties: %s)", char.uuid, char.properties)
                
                # Allow the connection and Windows BLE stack to fully stabilize
                await asyncio.sleep(1.0)
                return
            except (BleakError, TimeoutError, asyncio.TimeoutError) as exc:
                self._connected = False
                self._client = None
                logger.warning("  attempt with %s failed: %s", addr_type or "auto", exc)

        raise ConnectionFailed(
            f"Could not connect to {addr} after retries"
        )

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
        try:
            await self._client.write_gatt_char(SETTINGS.recv_char_uuid, cmd, response=True)
        except Exception as e:
            logger.warning("Write with response failed, retrying without response: %s", e)
            try:
                await self._client.write_gatt_char(SETTINGS.recv_char_uuid, cmd, response=False)
            except Exception as e2:
                logger.error("All write attempts failed: %s", e2)
                raise e2

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _handle_notification(self, _sender: int, data: bytes) -> None:
        if self._data_callback:
            self._data_callback(data)

    def _already_tried(self, *types: str) -> bool:
        return False  # simple guard — always try the explicit types too

    async def __aenter__(self) -> BLEManager:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.disconnect()
