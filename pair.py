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

# Lifesigns Protocol v1.1 — comm service UUID (used for advertisement detection)
LIFESIGNS_SERVICE_UUID = "49535343-fe7d-4ae5-8fa9-9fafd205e455"


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
        Discover any Lifesigns/BerryMed device using FOUR methods in order:

        1. By MAC address (SETTINGS.device_address) — fastest when known
        2. By Lifesigns service UUID 49535343-FE7D-... — protocol-correct,
           works even when the device advertises without a name
        3. By BerryMed OUI prefix (00:A0:50 / AC:67:B2) — hardware ID
        4. By device name containing "BerryMed" / "Lifesigns" / "niso"

        The watch MUST be worn (skin contact) to advertise.
        Raises DiscoveryTimeout if nothing is found within the timeout.
        """
        t = timeout or SETTINGS.scan_timeout
        addr = SETTINGS.device_address.strip().upper()

        logger.info("[SCAN] Starting %.0fs scan (addr=%s) …", t, addr or "any")

        # Single discover pass — return_adv=True gives advertisement data
        # so we can check service UUIDs per Lifesigns Protocol v1.1
        results = await BleakScanner.discover(timeout=t, return_adv=True)

        device = self._pick_device(results, addr)

        if device is None:
            raise DiscoveryTimeout(
                f"No BerryMed/Lifesigns device found in {t}s — "
                "make sure the watch is ON and worn on the wrist/finger."
            )

        self._device = device
        # Remember address for reconnects
        SETTINGS.device_address = device.address
        logger.info("[SCAN] Found: %s [%s]", device.name or "(unnamed)", device.address)
        return device

    def _pick_device(self, results: dict, target_addr: str) -> Optional[BLEDevice]:
        """
        Lifesigns Protocol v1.1 — device identification priority:
          1. Exact address match  (fastest when known)
          2. Lifesigns Comm Service UUID 49535343-FE7D-...  (protocol-correct)
          3. BerryMed OUI prefix  00:A0:50 / AC:67:B2  (reliable hardware ID)
          4. Name keyword  berrymed / lifesigns / niso
        """
        BERRY_OUI = {"00:A0:50", "AC:67:B2", "A4:C1:38"}

        by_svc  = []
        by_oui  = []
        by_name = []

        for addr, (dev, adv) in results.items():
            addr_up = addr.upper()

            # Priority 1 — exact address
            if target_addr and addr_up == target_addr:
                logger.info("[SCAN] Matched by address: %s", addr_up)
                return dev

            # Priority 2 — Lifesigns Comm Service UUID (v1.1 spec)
            svc_uuids = [s.lower() for s in (adv.service_uuids or [])]
            if LIFESIGNS_SERVICE_UUID in svc_uuids:
                logger.info("[SCAN] Matched by service UUID: %s [%s]",
                            dev.name or "(unnamed)", addr_up)
                by_svc.append(dev)
                continue

            # Priority 3 — BerryMed OUI prefix
            if addr_up[:8] in BERRY_OUI:
                logger.info("[SCAN] Matched by OUI prefix: %s [%s]",
                            dev.name or "(unnamed)", addr_up)
                by_oui.append(dev)
                continue

            # Priority 4 — name hint (fallback)
            name = (dev.name or "").lower()
            if "berrymed" in name or "lifesigns" in name or "niso" in name:
                logger.info("[SCAN] Matched by name: %s [%s]", dev.name, addr_up)
                by_name.append(dev)

        for bucket in (by_svc, by_oui, by_name):
            if bucket:
                return bucket[0]
        return None

    async def connect(self, timeout: Optional[int] = None) -> None:
        """Connect to the previously scanned device (bleak 1.x compatible)."""
        if self._device is None:
            raise RuntimeError("Call scan() before connect()")

        await self._safe_disconnect()

        t = timeout or SETTINGS.connect_timeout
        logger.info("[BLE] Connecting to %s …", self._device.address)

        try:
            self._client = BleakClient(self._device, timeout=t)
            await self._client.connect()
        except (BleakError, TimeoutError, asyncio.TimeoutError, OSError) as exc:
            self._client = None
            raise ConnectionFailed(
                f"Connect failed for {self._device.address}: {exc}"
            ) from exc

        self._connected = True
        logger.info("[BLE] Connected to %s", self._device.address)

        # Log services at DEBUG level
        for svc in self._client.services:
            logger.debug("  Service %s", svc.uuid)
            for ch in svc.characteristics:
                logger.debug("    Char %s  props=%s", ch.uuid, ch.properties)

        # Let the Windows BLE stack settle
        await asyncio.sleep(0.8)

    async def disconnect(self) -> None:
        await self._safe_disconnect()
        logger.info("[BLE] Disconnected")

    async def start_stream(self) -> None:
        if not self.connected:
            raise RuntimeError("Not connected — call connect() first")
        logger.info("[BLE] Subscribing to %s", SETTINGS.send_char_uuid)
        await self._client.start_notify(
            SETTINGS.send_char_uuid, self._handle_notification
        )
        logger.info("[BLE] Stream started")

    async def stop_stream(self) -> None:
        if self._client and self._client.is_connected:
            try:
                await self._client.stop_notify(SETTINGS.send_char_uuid)
            except Exception:
                pass
        logger.info("[BLE] Stream stopped")

    async def send_command(self, cmd: bytes) -> None:
        if not self.connected:
            raise RuntimeError("Not connected")
        logger.info("[BLE] Sending command 0x%s", cmd.hex().upper())
        try:
            await self._client.write_gatt_char(
                SETTINGS.recv_char_uuid, cmd, response=True
            )
        except Exception as e1:
            logger.warning("[BLE] write(response=True) failed: %s — retrying", e1)
            try:
                await self._client.write_gatt_char(
                    SETTINGS.recv_char_uuid, cmd, response=False
                )
            except Exception as e2:
                logger.error("[BLE] Command write failed: %s", e2)
                raise

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _handle_notification(self, _sender: int, data: bytes) -> None:
        if self._data_callback:
            self._data_callback(data)

    async def _safe_disconnect(self) -> None:
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
