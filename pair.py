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
<<<<<<< HEAD
        """
        Discover the BerryMed/Lifesigns device using THREE methods in order:

        1. By MAC address (SETTINGS.device_address) — fastest when known
        2. By Lifesigns service UUID (49535343-FE7D-...) — protocol-correct,
           works even when the device advertises without a name
        3. By device name containing "BerryMed" or "Lifesigns"

        The watch MUST be worn (skin contact) to advertise.
        Raises DiscoveryTimeout if nothing is found within the timeout.
        """
        t = timeout or SETTINGS.scan_timeout
        addr = SETTINGS.device_address.strip().upper()

        logger.info("[SCAN] Starting %.0fs scan (addr=%s) …", t, addr or "any")

        # Run a single discover pass — check all three criteria in one sweep
        results = await BleakScanner.discover(timeout=t, return_adv=True)

        device = self._pick_device(results, addr)

        if device is None:
            raise DiscoveryTimeout(
                f"No BerryMed/Lifesigns device found in {t}s — "
                "make sure the watch is ON and worn on the wrist/finger."
            )

=======
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
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd
        self._device = device
        # Update SETTINGS so the address is remembered for reconnects
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
        # Known BerryMed MAC OUI prefixes (first 3 bytes = 8 chars with colons)
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
                continue          # best non-address match, skip lower priorities

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

<<<<<<< HEAD
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
=======
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
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd

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
<<<<<<< HEAD
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
=======
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
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _handle_notification(self, _sender: int, data: bytes) -> None:
        if self._data_callback:
            self._data_callback(data)

<<<<<<< HEAD
    async def _safe_disconnect(self) -> None:
        if self._client:
            try:
                if self._client.is_connected:
                    await self._client.disconnect()
            except Exception:
                pass
            self._client = None
        self._connected = False
=======
    def _already_tried(self, *types: str) -> bool:
        return False  # simple guard — always try the explicit types too
>>>>>>> c8363ed4979902440fd9c1607725b6f5f7617acd

    async def __aenter__(self) -> BLEManager:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.disconnect()
