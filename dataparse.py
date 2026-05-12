from __future__ import annotations

import logging
import struct
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntFlag
from typing import Optional

from config import SETTINGS

logger = logging.getLogger(__name__)

NUM_ADC_SAMPLES = 48
ADC_BYTES = 4
HEADER_SIZE = 10
ADC_REGION_SIZE = NUM_ADC_SAMPLES * ADC_BYTES  # 192
TAIL_SIZE = 5
EXPECTED_SIZE = HEADER_SIZE + ADC_REGION_SIZE + TAIL_SIZE  # 207


class StatusFlag(IntFlag):
    SPO2_SENSOR_OFF = 0x01
    NO_FINGER = 0x02
    NO_PULSE_SIGNAL = 0x04
    PULSE_BEAT_TIPS = 0x08


@dataclass
class PlethPacket:
    timestamp: datetime = field(default_factory=datetime.now)
    raw_bytes: bytes = b""

    pkt_index: int = 0
    status: StatusFlag = StatusFlag(0)
    status_str: list[str] = field(default_factory=list)

    spo2: Optional[int] = None
    spo2_real: Optional[int] = None
    pulse_rate: Optional[int] = None
    pulse_rate_real: Optional[int] = None
    perfu_index: Optional[int] = None
    perfu_index_real: Optional[int] = None

    adc_samples: list[int] = field(default_factory=list)

    axis_x: int = 0
    axis_y: int = 0
    axis_z: int = 0
    battery: int = 0

    checksum: int = 0
    checksum_ok: bool = False
    valid: bool = False


def _invalid_or(value: int, sentinel: int) -> bool:
    return value == sentinel


class DataParser:
    def __init__(self) -> None:
        self._buffer = bytearray()
        self._packets_received = 0
        self._packets_parsed = 0
        self._packets_csum_bad = 0

    @property
    def stats(self) -> dict:
        return {
            "received": self._packets_received,
            "parsed": self._packets_parsed,
            "checksum_bad": self._packets_csum_bad,
        }

    def feed(self, data: bytes) -> Optional[PlethPacket]:
        self._buffer.extend(data)
        self._packets_received += 1
        return self._try_parse()

    def send_command(self, cmd: int) -> bytes:
        return struct.pack("<B", cmd)

    def _try_parse(self) -> Optional[PlethPacket]:
        if len(self._buffer) < EXPECTED_SIZE:
            return None

        head1, head2 = self._buffer[0], self._buffer[1]
        if head1 != SETTINGS.head1 or head2 != SETTINGS.head2:
            self._buffer.pop(0)
            return self._try_parse()

        if len(self._buffer) < EXPECTED_SIZE:
            return None

        packet_bytes = bytes(self._buffer[:EXPECTED_SIZE])
        self._buffer = self._buffer[EXPECTED_SIZE:]

        pkt = self._unpack(packet_bytes)
        if pkt is not None:
            self._packets_parsed += 1
            if not pkt.checksum_ok:
                self._packets_csum_bad += 1
        return pkt

    def _unpack(self, data: bytes) -> Optional[PlethPacket]:
        if len(data) != EXPECTED_SIZE:
            return None

        pkt = PlethPacket(raw_bytes=data)

        pkt.pkt_index = data[2]
        pkt.status = StatusFlag(data[3])

        flags = []
        if pkt.status & StatusFlag.SPO2_SENSOR_OFF:
            flags.append("sensor_off")
        if pkt.status & StatusFlag.NO_FINGER:
            flags.append("no_finger")
        if pkt.status & StatusFlag.NO_PULSE_SIGNAL:
            flags.append("no_pulse")
        if pkt.status & StatusFlag.PULSE_BEAT_TIPS:
            flags.append("pulse_beat")
        pkt.status_str = flags

        pkt.spo2 = self._opt_val(data[4], 127)
        pkt.spo2_real = self._opt_val(data[5], 127)
        pkt.pulse_rate = self._opt_val(data[6], 255)
        pkt.pulse_rate_real = self._opt_val(data[7], 255)
        pkt.perfu_index = self._opt_val(data[8], 0)
        pkt.perfu_index_real = self._opt_val(data[9], 0)

        samples = []
        for i in range(NUM_ADC_SAMPLES):
            offset = HEADER_SIZE + i * ADC_BYTES
            val = struct.unpack_from("<i", data, offset)[0]
            samples.append(val)
        pkt.adc_samples = samples

        pkt.axis_x = struct.unpack_from("<b", data, 202)[0]
        pkt.axis_y = struct.unpack_from("<b", data, 203)[0]
        pkt.axis_z = struct.unpack_from("<b", data, 204)[0]
        pkt.battery = data[205]

        pkt.checksum = data[206]
        calc_csum = sum(data[:206]) % 256
        pkt.checksum_ok = pkt.checksum == calc_csum
        pkt.valid = pkt.checksum_ok

        return pkt

    @staticmethod
    def _opt_val(value: int, sentinel: int) -> Optional[int]:
        return None if value == sentinel else value

    def flush(self) -> None:
        self._buffer.clear()
