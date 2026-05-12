import logging
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Settings:
    device_name: str = "BERRY-MED"
    scan_timeout: int = 10
    connect_timeout: int = 20

    comm_service_uuid: str = "49535343-FE7D-4AE5-8FA9-9FAFD205E455"
    send_char_uuid: str = "49535343-1E4D-4BD9-BA61-23C647249616"
    recv_char_uuid: str = "49535343-8841-43F4-A8D4-ECBE34729BB3"
    rename_char_uuid: str = "00005343-0000-1000-8000-00805F9B34FB"
    mac_char_uuid: str = "00005344-0000-1000-8000-00805F9B34FB"

    packet_length: int = 207
    head1: int = 0xFF
    head2: int = 0xAA

    cmd_original_wave: int = 0xF4
    cmd_filtered_wave: int = 0xF5
    cmd_wake_1min: int = 0xF7
    cmd_wake_3min: int = 0xF8
    cmd_wake_5min: int = 0xF9
    cmd_sw_version: int = 0xFF
    cmd_hw_version: int = 0xFE

    reconnect_delay: int = 3
    max_reconnect_attempts: int = 5
    log_level: int = logging.INFO
    log_format: str = "%(asctime)s | %(name)-18s | %(levelname)-6s | %(message)s"
    output_file: Optional[str] = None


SETTINGS = Settings()
