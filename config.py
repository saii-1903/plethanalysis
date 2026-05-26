import logging
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Settings:
    device_name: str = "BerryMed"
    device_address: str = ""       # empty = auto-discover any Lifesigns device
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

    # ---------------------------------------------------------------------------
    # Customized Real-Time IIR Filters Settings
    # ---------------------------------------------------------------------------
    # Filter 1: Real-time Butterworth Bandpass (Default: 0.5Hz - 8Hz, 2nd-order)
    filter1_lowcut: float = 0.5
    filter1_highcut: float = 8.0
    filter1_order: int = 2

    # Filter 2: Real-time Butterworth Bandpass (Default: 0.5Hz - 40Hz, 4th-order)
    filter2_lowcut: float = 0.5
    filter2_highcut: float = 40.0
    filter2_order: int = 4

    # ---------------------------------------------------------------------------
    # 30-Second Block-Processing Settings (Zero-Phase / FIR Filter)
    # ---------------------------------------------------------------------------
    block_duration_sec: float = 30.0
    block_filter_lowcut: float = 0.5
    block_filter_highcut: float = 40.0
    block_filter_type: str = "fir"  # options: 'fir', 'butter_filtfilt'
    block_fir_numtaps: int = 101

    # ---------------------------------------------------------------------------
    # Stream-specific CSV Filenames
    # ---------------------------------------------------------------------------
    save_csv_raw: str = "pleth_data_raw.csv"
    save_csv_filter1: str = "pleth_data_butter_0.5_8hz.csv"
    save_csv_filter2: str = "pleth_data_butter_0.5_40hz.csv"
    save_csv_block: str = "pleth_data_block_fir_0.5_40hz.csv"


SETTINGS = Settings()
