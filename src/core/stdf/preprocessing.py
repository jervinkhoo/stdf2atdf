# src/core/stdf/preprocessing.py
"""High-level STDF file operations."""
import struct
import logging
from typing import Tuple, Dict, Any

logger = logging.getLogger(__name__)

def determine_endianness(byte: bytes) -> str:
    """Determine endianness from STDF file byte."""
    return '>' if ord(byte) == 1 else '<'

def determine_file_params(stdf_file) -> Dict[str, Any]:
    """Determine STDF file parameters."""
    stdf_file.seek(4)
    byte = stdf_file.read(1)
    endianness = determine_endianness(byte)
    stdf_file.seek(0)
    return {'endianness': endianness}

def read_record_header(stdf_file, endianness: str) -> Tuple[int, int, int]:
    """Read and parse STDF record header."""
    header = stdf_file.read(4)
    if not header:
        return None
    return struct.unpack(endianness + 'HBB', header)