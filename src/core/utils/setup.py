# src/core/utils/setup.py
"""Utilities for setup and initialization."""
import logging
import struct
from typing import Dict, List, Optional, Any, Tuple
from .files import is_file
from .templates import get_record_types

logger = logging.getLogger(__name__)

def validate_input_file(input_stdf_file: str) -> None:
    """Validate input STDF file existence."""
    if not is_file(input_stdf_file):
        message = f"File {input_stdf_file} does not exist"
        logger.error(message)
        raise ValueError(message)

def initialize_record_entries() -> Dict[str, list]:
    """Initialize record lists for each record type."""
    return {record_type: [] for record_type in get_record_types()}

def setup_record_flags(records_to_process: Optional[List[str]]) -> Dict[str, bool]:
    """Set up record processing flags."""
    record_flags = {record_type: True for record_type in get_record_types()}
    if records_to_process:
        record_flags.update({record_type: False for record_type in get_record_types()})
        record_flags.update({rec: True for rec in records_to_process if rec in record_flags})
    return record_flags


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
