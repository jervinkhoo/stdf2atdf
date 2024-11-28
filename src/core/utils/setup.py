# src/core/utils/setup.py
"""Utilities for setup and initialization."""
import logging
from typing import Dict, List, Optional
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