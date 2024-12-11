# src/core/utils/files.py
"""Utilities for file handling operations."""
import gzip
from pathlib import Path
from contextlib import contextmanager
import struct
import logging

from typing import List, Optional

logger = logging.getLogger(__name__)


@contextmanager
def managed_files(stdf_path: str, atdf_path: Optional[str] = None):
    """Context manager for handling file resources safely."""
    stdf_file = None
    atdf_file = None
    try:
        stdf_file = get_file_handle(stdf_path, 'rb')
        reset_and_check_binary(stdf_file)

        if atdf_path:
            atdf_file = get_file_handle(atdf_path, 'w')

        yield stdf_file, atdf_file

    finally:
        if stdf_file:
            stdf_file.close()
        if atdf_file:
            atdf_file.close()


def get_file_handle(file_path: str, mode: str):
    """Get appropriate file handle for regular or gzip files."""
    file_extension = Path(file_path).suffix.lower()
    if file_extension == '.gz':
        return gzip.open(file_path, mode)
    return open(file_path, mode)


def is_binary(content: bytes) -> bool:
    """Check if content is binary."""
    return b'\x00' in content


def reset_and_check_binary(file_handle) -> None:
    """Reset file pointer and verify binary content."""
    file_handle.seek(0)
    if not is_binary(file_handle.read()):
        raise ValueError("File content is not binary")
    file_handle.seek(0)


def is_file(path: str) -> bool:
    """Check if path is a valid file."""
    return Path(path).is_file()


def find_stdf_files(path: Path) -> List[Path]:
    """Find all STDF files in a directory and its subdirectories."""
    if path.is_file():
        return [path]

    stdf_files = []
    for pattern in ['*.stdf', '*.STDF']:
        stdf_files.extend(path.rglob(pattern))
    return sorted(stdf_files)  # Sort for predictable processing order
