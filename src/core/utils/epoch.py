# src/core/utils/epoch.py
"""General utility functions not specific to STDF/ATDF."""
from datetime import datetime
import pytz
import logging

from typing import Optional

logger = logging.getLogger(__name__)


def convert_epoch_to_datetime(epoch_time: int,
                            dt_format: Optional[str] = None,
                            timezone_string: str = 'UTC') -> str:
    """Convert epoch time to formatted datetime string."""
    try:
        from django.utils import timezone
        selected_timezone = timezone.get_current_timezone()
    except ImportError:
        selected_timezone = pytz.timezone(timezone_string)

    dt = datetime.fromtimestamp(epoch_time, tz=selected_timezone)

    if dt_format == 'atdf':
        return dt.strftime('%H:%M:%S %d-%b-%Y').upper()
    elif dt_format == 'sqlite':
        return dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    elif dt_format:
        return dt.strftime('%Y-%m-%d %H:%M:%S%z')

    return dt