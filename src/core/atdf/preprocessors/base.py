# src/core/atdf/preprocessors/base.py

from typing import Dict, Any, Optional
import logging
from .advantest import process_advantest
from .teradyne import process_teradyne
from .eagle import process_eagle

logger = logging.getLogger(__name__)


def preprocess_record(record_type: str, record_data: Dict[str, Any],
                      preprocessor_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Preprocess a single record before it's added to the collection.

    Args:
        record_type: Type of record (MIR, SDR, etc)
        record_data: Dictionary containing record data
        preprocessor_type: String identifier for which preprocessor to use

    Returns:
        Modified record data dictionary
    """
    if not preprocessor_type:
        return record_data

    try:
        preprocessor = get_preprocessor(preprocessor_type)
        return preprocessor(record_type, record_data)
    except Exception as e:
        logger.error(f"Error during preprocessing: {str(e)}")
        return record_data


def get_preprocessor(preprocessor_type: str):
    """
    Factory function to get the appropriate preprocessor based on type.
    """
    preprocessors = {
        'advantest': process_advantest,
        'teradyne': process_teradyne,
        'eagle': process_eagle
    }

    if preprocessor_type not in preprocessors:
        raise ValueError(f"Unknown preprocessor type: {preprocessor_type}")

    return preprocessors[preprocessor_type]