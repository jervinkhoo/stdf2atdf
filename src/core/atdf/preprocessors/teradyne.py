# src/core/atdf/preprocessors/teradyne.py
from typing import Dict, Any

def process_teradyne(record_type: str, record_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Preprocessor for Teradyne tester data.
    Modify this file directly to add Teradyne-specific processing logic.
    """
    if record_type == 'MIR':
        # Add Teradyne-specific processing
        if 'job_name' in record_data:
            record_data['job_name'] = record_data['job_name'].upper()

        # Add any custom fields needed
        record_data['catalyst_version'] = 'CAT7.1'
        record_data['slot_number'] = 'SLOT3'

    return record_data