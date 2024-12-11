# src/core/atdf/preprocessors/eagle.py
from typing import Dict, Any

def process_eagle(record_type: str, record_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Preprocessor for Eagle test system data.
    Modify this file directly to add Eagle-specific processing logic.
    """
    if record_type == 'MIR':
        # Add Eagle-specific processing
        if 'lot_id' in record_data:
            record_data['lot_id'] = f"EGL_{record_data['lot_id']}"

        # Add any custom fields needed
        record_data['eagle_system'] = 'ETS-800'
        record_data['probe_card'] = 'PC123'

    return record_data