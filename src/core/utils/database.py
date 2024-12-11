# database.py
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime
from .epoch import convert_epoch_to_datetime
from typing import Optional, Dict, List, Any

logger = logging.getLogger(__name__)

# Record type groupings for the new schema
RECORD_GROUPS = {
    'file_metadata': ['FAR', 'ATR'],          # File attributes and audit trail
    'test_sessions': ['MIR', 'MRR'],  # Master Information/Results
    'wafer_info': ['WIR', 'WRR', 'WCR'],      # Wafer Information/Results/Config
    'device_info': ['PIR', 'PRR'],  # Part Information/Results
    'retest_info': ['RDR'],                   # Retest Data
    'test_results': ['PTR', 'FTR', 'MPR'],  # Test Results
    'pin_configurations': ['PMR', 'PGR', 'PLR'],  # Pin Related
    'bin_definitions': ['SBR', 'HBR'],  # Bin Related
    'bin_summaries': ['PCR'],  # Part Count Records
    'test_summaries': ['TSR'],  # Test Synopsis
    'test_configuration': ['SDR'],  # Site Description
    'program_sections': ['BPS', 'EPS'],  # Program Sections
    'generic_data': ['GDR', 'DTR']  # Generic Data and Text
}

# Map of fields to handle specially (like timestamps)
TIMESTAMP_FIELDS = ['modification_timestamp', 'setup_time', 'start_time', 'finish_time']


def transform_record_data(record_type: str, data: dict) -> dict:
    """Transform record data based on record type for the new schema."""
    # Start with basic metadata
    transformed_data = {
        'original_record_type': record_type,
        'created_at': datetime.now(),
        'file_id': None,  # Links to source file
        'test_session_id': None  # Links to test session (MIR/MRR group)
    }

    # Add type-specific relationship fields
    if record_type in ['WIR', 'WRR']:
        transformed_data['wafer_id'] = None  # Will be populated with full wafer ID
    elif record_type in ['PIR', 'PRR']:
        transformed_data.update({
            'full_wafer_id': None,  # Links to wafer
            'part_id': None  # Unique part identifier
        })
    elif record_type in ['PTR', 'FTR', 'MPR']:
        transformed_data.update({
            'part_id': None,  # Links to part
            'test_id': None  # Unique test identifier
        })

    # Copy original data at the end to preserve all fields
    transformed_data.update(data.copy())

    # Handle timestamp conversions after copying data
    for field in TIMESTAMP_FIELDS:
        if field in transformed_data:
            transformed_data[field] = convert_epoch_to_datetime(transformed_data[field])

    return transformed_data


def get_table_name_for_record(record_type: str) -> str:
    """Get the appropriate table name for a record type."""
    for table, records in RECORD_GROUPS.items():
        if record_type in records:
            return table
    return f'table_{record_type}'  # Fallback for unhandled record types


def create_database_from_atdf(output_atdf_database: str, atdf_processed_entries: Dict[str, List[Dict]]):
    """Create SQLite database from ATDF records using the new schema."""
    engine = create_engine(f"sqlite:///{output_atdf_database}")
    logger.info(f"Creating database at {output_atdf_database}")

    # Generate a unique identifier for this file/test run
    file_id = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Find MIR record first to get lot/test info if available
    mir_data = None
    if 'MIR' in atdf_processed_entries and atdf_processed_entries['MIR']:
        mir_data = atdf_processed_entries['MIR'][0]  # Get first MIR record
        test_session_id = f"{file_id}_{mir_data.get('lot_id', 'unknown')}"
    else:
        test_session_id = file_id

    # Group and transform the data
    grouped_data = {}
    for record_type, data in atdf_processed_entries.items():
        table_name = get_table_name_for_record(record_type)
        if table_name not in grouped_data:
            grouped_data[table_name] = []

        # Transform and add relationship IDs
        transformed_records = []
        for record in data:
            transformed = transform_record_data(record_type, record)

            # Add relationship IDs
            transformed['file_id'] = file_id
            transformed['test_session_id'] = test_session_id

            # Add additional relationships based on record type
            if record_type in ['WIR', 'WRR']:
                transformed['wafer_id'] = f"{test_session_id}_{transformed.get('wafer_id', 'unknown')}"
            elif record_type in ['PIR', 'PRR']:
                wafer_id = transformed.get('wafer_id')
                if wafer_id:
                    transformed['full_wafer_id'] = f"{test_session_id}_{wafer_id}"
                transformed['part_id'] = f"{test_session_id}_{transformed.get('part_id', 'unknown')}"
            elif record_type in ['PTR', 'FTR', 'MPR']:
                transformed['part_id'] = f"{test_session_id}_{transformed.get('part_id', 'unknown')}"
                transformed['test_id'] = f"{test_session_id}_{transformed.get('test_number', 'unknown')}"

            transformed_records.append(transformed)

        grouped_data[table_name].extend(transformed_records)

    # Create tables and insert data
    for table_name, data in grouped_data.items():
        if data:
            df = pd.DataFrame(data)
            df.to_sql(table_name, engine, index=True, if_exists='replace')
            logger.info(f"Created table '{table_name}' with {len(df)} records")

    engine.dispose()
    logger.info("Database creation complete.")


def create_dataframe(data: list, record_type: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Create DataFrame from record data."""
    if not data:
        logger.warning("Empty data list provided, returning None.")
        return None

    df = pd.DataFrame(data)
    return df