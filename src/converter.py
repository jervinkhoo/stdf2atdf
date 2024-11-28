# src/converter.py
import logging
from typing import Optional

from .core.utils.files import managed_files
from .core.stdf.preprocessing import determine_file_params, read_record_header
from .core.utils.setup import validate_input_file, initialize_record_entries, setup_record_flags
from .core.utils.decorators import timing_decorator
from .core.utils.database import create_database_from_atdf, insert_df_into_db
from .core.stdf.handler import process_stdf_entries
from .core.atdf.handler import process_atdf_entries, write_atdf_file
from .core.utils.templates import create_stdf_mapping, get_stdf_template, get_atdf_template

try:
    import django

    django_available = True
except ImportError:
    django_available = False

logger = logging.getLogger(__name__)


def process_record(params: dict) -> None:
    """Process a single STDF record and convert to ATDF if needed."""
    if params['data']:  # Special checking needed for EPS
        process_stdf_entries(params)

    if params['atdf_file'] or params['output_atdf_database']:
        process_atdf_entries(params)

    if params['atdf_file']:
        write_atdf_file(params['atdf_file'], params['atdf_template'])


@timing_decorator
def run_conversion(
        input_stdf_file: str,
        output_atdf_file: Optional[str] = None,
        output_atdf_database: Optional[str] = None,
        records_to_process: Optional[list] = None,
        primary_key_value: Optional[int] = None
) -> None:
    """Run STDF to ATDF conversion with optional database output."""
    validate_input_file(input_stdf_file)

    stdf_mapping = create_stdf_mapping()
    stdf_processed_entries = initialize_record_entries()
    atdf_processed_entries = initialize_record_entries()
    record_flags = setup_record_flags(records_to_process)
    counters = {'w': 0, 'p': 0}

    try:
        with managed_files(input_stdf_file, output_atdf_file) as (stdf_file, atdf_file):
            file_params = determine_file_params(stdf_file)

            while True:
                header_data = read_record_header(stdf_file, file_params['endianness'])
                if not header_data:
                    break

                rec_len, rec_typ, rec_sub = header_data
                data = stdf_file.read(rec_len)

                if len(data) < rec_len:
                    logger.error(f"Incomplete record data: expected {rec_len} bytes, got {len(data)}")
                    continue

                try:
                    stdf_template = get_stdf_template(stdf_mapping, rec_typ, rec_sub)
                    record_type = stdf_template['record_type']

                    if not record_flags.get(record_type, False):
                        continue

                    atdf_template = get_atdf_template(record_type)

                    process_record({
                        'data': data,
                        'endianness': file_params['endianness'],
                        'stdf_template': stdf_template,
                        'atdf_template': atdf_template,
                        'stdf_processed_entries': stdf_processed_entries,
                        'atdf_processed_entries': atdf_processed_entries,
                        'stdf_file': stdf_file,
                        'atdf_file': atdf_file,
                        'counters': counters,
                        'output_atdf_database': output_atdf_database,
                    })

                except Exception as e:
                    logger.error(f"Error processing record: {e}")
                    continue

        if output_atdf_database:
            if django_available:
                insert_df_into_db(atdf_processed_entries)
            else:
                create_database_from_atdf(output_atdf_database, atdf_processed_entries)

    except Exception as e:
        logger.exception(f"Fatal error during conversion: {e}")
        raise

    logger.info(f"Successfully processed {input_stdf_file}")