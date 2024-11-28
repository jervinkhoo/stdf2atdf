# src/core/atdf/handler.py
from .utils import *

from ..utils.epoch import convert_epoch_to_datetime
import logging

logger = logging.getLogger(__name__)


def process_atdf_entry(atdf_template, stdf_template):
    """Process ATDF record_type data."""
    record_type = atdf_template['record_type']
    atdf_processed_entry = {}

    field_processor_map = {
        ('pass_fail_flag', 'PTR'): process_pass_fail_flag,
        ('pass_fail_flag', 'MPR'): process_pass_fail_flag,
        ('alarm_flags', 'PTR'): process_alarm_flags,
        ('alarm_flags', 'MPR'): process_alarm_flags,

        ('programmed_state', 'PLR'): process_state_field,
        ('returned_state', 'PLR'): process_state_field,

        # ('data_file_type', 'FAR'): lambda _: 'A',
        ('data_file_type', 'FAR'): process_data_file_type,

        ('pass_fail_code', 'PRR'): process_pass_fail_code,
        ('retest_code', 'PRR'): process_retest_code,
        ('abort_code', 'PRR'): process_abort_code,

        ('head_number', 'PCR'): process_head_or_site_number,
        ('head_number', 'HBR'): process_head_or_site_number,
        ('head_number', 'SBR'): process_head_or_site_number,
        ('head_number', 'TSR'): process_head_or_site_number,
        ('site_number', 'PCR'): process_head_or_site_number,
        ('site_number', 'HBR'): process_head_or_site_number,
        ('site_number', 'SBR'): process_head_or_site_number,
        ('site_number', 'TSR'): process_head_or_site_number,

        ('limit_compare', 'PTR'): process_limit_compare,
        ('limit_compare', 'MPR'): process_limit_compare,
        ('pass_fail_flag', 'FTR'): process_ftr_pass_fail_flag,
        ('alarm_flags', 'FTR'): process_ftr_alarm_flags,

        ('relative_address', 'FTR'): process_ftr_relative_address,

        #('generic_data', 'GDR'): lambda value: '|'.join(value),
        ('generic_data', 'GDR'): process_generic_data,

        #('mode_array', 'PLR'): lambda value: ','.join(hex(num)[2:] for num in value),
        ('mode_array', 'PLR'): process_mode_array,

        ('radix_array', 'PLR'): process_radix_array,
    }

    for atdf_field, atdf_info in atdf_template['fields'].items():
        if isinstance(atdf_info['stdf'], tuple):
            stdf_values = [stdf_template['fields'][field]['value']
                           for field in atdf_info['stdf']]
            key = (atdf_field, record_type)
            if key in field_processor_map:
                atdf_info['value'] = field_processor_map[key](stdf_values)
            else:
                atdf_info['value'] = None

        elif isinstance(atdf_info['stdf'], str):
            stdf_value = stdf_template['fields'][atdf_info['stdf']]['value']
            key = (atdf_field, record_type)
            if key in field_processor_map:
                atdf_info['value'] = field_processor_map[key](stdf_value)
            else:
                atdf_info['value'] = process_default_value(stdf_value)
        elif atdf_info['stdf'] is None and atdf_field == 'atdf_version' and record_type == 'FAR':
            atdf_info['value'] = 2

        atdf_processed_entry[atdf_field] = atdf_info['value']

    return atdf_processed_entry


def update_counters(record, atdf_processed_entry, atdf_processed_entries, counters):
    """Update counters for ATDF records."""

    def get_latest_matching_entry(entries, record_type):
        if record_type not in entries:
            logger.warning(f"No {record_type} entries found for matching")
            return None

        try:
            return next(
                (entry for entry in reversed(entries[record_type])
                 if entry['head_number'] == atdf_processed_entry['head_number'] and
                 ('p_id' not in entry or entry.get('site_number') == atdf_processed_entry.get('site_number'))
                 ),
                None
            )
        except KeyError as e:
            logger.error(f"Missing required field in entry: {e}")
            return None

    if record == 'WIR':
        counters['w'] += 1
        atdf_processed_entry['w_id'] = counters['w']

    elif record == 'WRR':
        wafer_entry = get_latest_matching_entry(atdf_processed_entries, 'WIR')
        atdf_processed_entry['w_id'] = wafer_entry['w_id'] if wafer_entry else None

    elif record == 'PIR':
        counters['p'] += 1
        atdf_processed_entry['p_id'] = counters['p']
        wafer_entry = get_latest_matching_entry(atdf_processed_entries, 'WIR')
        atdf_processed_entry['w_id'] = wafer_entry['w_id'] if wafer_entry else None

    elif record in ['PTR', 'MPR', 'FTR']:
        part_entry = get_latest_matching_entry(atdf_processed_entries, 'PIR')
        atdf_processed_entry['p_id'] = part_entry['p_id'] if part_entry else None

        wafer_entry = get_latest_matching_entry(atdf_processed_entries, 'WIR')
        atdf_processed_entry['w_id'] = wafer_entry['w_id'] if wafer_entry else None

    elif record == 'PRR':
        if 'PIR' not in atdf_processed_entries or not atdf_processed_entries['PIR']:
            counters['p'] += 1
            atdf_processed_entry['p_id'] = counters['p']
        else:
            part_entry = get_latest_matching_entry(atdf_processed_entries, 'PIR')
            atdf_processed_entry['p_id'] = part_entry['p_id'] if part_entry else None

        wafer_entry = get_latest_matching_entry(atdf_processed_entries, 'WIR')
        atdf_processed_entry['w_id'] = wafer_entry['w_id'] if wafer_entry else None

    return atdf_processed_entry, counters


def write_atdf_file(atdf_file, atdf_template):
    """Write ATDF record to file."""
    fields = atdf_template['fields']

    # Write record header
    atdf_file.write(atdf_template['header'])

    if not fields:
        atdf_file.write("\n")
        return

    # Get keys in reverse order for cleanup
    keys_to_remove = list(fields.keys())[::-1]

    # Remove trailing empty/None fields
    for key in keys_to_remove:
        field = fields[key]
        if ((field['value'] == "" or field['value'] is None) and not field['req']):
            fields.pop(key)
        else:
            break

    # Write field values
    for index, (atdf_field, atdf_info) in enumerate(fields.items()):
        # Handle timestamp conversion
        if (atdf_field in ['modification_timestamp', 'setup_time', 'start_time', 'finish_time']
                and atdf_template['record_type'] in ['ATR', 'MIR', 'MRR', 'WIR', 'WRR']
                and isinstance(atdf_info['value'], int)):
            atdf_info['value'] = convert_epoch_to_datetime(atdf_info['value'], dt_format='atdf')

        # Write value or empty string
        atdf_file.write("" if atdf_info['value'] is None else str(atdf_info['value']))

        # Add separator or newline
        atdf_file.write("\n" if index == len(fields) - 1 else "|")


def process_atdf_entries(params):
    """Process an ATDF record."""
    atdf_template = params['atdf_template']
    stdf_template = params['stdf_template']
    atdf_processed_entries = params['atdf_processed_entries']
    counters = params['counters']

    atdf_processed_entry = process_atdf_entry(atdf_template, stdf_template)

    record_type = atdf_template['record_type']
    atdf_processed_entry, counters = update_counters(record_type, atdf_processed_entry, atdf_processed_entries, counters)

    atdf_processed_entries[record_type].append(atdf_processed_entry)
