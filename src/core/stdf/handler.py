# src/core/stdf/handler.py
from .utils import *

import logging
logger = logging.getLogger(__name__)


def process_stdf_entry(stdf_template, data, endianness):
    """Process STDF record data."""
    offset = 0
    stdf_processed_entry = {}

    # Start from third field (skip rec_len, rec_typ, rec_sub)
    fields_to_process = list(stdf_template['fields'].items())[3:]

    for stdf_field, stdf_info in fields_to_process:
        dtype = stdf_info['dtype']
        ref = stdf_info['ref']

        array_size = (stdf_template['fields'][ref]['value']
                      if ref else 0)

        value, offset = unpack_dtype(dtype, data, endianness, offset, array_size=array_size)

        stdf_info['value'] = value
        stdf_processed_entry[stdf_field] = value

        check_invalid_and_set_None_after_unpack(stdf_template, stdf_field)

        if offset >= len(data):
            break

    return stdf_processed_entry

def process_stdf_entries(params):
    """
    Process an STDF record.

    Args:
        params (dict): Dictionary of parameters needed for processing the STDF record.
    """
    stdf_template = params['stdf_template']
    data = params['data']
    endianness = params['endianness']
    stdf_processed_entries = params['stdf_processed_entries']

    stdf_processed_entry = process_stdf_entry(stdf_template, data, endianness)
    stdf_processed_entries[stdf_template['record_type']].append(stdf_processed_entry)
