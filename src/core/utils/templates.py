# src/core/utils/templates.py

from src.core.stdf.templates import STDF_TEMPLATES
from src.core.atdf.templates import ATDF_TEMPLATES

def get_record_types():
    return list(STDF_TEMPLATES.keys())

def create_stdf_mapping():
    """Create mapping of (rec_typ, rec_sub) to record types."""
    mapping = {}
    for record, template in STDF_TEMPLATES.items():
        rec_typ = template["rec_typ"]["value"]
        rec_sub = template["rec_sub"]["value"]
        if rec_typ is not None and rec_sub is not None:
            mapping[(rec_typ, rec_sub)] = record
    return mapping

def create_stdf_template(record_type):
    if record_type not in STDF_TEMPLATES:
        raise ValueError(f"No template found for STDF record type {record_type}")
    
    return {
        "record_type": record_type,
        "fields": STDF_TEMPLATES[record_type].copy()
    }

def create_atdf_template(record_type):
    if record_type not in ATDF_TEMPLATES:
        raise ValueError(f"No template found for ATDF record type {record_type}")
    
    return {
        "record_type": record_type,
        "header": f"{record_type}:",
        "fields": ATDF_TEMPLATES[record_type].copy()
    }

def get_stdf_template(stdf_mapping, rec_typ, rec_sub):
    key = (rec_typ, rec_sub)
    record_type = stdf_mapping.get(key)
    
    if record_type:
        return create_stdf_template(record_type)
    else:
        message = f"No template found for rec_typ={rec_typ}, rec_sub={rec_sub}"
        raise ValueError(message)


def get_atdf_template(record_type: str) -> dict:
    try:
        return create_atdf_template(record_type)
    except ValueError as e:
        message = f"Template for '{record_type}' not found."
        #logging.error(message)
        raise ValueError(message)