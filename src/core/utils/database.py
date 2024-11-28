# src/core/utils/database.py
import pandas as pd
from sqlalchemy import create_engine
import logging

from .epoch import convert_epoch_to_datetime
from typing import Optional, Dict, List, Union, Any

# Try importing Django modules if available
try:
    from django.conf import settings
    from django.apps import apps
    django_available = True
except ImportError:
    django_available = False

logger = logging.getLogger(__name__)


def create_dataframe(data: list, record_type: Optional[str] = None, foreign_key_value: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Create DataFrame from record data.

    Args:
        data (list): List of dictionaries containing record data
        record_type (str, optional): Type of record ('FAR', 'ATR', 'MIR', etc.)
        foreign_key_value (int, optional): Foreign key value for database relations

    Returns:
        pd.DataFrame or None: DataFrame created from data, or None if data is empty
    """
    if not data:
        logger.warning("Empty data list provided, returning None.")
        return None

    df = pd.DataFrame(data)

    if foreign_key_value is not None and record_type is not None:
        if django_available and record_type in ['FAR', 'ATR', 'MIR']:
            df['file_id'] = foreign_key_value
        elif django_available:
            df['master_id'] = foreign_key_value

    print(df)
    # logger.info(f"Created DataFrame with shape: {df.shape}")
    return df

def create_database_from_atdf(output_atdf_database, atdf_processed_entries):
    """Create SQLite database from ATDF records."""

    engine = create_engine(f"sqlite:///{output_atdf_database}")
    logger.info(f"Creating database at {output_atdf_database}")

    # record_type: str, data: List[Dict[str, Any]]
    for record_type, data in atdf_processed_entries.items():
        df = create_dataframe(data=data)
        if df is not None:
            table_name = f'table_{record_type}'

            for column in df.columns:
                if column in ['modification_timestamp', 'setup_time', 'start_time', 'finish_time'] \
                        and record_type in ['ATR', 'MIR', 'MRR', 'WIR', 'WRR']:
                    df[column] = df[column].apply(lambda x: convert_epoch_to_datetime(x, dt_format='sqlite'))

            df.to_sql(table_name, engine, index=True, if_exists='replace')
            # df.to_sql(table_name, engine, index=True, if_exists='append')
            logger.info(f"Inserted data into table '{table_name}' with {len(df)} records.")

    engine.dispose()
    logger.info("Database creation complete.")

def insert_df_into_db(atdf_processed_entries):
    """
    Insert ATDF record_type data into a Django-managed database.

    Args:
        atdf_processed_entries (dict): Dictionary containing ATDF record_type data.
    """
    if not django_available:
        logger.error("Django is not available, cannot insert data into Django-managed database.")
        raise ImportError("Django is required to insert data into the database.")

    def get_table_names():
        #app_name = 'testdataapp'
        app_name = get_current_app_name()
        models = apps.get_app_config(app_name).get_models()
        # for model in models:
        #     table_name = model._meta.db_table
        #     print(f"The table name for {model.__name__} is: {table_name}")

        return [model._meta.db_table for model in models]

    mapping = {
        'FAR': 'File Attributes Record',
        'ATR': 'Audit Trail Record',
        'MIR': 'Master Information Record',
        'MRR': 'Master Results Record',
        'PCR': 'Part Count Record',
        'HBR': 'Hardware Bin Record',
        'SBR': 'Software Bin Record',
        'PMR': 'Pin Map Record',
        'PGR': 'Pin Group Record',
        'PLR': 'Pin List Record',
        'RDR': 'Retest Data Record',
        'SDR': 'Site Description Record',
        'WIR': 'Wafer Information Record',
        'WRR': 'Wafer Results Record',
        'WCR': 'Wafer Configuration Record',
        'PIR': 'Part Information Record',
        'PRR': 'Part Results Record',
        'TSR': 'Test Sypnosis Record',
        'PTR': 'Parametric Test Record',
        'MPR': 'Multiple Result Parametric Record',
        'FTR': 'Functional Test Record',
        'BPS': 'Begin Program Section Record',
        'EPS': 'End Program Section Record',
        'GDR': 'Generic Data Record',
        'DTR': 'Datalog Text Record',
    }

    # Retrieve the Django database settings
    database_settings = settings.DATABASES['default']

    # Construct the SQLite database URL for SQLAlchemy
    database_url = f"sqlite:///{database_settings['NAME']}"

    # Create a SQLAlchemy engine
    engine = create_engine(database_url)

    logger.info("Inserting data into Django-managed database.")

    # record_type: str, data: List[Dict[str, Any]]
    for record_type, data in atdf_processed_entries.items():
        df = create_dataframe(data=data, record_type=record_type)
        #if isinstance(df, pd.DataFrame):
        if df is not None:

            for column in df.columns:
                if column in ['modification_timestamp', 'setup_time', 'start_time', 'finish_time'] and record_type in ['ATR', 'MIR', 'MRR', 'WIR', 'WRR']:
                    #df[column] = df[column].apply(lambda x: convert_epoch_to_datetime(x, dt_format=get_database_engine()))
                    df[column] = df[column].apply(convert_epoch_to_datetime)

            # if record_type in ['WIR', 'WRR']:
            #     df['w_id'] = range(1, len(df) + 1)
            #     print(record_dataframes[record_type])

            # if record_type in ['PIR', 'PRR']:
            #     df['p_id'] = range(1, len(df) + 1)
            #     print(record_dataframes[record_type])

            # if record_type in ['PTR', 'MPR', 'FTR']:
            #     for index, row in df.iterrows():
            #         #print(f"{row['head_number']}, {row['site_number']}")
            #         if index + 1 < len(df):
            #             if df.iloc[index]['head_number'] < df.iloc[index + 1]['head_number'] or df.iloc[index]['site_number'] < df.iloc[index + 1]['site_number']:
            #                 print(f"{df.iloc[index + 1]['head_number']}, {df.iloc[index + 1]['site_number']}")

            for table in get_table_names():
                # if record_type.lower() in table:
                #     df.to_sql(name=table, con=engine, if_exists='append', index=False)

                #print(table)
                if mapping[record_type].replace(" ", "").lower() in table:
                    df.to_sql(name=table, con=engine, if_exists='append', index=False)
                    logger.info(f"Inserted data into Django table '{table}' for record_type '{record_type}' with {len(df)} records.")

    engine.dispose()
    logger.info("Data insertion into Django-managed database complete.")

def get_current_app_name():
    """
    Get the current Django app name.

    Returns:
        str: The name of the current app or 'Unknown App' if not found.
    """
    app_config = apps.get_containing_app_config(__name__)
    return app_config.name if app_config else "Unknown App"