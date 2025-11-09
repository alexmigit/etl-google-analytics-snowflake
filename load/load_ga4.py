import os
import subprocess
from config.db_connection import get_snowflake_connection
from snowflake.connector.pandas_tools import write_pandas
from utils.logging_facility import log
from dotenv import load_dotenv

load_dotenv()

def load_ga4_data(transformed_data):
    conn = get_snowflake_connection()
    raw_schema = os.getenv("GA_RAW_SCHEMA")
    stg_schema = os.getenv("GA_STAGING_SCHEMA")

    # --- Load RAW tables ---
    for table_name in ["USER_RAW", "EVENT_RAW"]:
        if table_name in transformed_data:
            df = transformed_data[table_name]
            log.info(f"Loading {len(df)} rows into {raw_schema}.{table_name} ...")
            write_pandas(conn, df, f"{raw_schema}.{table_name}")
            log.info(f"✅ Loaded {raw_schema}.{table_name}")

    # --- Load STAGING tables ---
    for table_name in ["STG_USER", "STG_EVENT", "STG_GEO", "STG_DEVICE", "STG_TRAFFICSOURCE"]:
        df = transformed_data[table_name]
        log.info(f"Loading {len(df)} rows into {stg_schema}.{table_name} ...")
        write_pandas(conn, df, f"{stg_schema}.{table_name}")
        log.info(f"✅ Loaded {stg_schema}.{table_name}")

    conn.close()
