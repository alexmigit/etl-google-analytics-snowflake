import pandas as pd
from config.db_connection import get_bigquery_client
from utils.logging_facility import log
import os

def extract_ga4_data(start_date, end_date):
    client = get_bigquery_client()
    dataset = os.getenv("BIGQUERY_DATASET")

    log.info(f"Starting extraction from BigQuery dataset: {dataset}")

    # --- Extract events_* ---
    events_query = f"""
        SELECT
            *
        FROM `{dataset}.events_*`
        WHERE PARSE_DATE('%Y%m%d', event_date)
            BETWEEN '{start_date}' AND '{end_date}'
    """
    log.info("Querying GA4 events_* data...")
    event_df = client.query(events_query).to_dataframe()
    log.info(f"Extracted {len(event_df)} rows from events_*")

    # --- Extract pseudonymous_users_* ---
    user_query = f"""
        SELECT
            *
        FROM `{dataset}.pseudonymous_users_*`
        WHERE _TABLE_SUFFIX BETWEEN 
            FORMAT_DATE('%Y%m%d', DATE('{start_date}')) 
            AND FORMAT_DATE('%Y%m%d', DATE('{end_date}'))
    """
    log.info("Querying GA4 pseudonymous_users_* data...")
    user_df = client.query(user_query).to_dataframe()
    log.info(f"Extracted {len(user_df)} rows from pseudonymous_users_*")

    return {"event_data": event_df, "user_data": user_df}
