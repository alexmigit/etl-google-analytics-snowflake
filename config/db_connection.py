import os
from dotenv import load_dotenv
from google.cloud import bigquery
from snowflake.connector import connect

load_dotenv()

def get_bigquery_client():
    project_id = os.getenv("GCP_PROJECT_ID")
    return bigquery.Client(project=project_id)

def get_snowflake_connection():
    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    return conn
