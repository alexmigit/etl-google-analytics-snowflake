from extract.extract_ga4 import extract_ga4_data
from transform.transform_ga4 import transform_ga4_data
from load.load_ga4 import load_ga4_data   
from utils.logging_facility import log

def etl_ga_data(start_date, end_date):
    try:
        log.info("Starting ETL process for Google Analytics data...")
    
        # Extract
        log.info(f"Extracting data from {start_date} to {end_date} ...")
        raw_data = extract_ga4_data(start_date, end_date)
        log.info("Data extraction complete.")
    
        # Transform
        log.info("Transforming data...")
        transformed_data = transform_ga4_data(raw_data)
        log.info("Data transformation complete.")
    
        # Load
        log.info("Loading data into Snowflake DW...")
        load_ga4_data(transformed_data)
        log.info("Data loading complete.")
    
        # Success
        log.info("ETL process for Google Analytics 4 data completed successfully!")    

    except Exception as e:
        log.error(f"The ETL process has failed: {e}")
        raise

if __name__ == "__main__":
    # Example date range for ETL process
    etl_ga_data("2025-10-31", "2025-11-01") 
