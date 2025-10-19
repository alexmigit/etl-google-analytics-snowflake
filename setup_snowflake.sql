-------------------------------------------------------------------
-- Snowflake Setup Script for Google Analytics (Optimized)
-- Purpose: Creates roles, warehouse, database, schemas, tables, and file formats
-- Best Practices: Security, modularity, maintainability, and consistency
-------------------------------------------------------------------
-- 0. Use admin role and warehouse for setup
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-------------------------------------------------------------------
-- 1. Role Creation and Privileges
-------------------------------------------------------------------
CREATE OR REPLACE ROLE BI_ROLE
  COMMENT = 'Role for data analytics and BI operations';

-- Grant role to admin and current user
GRANT ROLE BI_ROLE TO ROLE SYSADMIN;
SET MY_USER = CURRENT_USER();
GRANT ROLE BI_ROLE TO USER IDENTIFIER ($MY_USER);

-- Grant minimal required privileges
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE BI_ROLE;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE BI_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE BI_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE BI_ROLE;

-------------------------------------------------------------------
-- 2. Warehouse Creation
-------------------------------------------------------------------
CREATE OR REPLACE WAREHOUSE BI_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for Data Analytics & BI Operations';

GRANT OPERATE, MONITOR ON WAREHOUSE BI_WH TO ROLE BI_ROLE;

-------------------------------------------------------------------
-- 3. Database and Schemas
-------------------------------------------------------------------
CREATE OR REPLACE DATABASE BUSINESS_DB
  COMMENT = 'Database for Business Services Management'
  DATA_RETENTION_TIME_IN_DAYS = 1
  TRANSIENT = TRUE;

GRANT USAGE, CREATE SCHEMA ON DATABASE BUSINESS_DB TO ROLE BI_ROLE;

-- Switch to database context
USE DATABASE BUSINESS_DB;

-- Drop public schema to enforce controlled schema usage
DROP SCHEMA IF EXISTS PUBLIC;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS GA_RAW
  COMMENT = 'Stores raw GA4 data; append-only, minimal transformations'
  TRANSIENT = TRUE;

CREATE SCHEMA IF NOT EXISTS GA_STAGING
  COMMENT = 'Flattened, cleaned GA4 data ready for analytics'
  TRANSIENT = TRUE;

CREATE SCHEMA IF NOT EXISTS MART
  COMMENT = 'Data marts for business use'
  TRANSIENT = TRUE;

CREATE SCHEMA IF NOT EXISTS ANALYTICS
  COMMENT = 'Analytics and reporting layer'
  TRANSIENT = TRUE;

-------------------------------------------------------------------
-- 4. Tables (Examples)
-------------------------------------------------------------------
-- GA Raw
CREATE TABLE IF NOT EXISTS GA_RAW.GA_SESSIONS_RAW (
    EXTRACT_DATE DATE DEFAULT CURRENT_DATE,
    EVENT_DATE DATE,
    PAGE_PATH VARCHAR,
    SOURCE VARCHAR,
    SESSIONS INTEGER,
    ENGAGEMENT_RATE FLOAT,
    TOTAL_USERS INTEGER,
    RAW_PAYLOAD VARIANT,
    INSERTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'Raw GA sessions data (immutable)';

-- GA Staging
CREATE TABLE IF NOT EXISTS GA_STAGING.GA_SESSIONS_STAGING (
    EVENT_DATE DATE NOT NULL,
    PAGE_PATH VARCHAR NOT NULL,
    SOURCE VARCHAR,
    SESSIONS INTEGER DEFAULT 0,
    ENGAGEMENT_RATE FLOAT DEFAULT 0,
    TOTAL_USERS INTEGER DEFAULT 0,
    ETL_LOAD_DATE DATE DEFAULT CURRENT_DATE,
    PRIMARY KEY (EVENT_DATE, PAGE_PATH, SOURCE)
)
COMMENT = 'Cleaned GA sessions data ready for analytics';

-- Analytics Fact
CREATE TABLE IF NOT EXISTS ANALYTICS.FACT_DAILY_PAGE_METRICS (
    EVENT_DATE DATE NOT NULL,
    PAGE_PATH VARCHAR NOT NULL,
    SOURCE VARCHAR,
    TOTAL_SESSIONS INTEGER,
    AVG_ENGAGEMENT_RATE FLOAT,
    TOTAL_USERS INTEGER,
    PRIMARY KEY (EVENT_DATE, PAGE_PATH, SOURCE)
)
COMMENT = 'Daily aggregated page metrics';

-- Analytics Dimensions
CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_PAGES (
    PAGE_PATH VARCHAR PRIMARY KEY,
    PAGE_TITLE VARCHAR,
    CATEGORY VARCHAR
);

CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_SOURCES (
    SOURCE VARCHAR PRIMARY KEY,
    MEDIUM VARCHAR,
    CAMPAIGN VARCHAR
);

CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_DATE (
    DATE_KEY DATE PRIMARY KEY,
    YEAR INT,
    MONTH INT,
    DAY INT,
    QUARTER INT,
    DAY_OF_WEEK INT,
    WEEK_OF_YEAR INT
);

-------------------------------------------------------------------
-- 5. File Formats
-------------------------------------------------------------------
-- Parquet
CREATE FILE FORMAT IF NOT EXISTS BUSINESS_DB.GA_RAW.PARQUET_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
    COMMENT = 'Parquet format with Snappy compression';

-- CSV
CREATE FILE FORMAT IF NOT EXISTS BUSINESS_DB.GA_RAW.CSV_SKIPHEADER_DOUBLEQUOTE
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = TRUE
    NULL_IF = ('NULL', 'null')
    COMMENT = 'CSV format with header skipping and double quotes';

-- JSON
CREATE FILE FORMAT IF NOT EXISTS BUSINESS_DB.GA_RAW.JSON_FORMAT
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = TRUE
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = TRUE
    IGNORE_UTF8_ERRORS = FALSE
    COMMENT = 'JSON format with auto-compression and null stripping';

-------------------------------------------------------------------
-- 6. Data Quality Checks
-------------------------------------------------------------------
-- Procedure: Duplicate check for staging
CREATE OR REPLACE PROCEDURE GA_STAGING.CHECK_DUPLICATES()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    dup_count INT;
BEGIN
    SELECT COUNT(*) INTO dup_count
    FROM (
        SELECT EVENT_DATE, PAGE_PATH, SOURCE
        FROM GA_STAGING.GA_SESSIONS_STAGING
        GROUP BY EVENT_DATE, PAGE_PATH, SOURCE
        HAVING COUNT(*) > 1
    );

    RETURN IFF(dup_count > 0, 'Duplicate records found: ' || dup_count, 'No duplicate records found.');
END;
$$;  

-- Task: Schedule duplicate check daily at 2 AM UTC
CREATE OR REPLACE TASK ga_staging.duplicate_check_task
    WAREHOUSE = BI_WH
    SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- daily at 2 AM UTC
AS
    CALL ga_staging.check_duplicates(); 

-------------------------------------------------------------------
-- 7. Context Switching for BI Operations
-------------------------------------------------------------------
USE ROLE BI_ROLE;
USE WAREHOUSE BI_WH;
USE DATABASE BUSINESS_DB;
USE SCHEMA ANALYTICS;

-------------------------------------------------------------------
-- End of Snowflake Setup Script
-------------------------------------------------------------------     

-------------------------------------------------------------------
-- Google BigQuery Sample Queries
------------------------------------------------------------------- 
--GA_RAW.USER_RAW
SELECT
 *
FROM
  `business-intelligence-475520.analytics_272889538.pseudonymous_users_20251018` 
;

--GA_RAW.EVENT_RAW
SELECT
  *
FROM
  `business-intelligence-475520.analytics_272889538.events_20251018`
;

--GA_STAGING.STG_USER
SELECT
  e.`user_id`,
  pu.`pseudo_user_id`,
  pu.`stream_id`,
  pu.`geo`.`city`,
  pu.`geo`.`continent`,
  pu.`geo`.`country`,
  pu.`geo`.`region`,
  pu.`last_updated_date`,
  pu.`occurrence_date`,
  e.`is_active_user`,
  e.`user_first_touch_timestamp`,
  e.`user_ltv`.`currency`,
  e.`user_ltv`.`revenue`
FROM
  `business-intelligence-475520.analytics_272889538.pseudonymous_users_20251018` pu
JOIN
  `business-intelligence-475520.analytics_272889538.events_20251018` e
ON
  pu.pseudo_user_id = e.user_pseudo_id
;

CREATE OR REPLACE TABLE GA_STAGING.STG_USER AS (
  USER_ID STRING,
  PSEUDO_USER_ID STRING,
  STREAM_ID STRING,
  CITY STRING,
  CONTINENT STRING,
  COUNTRY STRING,
  REGION STRING,
  LAST_UPDATED_DATE DATE,
  OCCURRENCE_DATE DATE,
  IS_ACTIVE_USER BOOLEAN,
  USER_FIRST_TOUCH_TIMESTAMP TIMESTAMP,
  LTV_CURRENCY STRING,
  LTV_REVENUE FLOAT
);

--GA_STAGING.STG_EVENT
SELECT
  `event_bundle_sequence_id`,
  `event_date`,
  `event_dimensions`.`hostname`,
  `event_name`,
  `event_previous_timestamp`,
  `event_server_timestamp_offset`,
  `event_timestamp`,
  `event_value_in_usd`,
  `user_id`,
  `user_pseudo_id`
FROM
  `business-intelligence-475520.analytics_272889538.events_20251018` ;

--GA_STAGING.STG_GEO
SELECT
  pu.`pseudo_user_id`,
  pu.`stream_id`,
  pu.`geo`.`city`,
  pu.`geo`.`continent`,
  pu.`geo`.`country`,
  pu.`geo`.`region`,
  e.`geo`.`metro`,
  e.`geo`.`sub_continent`,
  pu.`last_updated_date`,
  pu.`occurrence_date`
FROM
  `business-intelligence-475520.analytics_272889538.pseudonymous_users_20251018` pu
JOIN
  `business-intelligence-475520.analytics_272889538.events_20251018` e
ON
  pu.pseudo_user_id = e.user_pseudo_id
;

--GA_STAGING.STG_DEVICE
SELECT
  pu.`device`.`category`,
  pu.`device`.`mobile_brand_name`,
  pu.`device`.`mobile_model_name`,
  pu.`device`.`operating_system`,
  pu.`device`.`unified_screen_name`,
  e.`device`.`advertising_id`,
  --e.`device`.`category`,
  e.`device`.`language`,
  --e.`device`.`mobile_brand_name`,
  e.`device`.`mobile_marketing_name`,
  --e.`device`.`mobile_model_name`,
  e.`device`.`mobile_os_hardware_model`,
  --e.`device`.`operating_system`,
  e.`device`.`operating_system_version`,
  e.`device`.`vendor_id`,
  e.`device`.`browser`,
  e.`device`.`browser_version`,
  e.`device`.`is_limited_ad_tracking`,
  e.`device`.`time_zone_offset_seconds`,
  e.`device`.`web_info`.`browser`,
  e.`device`.`web_info`.`browser_version`,
  e.`device`.`web_info`.`hostname`
FROM
  `business-intelligence-475520.analytics_272889538.pseudonymous_users_20251018` pu
JOIN
  `business-intelligence-475520.analytics_272889538.events_20251018` e
ON
  pu.pseudo_user_id = e.user_pseudo_id
;

--GA_STAGING.STG_TRAFFICSOURCE
SELECT
  `platform`,
  `stream_id`,
  `traffic_source`.`medium`,
  `traffic_source`.`name`,
  `traffic_source`.`source`,
  `collected_traffic_source`.`gclid`,
  `collected_traffic_source`.`manual_campaign_id`,
  `collected_traffic_source`.`manual_campaign_name`,
  `collected_traffic_source`.`manual_content`,
  `collected_traffic_source`.`manual_creative_format`,
  `collected_traffic_source`.`manual_marketing_tactic`,
  `collected_traffic_source`.`manual_medium`,
  `collected_traffic_source`.`manual_source`,
  `collected_traffic_source`.`manual_source_platform`,
  `collected_traffic_source`.`manual_term`,
  `collected_traffic_source`.`dclid`,
  `collected_traffic_source`.`srsltid`,
  `session_traffic_source_last_click`.`manual_campaign`.`campaign_id`,
  `session_traffic_source_last_click`.`manual_campaign`.`campaign_name`,
  `session_traffic_source_last_click`.`manual_campaign`.`content`,
  `session_traffic_source_last_click`.`manual_campaign`.`creative_format`,
  `session_traffic_source_last_click`.`manual_campaign`.`marketing_tactic`,
  `session_traffic_source_last_click`.`manual_campaign`.`medium`,
  `session_traffic_source_last_click`.`manual_campaign`.`source`,
  `session_traffic_source_last_click`.`manual_campaign`.`source_platform`,
  `session_traffic_source_last_click`.`manual_campaign`.`term`,
  `session_traffic_source_last_click`.`cross_channel_campaign`.`campaign_id`,
  `session_traffic_source_last_click`.`cross_channel_campaign`.`campaign_name`,
  `session_traffic_source_last_click`.`cross_channel_campaign`.`default_channel_group`,
  `session_traffic_source_last_click`.`cross_channel_campaign`.`medium`,
  `session_traffic_source_last_click`.`cross_channel_campaign`.`primary_channel_group`,
  `session_traffic_source_last_click`.`cross_channel_campaign`.`source`,
  `session_traffic_source_last_click`.`cross_channel_campaign`.`source_platform`
FROM
  `business-intelligence-475520.analytics_272889538.events_20251018` ;

--MART.DIM_GEO
--MART.DIM_DEVICE
--MART.DIM_USER
--MART.DIM_EVENT
--MART.DIM_TRAFFICSOURCE
--MART.FCT_EVENTS

--ANALYTICS.
