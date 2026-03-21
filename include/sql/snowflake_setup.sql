-- Snowflake Setup Scripts for AQI Pipeline
-- Run these in Snowflake Web UI as ACCOUNTADMIN
-- Reference: https://github.com/sonikirtan110/aqi-pipeline

-- 1. CREATE DATABASE
CREATE DATABASE IF NOT EXISTS aqi_db;

-- 2. CREATE SCHEMAS
USE DATABASE aqi_db;

CREATE SCHEMA IF NOT EXISTS bronze COMMENT='Raw data from OGD India API and OpenAQ API via S3';
CREATE SCHEMA IF NOT EXISTS silver COMMENT='Cleaned, deduplicated, and typed data with AQI scoring';
CREATE SCHEMA IF NOT EXISTS gold COMMENT='Business-ready aggregated data for dashboards';

-- 3. CREATE WAREHOUSE (xsmall for dev, xsmall → large for prod)
CREATE WAREHOUSE IF NOT EXISTS aqi_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 1;

-- 4. CREATE ROLE FOR dbt TRANSFORMER
CREATE ROLE IF NOT EXISTS transformer;
GRANT USAGE ON WAREHOUSE aqi_wh TO ROLE transformer;
GRANT ALL ON DATABASE aqi_db TO ROLE transformer;
GRANT ALL ON SCHEMA aqi_db.bronze TO ROLE transformer;
GRANT ALL ON SCHEMA aqi_db.silver TO ROLE transformer;
GRANT ALL ON SCHEMA aqi_db.gold TO ROLE transformer;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE aqi_db TO ROLE transformer;
GRANT CREATE TABLE ON SCHEMA aqi_db.silver TO ROLE transformer;
GRANT CREATE TABLE ON SCHEMA aqi_db.gold TO ROLE transformer;

-- 5. AWS ACCESS MODE
-- You provided IAM USER ARN (arn:aws:iam::160782514426:user/snowuser).
-- IAM user ARN cannot be used in STORAGE_AWS_ROLE_ARN. Use key-based stage credentials here.
-- Create/rotate access keys for this IAM user and paste them below when executing.

-- 6. FILE FORMAT FOR JSONL
CREATE FILE FORMAT IF NOT EXISTS aqi_db.bronze.jsonl_format
  TYPE = 'JSON'
  COMPRESSION = 'AUTO'
  COMMENT = 'Format for JSONL files from OGD and OpenAQ APIs';

-- 7. EXTERNAL STAGE
CREATE STAGE IF NOT EXISTS aqi_db.bronze.s3_aqi_stage
  URL = 's3://aqi-bronze/raw/ogd/'
  CREDENTIALS = (
    AWS_KEY_ID = 'YOUR_AWS_ACCESS_KEY_ID'
    AWS_SECRET_KEY = 'YOUR_AWS_SECRET_ACCESS_KEY'
  )
  FILE_FORMAT = aqi_db.bronze.jsonl_format
  COMMENT = 'Stage pointing to raw AQI data in S3 bucket';

-- 8. BRONZE TABLE (semi-structured)
USE SCHEMA aqi_db.bronze;

CREATE TABLE IF NOT EXISTS AQI_MEASUREMENTS (
  RAW_DATA VARIANT COMMENT 'Semi-structured raw API response',
  FILE_NAME VARCHAR COMMENT 'S3 filename for traceability',
  LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'When loaded to Snowflake'
)
COMMENT = 'Raw AQI measurements from OGD India and OpenAQ APIs (via S3)';

-- 9. COPY INTO TEMPLATE (run manually after data in S3)
-- COPY INTO aqi_db.bronze.AQI_MEASUREMENTS (RAW_DATA, FILE_NAME)
-- FROM (
--   SELECT
--     $1 AS RAW_DATA,
--     METADATA$FILENAME AS FILE_NAME
--   FROM @aqi_db.bronze.s3_aqi_stage
-- )
-- FILE_FORMAT = aqi_db.bronze.jsonl_format
-- ON_ERROR = 'CONTINUE'
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- 10. VERIFY SETUP
SHOW DATABASES LIKE 'aqi_db';
SHOW SCHEMAS IN DATABASE aqi_db;
SHOW WAREHOUSE LIKE 'aqi_wh';
SHOW STAGES IN SCHEMA aqi_db.bronze;
SELECT 'AQI Snowflake setup complete!' AS status;

