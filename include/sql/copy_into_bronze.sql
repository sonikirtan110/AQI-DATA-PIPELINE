-- COPY INTO Bronze Layer from S3
-- Executed by Airflow as part of daily pipeline
-- Reference: https://github.com/sonikirtan110/aqi-pipeline

USE SCHEMA aqi_db.bronze;

-- Load raw JSON from S3 into VARIANT column
COPY INTO aqi_db.bronze.AQI_MEASUREMENTS (RAW_DATA, FILE_NAME)
FROM (
  SELECT
    $1 AS RAW_DATA,
    METADATA$FILENAME AS FILE_NAME
  FROM @aqi_db.bronze.s3_aqi_stage
)
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- Verify load
SELECT 
  COUNT(*) as total_records,
  MIN(LOAD_TIMESTAMP) as earliest,
  MAX(LOAD_TIMESTAMP) as latest
FROM aqi_db.bronze.AQI_MEASUREMENTS;
