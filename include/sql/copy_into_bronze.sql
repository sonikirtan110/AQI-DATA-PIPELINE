-- COPY INTO Bronze Layer from S3
-- Executed by Airflow as part of daily pipeline
-- Reference: https://github.com/sonikirtan110/aqi-pipeline

USE SCHEMA aqi_db.bronze;

-- Load one row per AQI reading from payload.records[] into VARIANT column.
-- Stage aliasing avoids identifier issues with FLATTEN in COPY SELECT transforms.
COPY INTO aqi_db.bronze.AQI_MEASUREMENTS (RAW_DATA, FILE_NAME)
FROM (
  SELECT
    rec.value AS RAW_DATA,
    src.METADATA$FILENAME AS FILE_NAME
  FROM @aqi_db.bronze.s3_aqi_stage
    (FILE_FORMAT => aqi_db.bronze.jsonl_format) AS src,
    LATERAL FLATTEN(INPUT => src.$1:records) AS rec
)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- Verify load
SELECT 
  COUNT(*) as total_records,
  MIN(LOAD_TIMESTAMP) as earliest,
  MAX(LOAD_TIMESTAMP) as latest
FROM aqi_db.bronze.AQI_MEASUREMENTS;
