{{ config(
    materialized='incremental',
    unique_key='measurement_id',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'AQI_MEASUREMENTS') }}
    {% if is_incremental() %}
    WHERE LOAD_TIMESTAMP > (SELECT MAX(LOAD_TIMESTAMP) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        MD5(CONCAT(
            COALESCE(RAW_DATA:station::VARCHAR, ''),
            '|',
            COALESCE(RAW_DATA:pollutant_id::VARCHAR, ''),
            '|',
            COALESCE(CAST(RAW_DATA:last_update::VARCHAR AS VARCHAR), '')
        )) AS measurement_id,
        TRIM(UPPER(COALESCE(RAW_DATA:city::VARCHAR, 'UNKNOWN'))) AS city,
        TRIM(UPPER(COALESCE(RAW_DATA:state::VARCHAR, 'UNKNOWN'))) AS state,
        RAW_DATA:station::VARCHAR AS station_id,
        RAW_DATA:station::VARCHAR AS station_name,
        TRIM(UPPER(RAW_DATA:pollutant_id::VARCHAR)) AS pollutant,
        CAST(RAW_DATA:pollutant_avg::FLOAT AS FLOAT) AS value,
        TO_TIMESTAMP_NTZ(RAW_DATA:last_update::VARCHAR) AS measured_at,
        RAW_DATA:latitude::FLOAT AS latitude,
        RAW_DATA:longitude::FLOAT AS longitude,
        LOAD_TIMESTAMP
    FROM source
    WHERE RAW_DATA:pollutant_avg::FLOAT IS NOT NULL
      AND RAW_DATA:pollutant_avg::FLOAT >= 0
      AND RAW_DATA:last_update::VARCHAR IS NOT NULL
),

aqi_category AS (
    SELECT *,
        CASE
            WHEN pollutant = 'PM2.5' THEN
                CASE
                    WHEN value <= 12.0 THEN 'Good'
                    WHEN value <= 35.4 THEN 'Moderate'
                    WHEN value <= 55.4 THEN 'Unhealthy for Sensitive Groups'
                    WHEN value <= 150.4 THEN 'Unhealthy'
                    WHEN value <= 250.4 THEN 'Very Unhealthy'
                    ELSE 'Hazardous'
                END
            WHEN pollutant = 'PM10' THEN
                CASE
                    WHEN value <= 54 THEN 'Good'
                    WHEN value <= 154 THEN 'Moderate'
                    WHEN value <= 254 THEN 'Unhealthy for Sensitive Groups'
                    WHEN value <= 354 THEN 'Unhealthy'
                    WHEN value <= 424 THEN 'Very Unhealthy'
                    ELSE 'Hazardous'
                END
            ELSE 'N/A'
        END AS aqi_category
    FROM cleaned
)

SELECT * FROM aqi_category
