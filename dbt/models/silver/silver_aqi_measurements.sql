{{ config(
    materialized='incremental',
    unique_key='measurement_id',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'AQI_MEASUREMENTS') }}
    {% if is_incremental() %}
    WHERE LOAD_TIMESTAMP > (
        SELECT COALESCE(MAX(LOAD_TIMESTAMP), TO_TIMESTAMP_NTZ('1900-01-01'))
        FROM {{ this }}
    )
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
                TRIM(UPPER(COALESCE(RAW_DATA:pollutant_id::VARCHAR, RAW_DATA:pollutant::VARCHAR))) AS pollutant,
                TRY_TO_DOUBLE(RAW_DATA:pollutant_avg::VARCHAR) AS value,
                COALESCE(
                    TRY_TO_TIMESTAMP_NTZ(RAW_DATA:last_update::VARCHAR, 'DD-MM-YYYY HH24:MI:SS'),
                    TRY_TO_TIMESTAMP_NTZ(RAW_DATA:last_update::VARCHAR)
                ) AS measured_at,
                TRY_TO_DOUBLE(RAW_DATA:latitude::VARCHAR) AS latitude,
                TRY_TO_DOUBLE(RAW_DATA:longitude::VARCHAR) AS longitude,
        LOAD_TIMESTAMP
    FROM source
        WHERE TRY_TO_DOUBLE(RAW_DATA:pollutant_avg::VARCHAR) IS NOT NULL
            AND TRY_TO_DOUBLE(RAW_DATA:pollutant_avg::VARCHAR) >= 0
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
