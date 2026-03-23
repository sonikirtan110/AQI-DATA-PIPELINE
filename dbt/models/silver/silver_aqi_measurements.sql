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

subindex AS (
    SELECT
        *,
        CASE
            WHEN pollutant = 'PM2.5' THEN
                CASE
                    WHEN value <= 30 THEN (50 / 30) * value
                    WHEN value <= 60 THEN ((100 - 51) / (60 - 31)) * (value - 31) + 51
                    WHEN value <= 90 THEN ((200 - 101) / (90 - 61)) * (value - 61) + 101
                    WHEN value <= 120 THEN ((300 - 201) / (120 - 91)) * (value - 91) + 201
                    WHEN value <= 250 THEN ((400 - 301) / (250 - 121)) * (value - 121) + 301
                    WHEN value <= 500 THEN ((500 - 401) / (500 - 251)) * (value - 251) + 401
                    ELSE 500
                END
            WHEN pollutant = 'PM10' THEN
                CASE
                    WHEN value <= 50 THEN (50 / 50) * value
                    WHEN value <= 100 THEN ((100 - 51) / (100 - 51)) * (value - 51) + 51
                    WHEN value <= 250 THEN ((200 - 101) / (250 - 101)) * (value - 101) + 101
                    WHEN value <= 350 THEN ((300 - 201) / (350 - 251)) * (value - 251) + 201
                    WHEN value <= 430 THEN ((400 - 301) / (430 - 351)) * (value - 351) + 301
                    WHEN value <= 600 THEN ((500 - 401) / (600 - 431)) * (value - 431) + 401
                    ELSE 500
                END
            WHEN pollutant = 'NO2' THEN
                CASE
                    WHEN value <= 40 THEN (50 / 40) * value
                    WHEN value <= 80 THEN ((100 - 51) / (80 - 41)) * (value - 41) + 51
                    WHEN value <= 180 THEN ((200 - 101) / (180 - 81)) * (value - 81) + 101
                    WHEN value <= 280 THEN ((300 - 201) / (280 - 181)) * (value - 181) + 201
                    WHEN value <= 400 THEN ((400 - 301) / (400 - 281)) * (value - 281) + 301
                    WHEN value <= 1000 THEN ((500 - 401) / (1000 - 401)) * (value - 401) + 401
                    ELSE 500
                END
            WHEN pollutant = 'SO2' THEN
                CASE
                    WHEN value <= 40 THEN (50 / 40) * value
                    WHEN value <= 80 THEN ((100 - 51) / (80 - 41)) * (value - 41) + 51
                    WHEN value <= 380 THEN ((200 - 101) / (380 - 81)) * (value - 81) + 101
                    WHEN value <= 800 THEN ((300 - 201) / (800 - 381)) * (value - 381) + 201
                    WHEN value <= 1600 THEN ((400 - 301) / (1600 - 801)) * (value - 801) + 301
                    WHEN value <= 2000 THEN ((500 - 401) / (2000 - 1601)) * (value - 1601) + 401
                    ELSE 500
                END
            WHEN pollutant = 'CO' THEN
                CASE
                    WHEN value <= 1 THEN (50 / 1) * value
                    WHEN value <= 2 THEN ((100 - 51) / (2 - 1.1)) * (value - 1.1) + 51
                    WHEN value <= 10 THEN ((200 - 101) / (10 - 2.1)) * (value - 2.1) + 101
                    WHEN value <= 17 THEN ((300 - 201) / (17 - 10.1)) * (value - 10.1) + 201
                    WHEN value <= 34 THEN ((400 - 301) / (34 - 17.1)) * (value - 17.1) + 301
                    WHEN value <= 50 THEN ((500 - 401) / (50 - 34.1)) * (value - 34.1) + 401
                    ELSE 500
                END
            WHEN pollutant = 'OZONE' THEN
                CASE
                    WHEN value <= 50 THEN (50 / 50) * value
                    WHEN value <= 100 THEN ((100 - 51) / (100 - 51)) * (value - 51) + 51
                    WHEN value <= 168 THEN ((200 - 101) / (168 - 101)) * (value - 101) + 101
                    WHEN value <= 208 THEN ((300 - 201) / (208 - 169)) * (value - 169) + 201
                    WHEN value <= 748 THEN ((400 - 301) / (748 - 209)) * (value - 209) + 301
                    WHEN value <= 1000 THEN ((500 - 401) / (1000 - 749)) * (value - 749) + 401
                    ELSE 500
                END
            WHEN pollutant = 'NH3' THEN
                CASE
                    WHEN value <= 200 THEN (50 / 200) * value
                    WHEN value <= 400 THEN ((100 - 51) / (400 - 201)) * (value - 201) + 51
                    WHEN value <= 800 THEN ((200 - 101) / (800 - 401)) * (value - 401) + 101
                    WHEN value <= 1200 THEN ((300 - 201) / (1200 - 801)) * (value - 801) + 201
                    WHEN value <= 1800 THEN ((400 - 301) / (1800 - 1201)) * (value - 1201) + 301
                    WHEN value <= 2000 THEN ((500 - 401) / (2000 - 1801)) * (value - 1801) + 401
                    ELSE 500
                END
            ELSE NULL
        END AS sub_index
    FROM cleaned
),

final AS (
    SELECT
        *,
        CASE
            WHEN sub_index <= 50 THEN 'Good'
            WHEN sub_index <= 100 THEN 'Moderate'
            WHEN sub_index <= 200 THEN 'Poor'
            WHEN sub_index <= 300 THEN 'Very Poor'
            WHEN sub_index <= 400 THEN 'Severe'
            WHEN sub_index IS NULL THEN 'N/A'
            ELSE 'Severe+'
        END AS aqi_category
    FROM subindex
)

SELECT * FROM final
