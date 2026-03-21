{{ config(materialized='table') }}

SELECT
    city,
    state,
    DATE(measured_at) AS measurement_date,
    pollutant,
    ROUND(AVG(value), 2) AS avg_value,
    ROUND(MAX(value), 2) AS max_value,
    ROUND(MIN(value), 2) AS min_value,
    COUNT(*) AS reading_count,
    MODE(aqi_category) AS dominant_category,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('silver_aqi_measurements') }}
GROUP BY 1, 2, 3, 4
