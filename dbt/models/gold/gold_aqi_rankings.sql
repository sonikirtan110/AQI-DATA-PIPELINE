{{ config(materialized='table') }}

WITH anchor AS (
    SELECT MAX(measurement_date) AS max_measurement_date
    FROM {{ ref('gold_city_aqi_daily') }}
    WHERE pollutant = 'PM2.5'
),

recent_pm25 AS (
    SELECT d.*
    FROM {{ ref('gold_city_aqi_daily') }} d
    CROSS JOIN anchor a
    WHERE d.pollutant = 'PM2.5'
      AND d.measurement_date >= DATEADD('day', -7, a.max_measurement_date)
)

SELECT
    city,
    state,
    ROUND(AVG(avg_value), 2) AS week_avg_pm25,
    RANK() OVER (ORDER BY AVG(avg_value) DESC) AS pollution_rank,
    MODE(dominant_category) AS typical_category,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM recent_pm25
GROUP BY 1, 2
ORDER BY pollution_rank
