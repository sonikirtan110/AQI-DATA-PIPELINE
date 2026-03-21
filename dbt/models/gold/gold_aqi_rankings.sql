{{ config(materialized='table') }}

SELECT
    city,
    state,
    ROUND(AVG(avg_value), 2) AS week_avg_pm25,
    RANK() OVER (ORDER BY AVG(avg_value) DESC) AS pollution_rank,
    MODE(dominant_category) AS typical_category,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('gold_city_aqi_daily') }}
WHERE measurement_date >= DATEADD('day', -7, CURRENT_DATE())
  AND pollutant = 'PM2.5'
GROUP BY 1, 2
ORDER BY pollution_rank
