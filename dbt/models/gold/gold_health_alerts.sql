{{ config(materialized='table') }}

SELECT
    city,
    state,
    measurement_date,
    pollutant,
    avg_value,
    max_value,
    dominant_category,
    dbt_updated_at
FROM {{ ref('gold_city_aqi_daily') }}
WHERE dominant_category IN ('Unhealthy', 'Very Unhealthy', 'Hazardous')
ORDER BY measurement_date DESC, avg_value DESC
