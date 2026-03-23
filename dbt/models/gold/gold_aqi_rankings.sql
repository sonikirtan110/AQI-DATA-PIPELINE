{{ config(materialized='table') }}

WITH anchor AS (
    SELECT MAX(DATE(measured_at)) AS max_measurement_date
    FROM {{ ref('silver_aqi_measurements') }}
),

recent_silver AS (
    SELECT
        city,
        state,
        DATE(measured_at) AS measurement_date,
        pollutant,
        sub_index
    FROM {{ ref('silver_aqi_measurements') }} s
    CROSS JOIN anchor a
    WHERE DATE(s.measured_at) >= DATEADD('day', -7, a.max_measurement_date)
      AND s.sub_index IS NOT NULL
),

daily_pollutant AS (
    SELECT
        city,
        state,
        measurement_date,
        pollutant,
        AVG(sub_index) AS pollutant_subindex_daily
    FROM recent_silver
    GROUP BY 1, 2, 3, 4
),

daily_city_aqi AS (
    SELECT
        city,
        state,
        measurement_date,
        pollutant AS dominant_pollutant,
        ROUND(pollutant_subindex_daily, 2) AS daily_composite_aqi
    FROM daily_pollutant
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY city, state, measurement_date
        ORDER BY pollutant_subindex_daily DESC NULLS LAST
    ) = 1
),

ranked AS (
    SELECT
        city,
        state,
        ROUND(AVG(daily_composite_aqi), 2) AS composite_aqi_7d,
        ROUND(MAX(daily_composite_aqi), 2) AS peak_aqi_7d,
        MODE(dominant_pollutant) AS dominant_pollutant_7d,
        MODE(
            CASE
                WHEN daily_composite_aqi <= 50 THEN 'Good'
                WHEN daily_composite_aqi <= 100 THEN 'Moderate'
                WHEN daily_composite_aqi <= 200 THEN 'Poor'
                WHEN daily_composite_aqi <= 300 THEN 'Very Poor'
                WHEN daily_composite_aqi <= 400 THEN 'Severe'
                ELSE 'Severe+'
            END
        ) AS typical_category,
        CURRENT_TIMESTAMP() AS dbt_updated_at
    FROM daily_city_aqi
    GROUP BY 1, 2
)

SELECT
    city,
    state,
    composite_aqi_7d,
    peak_aqi_7d,
    dominant_pollutant_7d,
    RANK() OVER (ORDER BY composite_aqi_7d DESC, peak_aqi_7d DESC) AS pollution_rank,
    typical_category,
    dbt_updated_at
FROM ranked
ORDER BY pollution_rank
