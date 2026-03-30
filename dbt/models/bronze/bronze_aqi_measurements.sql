{{ config(
    materialized='view',
    description='Bronze layer view - raw AQI measurements from OGD India API via S3'
) }}

SELECT * FROM {{ source('bronze', 'AQI_MEASUREMENTS') }}
