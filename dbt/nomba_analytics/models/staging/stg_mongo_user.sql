{{ config(materialized='view') }}

SELECT
    source_id AS user_id,
    first_name,
    last_name,
    occupation,
    state,
    updated_at
FROM {{ source('warehouse', 'stg_mongo_user') }}
