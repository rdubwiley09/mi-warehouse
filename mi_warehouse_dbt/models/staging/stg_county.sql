{{ config(materialized='external', location='../data/staging/stg_county.parquet', format='parquet') }}

WITH raw_county AS (
    SELECT
        "0" AS county_code,
        "1" AS county_name
    FROM 
        read_parquet(
            '../data/raw/election_results/county.parquet'
        )
),

output AS (
    SELECT *
    FROM raw_county
)

SELECT * FROM output
