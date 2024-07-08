{{ config(materialized='external', location='../data/staging/stg_city.parquet', format='parquet') }}

WITH raw_city AS (
    SELECT
        "0" AS election_year,
        "1" AS election_type,
        "2" AS county_code,
        "3" AS city_township_code,
        "4" AS city_township_description
    FROM 
        read_parquet(
            '../data/raw/election_results/*city.parquet', union_by_name = True
        )
),

output AS (
    SELECT *
    FROM raw_city
)

SELECT * FROM output
