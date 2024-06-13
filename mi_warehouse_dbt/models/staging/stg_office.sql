{{ config(materialized='external', location='../data/staging/stg_office.parquet', format='parquet') }}

WITH raw_office AS (
    SELECT
        "0" AS election_year,
        "1" AS election_type,
        "2" AS office_code,
        "3" AS district_code,
        "4" AS status_code,
        "5" AS office_description
    FROM 
        read_parquet(
            '../data/raw/election_results/*offc.parquet', union_by_name = True
        )
),

output AS (
    SELECT *
    FROM raw_office
)

SELECT * FROM output
