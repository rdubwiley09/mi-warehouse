{{ config(materialized='external', location='../data/staging/stg_name.parquet', format='parquet') }}

WITH raw_name AS (
    SELECT
        "0" AS election_year,
        "1" AS election_type,
        "2" AS office_code,
        "3" AS district_code,
        "4" AS status_code,
        "5" AS candidate_id,
        "6" AS candidate_last_name,
        "7" AS candidate_first_name,
        "8" AS candidate_middle_name,
        "9" AS candidate_party_name
    FROM 
        read_parquet(
            '../data/raw/election_results/*name.parquet', union_by_name = True
        )
),

output AS (
    SELECT *
    FROM raw_name
)

SELECT * FROM output
