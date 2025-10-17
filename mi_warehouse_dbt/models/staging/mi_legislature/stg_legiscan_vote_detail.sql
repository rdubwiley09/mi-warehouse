{{ config(materialized='external', location='../data/staging/mi_legislature/stg_legiscan_vote_detail.parquet', format='parquet') }}

WITH raw_vote_detail AS (
    SELECT *
    FROM
        read_parquet(
            '../data/raw/legiscan/*/parsed/vote_details.parquet', union_by_name = True
        )
),
output AS (
    SELECT *
    FROM raw_vote_detail
)

SELECT * FROM output