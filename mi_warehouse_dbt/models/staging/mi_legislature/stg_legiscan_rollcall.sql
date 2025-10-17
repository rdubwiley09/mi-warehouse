{{ config(materialized='external', location='../data/staging/mi_legislature/stg_legiscan_rollcall.parquet', format='parquet') }}

WITH raw_rollcall AS (
    SELECT *
    FROM
        read_parquet(
            '../data/raw/legiscan/*/parsed/vote_results.parquet', union_by_name = True
        )
),
output AS (
    SELECT *,
        CASE WHEN total<=20 THEN 1 ELSE 0 END AS is_committee_vote
    FROM raw_rollcall
)

SELECT * FROM output