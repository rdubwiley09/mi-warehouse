{{ config(materialized='external', location='../data/staging/mi_legislature/stg_people.parquet', format='parquet') }}

WITH raw_people AS (
    SELECT *
    FROM
        read_parquet(
            '../data/raw/legiscan/2023-2024_102nd_Legislature/parsed/people.parquet', union_by_name = True
        )
),
output AS (
    SELECT *
    FROM raw_people
)

SELECT * FROM output