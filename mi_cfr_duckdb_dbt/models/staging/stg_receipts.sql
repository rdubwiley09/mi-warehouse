{{ config(materialized='view', location='../data/staging/stg_receipts.parquet', format='parquet') }}

WITH raw_receipts AS (
    SELECT *
    FROM read_parquet('../data/raw/*_mi_cfr_receipts.parquet', union_by_name=True)
),
output AS (
    SELECT *
    FROM raw_receipts
)
SELECT * FROM output