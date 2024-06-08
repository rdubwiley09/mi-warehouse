{{ config(materialized='external', location='../data/ml/dbt_ml_poc.parquet', format='parquet') }}

WITH exp AS (
    SELECT
        LOWER(CONCAT(expenditure_description, ' ', expenditure_purpose)) AS concat_expenditure
    FROM {{ ref("stg_expenditures") }}
    LIMIT 5
),
listed AS (
    SELECT
        list(concat_expenditure) AS list_concat_expenditure
    FROM exp
),
output AS (
    SELECT
        unnest(list_concat_expenditure) AS 'concat_expenditure',
        unnest(classify_expenditure(list_concat_expenditure, 0.75)) AS 'unpacked_predictions'
    FROM listed
)
SELECT * FROM output