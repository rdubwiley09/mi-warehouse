{{ config(materialized='external', location='../data/ml/dbt_ml_poc.parquet', format='parquet') }}

WITH exp AS (
    SELECT
        LOWER(CONCAT(expenditure_description, ' ', expenditure_purpose))
            AS concat_expenditure
    FROM {{ ref("stg_expenditures") }}
    LIMIT 5
),

listed AS (
    SELECT LIST(concat_expenditure) AS list_concat_expenditure
    FROM exp
),

output AS (
    SELECT
        UNNEST(list_concat_expenditure) AS concat_expenditure,
        UNNEST(CLASSIFY_EXPENDITURE(list_concat_expenditure, 0.75))
            AS unpacked_predictions
    FROM listed
)

SELECT * FROM output
