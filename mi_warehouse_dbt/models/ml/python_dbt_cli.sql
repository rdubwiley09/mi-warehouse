{{ config(materialized='external', location='../data/ml/python_dbt_cli.parquet', format='parquet') }}

WITH output AS (
    SELECT
        predicted_label,
        COUNT(*) AS count
    FROM READ_PARQUET('../data/ml/model_output.parquet', union_by_name = True)
    GROUP BY ALL
)

SELECT * FROM output
