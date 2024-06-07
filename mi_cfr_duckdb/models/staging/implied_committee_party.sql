{{ config(materialized='view', location='../data/staging/implied_committee_party.parquet', format='parquet') }}

WITH republicans AS (
    SELECT 
        DISTINCT cfr_committee_id,
        'R' AS implied_party
    FROM {{ ref('stg_expenditures') }} 
    WHERE expenditure_last_name_or_org LIKE '%WINRED%'
),
democrats AS (
    SELECT 
        DISTINCT cfr_committee_id,
        'D' AS implied_party
    FROM {{ ref('stg_expenditures') }} 
    WHERE expenditure_last_name_or_org LIKE '%ACT BLUE%' OR expenditure_last_name_or_org LIKE '%ACTBLUE%'
),
output AS (
    SELECT
        cfr_committee_id,
        implied_party
    FROM republicans
    UNION ALL
    SELECT
        cfr_committee_id,
        implied_party
    FROM democrats
)
SELECT * FROM output