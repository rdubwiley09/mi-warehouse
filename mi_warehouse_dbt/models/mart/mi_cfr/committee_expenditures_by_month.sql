{{ config(materialized='external', location='../data/mart/mi_cfr/committee_expenditures_by_month.parquet', format='parquet') }}

WITH expenditures AS (
    SELECT
        cfr_committee_id,
        committee_type,
        committee_common_name,
        expenditure_id,
        expenditure_year,
        expenditure_month,
        expenditure_amount
    FROM {{ ref("stg_expenditures") }}
),

party_lookup AS (
    SELECT
        cfr_committee_id,
        implied_party
    FROM {{ ref("implied_committee_party") }}
),

output AS (
    SELECT
        expenditures.cfr_committee_id,
        implied_party,
        committee_type,
        committee_common_name,
        expenditure_year,
        expenditure_month,
        ROUND(SUM(expenditure_amount), 2) AS total_spent,
        COUNT(DISTINCT expenditure_id) AS total_expenditures,
        MAX(expenditure_amount) AS max_expenditure
    FROM expenditures
    LEFT JOIN party_lookup
        ON expenditures.cfr_committee_id = party_lookup.cfr_committee_id
    GROUP BY ALL
)

SELECT * FROM output
