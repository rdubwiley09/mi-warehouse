{{ config(materialized='external', location='../data/mart/committee_contributions_by_month.parquet', format='parquet') }}

WITH contributions AS (
    SELECT 
        cfr_committee_id,
        committee_common_name,
        donation_received_year,
        donation_received_month,
        contribution_amount
    FROM {{ ref("stg_contributions")}}
),
output AS (
    SELECT 
        cfr_committee_id,
        committee_common_name,
        donation_received_year,
        donation_received_month,
        ROUND(SUM(contribution_amount),2) AS total_raised
    FROM contributions
    GROUP BY
        cfr_committee_id,
        committee_common_name,
        donation_received_year,
        donation_received_month
)
SELECT * FROM output