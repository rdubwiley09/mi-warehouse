{{ config(materialized='external', location='../data/mart/mi_cfr/committee_contributions_by_month.parquet', format='parquet') }}

WITH contributions AS (
    SELECT
        cfr_committee_id,
        committee_type,
        committee_common_name,
        contribution_id,
        donation_received_year,
        donation_received_month,
        contribution_amount
    FROM {{ ref("stg_contributions") }}
),

party_lookup AS (
    SELECT
        cfr_committee_id,
        implied_party
    FROM {{ ref("implied_committee_party") }}
),

output AS (
    SELECT
        contributions.cfr_committee_id,
        implied_party,
        committee_type,
        committee_common_name,
        donation_received_year,
        donation_received_month,
        ROUND(SUM(contribution_amount), 2) AS total_raised,
        COUNT(DISTINCT contribution_id) AS total_contributions,
        ROUND(
            ROUND(SUM(contribution_amount), 2)
            / COUNT(DISTINCT contribution_id),
            2
        ) AS average_contribution
    FROM contributions
    LEFT JOIN party_lookup
        ON contributions.cfr_committee_id = party_lookup.cfr_committee_id
    GROUP BY ALL
)

SELECT * FROM output
