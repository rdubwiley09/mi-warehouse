{{ config(materialized='external', location='../data/mart/committee_burn_by_month.parquet', format='parquet') }}

WITH contributions AS (
    SELECT
        cfr_committee_id,
        committee_type,
        implied_party,
        committee_common_name,
        donation_received_year,
        donation_received_month,
        total_raised
    FROM {{ ref("committee_contributions_by_month") }}
),
expenditures AS (
    SELECT
        cfr_committee_id,
        implied_party,
        committee_type,
        committee_common_name,
        expenditure_year,
        expenditure_month,
        total_spent
    FROM {{ ref("committee_expenditures_by_month")}}
),
joined AS (
    SELECT
        {{ choose_nonnull_value('contributions.cfr_committee_id', 'expenditures.cfr_committee_id', 'cfr_committee_id') }},
        {{ choose_nonnull_value('contributions.implied_party', 'expenditures.implied_party', 'implied_party') }},
        {{ choose_nonnull_value('contributions.committee_type', 'expenditures.committee_type', 'committee_type') }},
        {{ choose_nonnull_value('contributions.committee_common_name', 'expenditures.committee_common_name', 'committee_common_name') }},
        {{ choose_nonnull_value('donation_received_year', 'expenditure_year', 'year') }},
        {{ choose_nonnull_value('donation_received_month', 'expenditure_month', 'month') }},
        CASE WHEN total_raised IS NULL THEN 0 ELSE total_raised END AS total_raised,
        CASE WHEN total_spent IS NULL THEN 0 ELSE total_spent END AS total_spent
    FROM contributions
    JOIN expenditures
    ON contributions.cfr_committee_id = expenditures.cfr_committee_id
        AND contributions.donation_received_year = expenditures.expenditure_year
        AND contributions.donation_received_month = expenditures.expenditure_month
),
output AS (
    SELECT
        cfr_committee_id,
        implied_party,
        committee_type,
        committee_common_name,
        year,
        month,
        total_raised,
        total_spent,
        total_raised-total_spent AS surplus,
        CASE WHEN total_raised != 0 THEN ROUND(1.0*total_spent/total_raised,2) ELSE NULL END AS burn_rate
    FROM joined
)
SELECT * FROM output