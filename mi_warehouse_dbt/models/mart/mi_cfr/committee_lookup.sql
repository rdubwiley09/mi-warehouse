{{ config(materialized='external', location='../data/mart/mi_cfr/committee_lookup.parquet', format='parquet') }}

WITH output AS (
    SELECT
        cfr_committee_id,
        MAX(committee_type) AS committee_type,
        MAX(implied_party) AS implied_party,
        MAX(committee_common_name) AS committee_common_name,
        MIN(donation_received_year) AS data_start_year,
        MAX(donation_received_year) AS data_end_year,
        MIN(donation_received_month) AS data_start_month,
        MAX(donation_received_month) AS data_end_month,
        SUM(total_raised) AS cumulative_raised
    FROM {{ ref("committee_contributions_by_month") }}
    GROUP BY cfr_committee_id
)
SELECT * FROM output