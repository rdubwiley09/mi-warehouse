{{ config(materialized='external', location='../data/mart/state_party_contributions_pivot.parquet', format='parquet') }}

WITH contributions AS (
    SELECT
        organization,
        party,
        branch,
        contribution_received_date,
        donation_received_year,
        donation_received_month,
        total_raised
    FROM {{ ref('state_party_contributions') }}
),
pivoted AS (
    PIVOT contributions
    ON party
    USING SUM(total_raised)
    GROUP BY branch, donation_received_year
    ORDER BY donation_received_year
),
OUTPUT AS (
    SELECT
        branch,
        donation_received_year,
        ROUND(Democratic,0) AS total_democratic,
        ROUND(Republican,0) AS total_republican,
        ROUND(Democratic-Republican,0) AS fundraising_margin
    FROM pivoted
)
SELECT * FROM output