{{ config(materialized='external', location='../data/mart/mi_cfr/state_party_contributions_pivot.parquet', format='parquet') }}

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

output AS (
    SELECT
        branch,
        donation_received_year,
        ROUND(democratic, 0) AS total_democratic,
        ROUND(republican, 0) AS total_republican,
        ROUND(democratic - republican, 0) AS fundraising_margin
    FROM pivoted
)

SELECT * FROM output
