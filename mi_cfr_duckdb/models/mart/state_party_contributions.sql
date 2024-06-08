{{ config(materialized='external', location='../data/mart/state_party_contributions.parquet', format='parquet') }}

WITH contributions AS (
    SELECT
        cfr_committee_id,
        CASE 
            WHEN cfr_committee_id=502755 THEN 'House Democratic Fund' 
            WHEN cfr_committee_id=436 THEN 'House Republican Fund' 
            WHEN cfr_committee_id=503510 THEN 'Senate Democratic Fund'
            WHEN cfr_committee_id=2399 THEN 'Senate Republican Fund'
        END AS organization,
        CASE 
            WHEN cfr_committee_id IN (502755, 436) THEN 'House'
            WHEN cfr_committee_id IN (503510, 2399) THEN 'Senate'
        END AS branch,
        CASE 
            WHEN cfr_committee_id IN (502755, 503510) THEN 'Democratic'
            WHEN cfr_committee_id IN (436, 2399) THEN 'Republican'
        END AS party,
        strftime(contribution_received_date, '%Y-%m-%d') AS contribution_received_date,
        donation_received_year,
        donation_received_month,
        SUM(contribution_amount) AS contribution_amount
    FROM {{ ref('stg_contributions') }}
    WHERE contribution_type != 'LOAN FROM A PERSON'
    GROUP BY ALL

),
output AS (
    SELECT
        organization,
        party,
        branch,
        contribution_received_date,
        donation_received_year,
        donation_received_month,
        contribution_amount AS total_raised,
        SUM(contribution_amount) OVER (PARTITION BY organization ORDER BY contribution_received_date) AS total_raised_cumulative,
        SUM(contribution_amount) OVER (PARTITION BY organization, donation_received_year ORDER BY contribution_received_date) AS total_raised_cumulative_year
    FROM contributions
    WHERE cfr_committee_id IN (502755, 436, 503510, 2399)
    GROUP BY organization, party, branch, contribution_received_date, contribution_amount, donation_received_month, donation_received_year
)
SELECT * FROM output
