{{ config(materialized='external', location='../data/mart/mi_cfr/state_party_contributions.parquet', format='parquet') }}

WITH important_committee_lookup AS (
    SELECT
        cfr_committee_id,
        organization,
        branch,
        party
    FROM {{ ref('stg_important_committee_lookup') }}
),

contributions AS (
    SELECT
        cfr_committee_id,
        donation_received_year,
        donation_received_month,
        strftime(contribution_received_date, '%Y-%m-%d')
            AS contribution_received_date,
        sum(contribution_amount) AS contribution_amount
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
        sum(contribution_amount)
            OVER (PARTITION BY organization ORDER BY contribution_received_date)
            AS total_raised_cumulative,
        sum(contribution_amount)
            OVER (
                PARTITION BY organization, donation_received_year
                ORDER BY contribution_received_date
            )
            AS total_raised_cumulative_year
    FROM contributions
    INNER JOIN important_committee_lookup
        ON
            contributions.cfr_committee_id
            = important_committee_lookup.cfr_committee_id
    GROUP BY
        organization,
        party,
        branch,
        contribution_received_date,
        contribution_amount,
        donation_received_month,
        donation_received_year
)

SELECT * FROM output
