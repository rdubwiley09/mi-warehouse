{{ config(materialized='external', location='../data/mart/mi_elections/office_control.parquet', format='parquet') }}

WITH election_results AS (
    SELECT
        election_year,
        office_code_description,
        SUM(CASE WHEN winning_party='Dem' THEN 1 ELSE 0 END) AS democratic_seats,
        COUNT(*) AS total_seats
    FROM {{ ref('election_results_by_district') }}
    GROUP BY ALL
),
output AS (
    SELECT
        election_year,
        office_code_description,
        democratic_seats,
        total_seats,
        CASE WHEN democratic_seats/total_seats >= 0.5 THEN 'Dem' ELSE 'Rep' END as party_control
    FROM election_results
)

SELECT * FROM output