{{ config(materialized='external', location='../data/staging/stg_vote.parquet', format='parquet') }}

WITH raw_vote AS (
    SELECT
        "0" AS election_year,
        "1" AS election_type,
        "2" AS office_code,
        "3" AS district_code,
        "4" AS status_code,
        "5" AS candidate_id,
        "6" AS county_code,
        "7" AS city_town_code,
        "8" AS ward_number,
        "9" AS precinct_number,
        "10" AS precinct_label,
        "11" AS precinct_votes
    FROM 
        read_parquet(
            '../data/raw/election_results/*vote.parquet', union_by_name = True
        )
),

output AS (
    SELECT *,
        CASE WHEN office_code=0 THEN 'Pollbook'
            WHEN office_code=1 THEN 'President'
            WHEN office_code=2 THEN 'Governor'
            WHEN office_code=3 THEN 'SOS'
            WHEN office_code=4 THEN 'AG'
            WHEN office_code=5 THEN 'US Senator'
            WHEN office_code=6 THEN 'US Congress'
            WHEN office_code=7 THEN 'State Senator'
            WHEN office_code=8 THEN 'State Representative'
            WHEN office_code=9 THEN 'State BOE'
            WHEN office_code=10 THEN 'University of Michigan Board of Regents'
            WHEN office_code=11 THEN 'Michigan State University Board of Trustees'
            WHEN office_code=12 THEN 'Wayne State University Board of Governors'
            WHEN office_code=13 THEN 'Michigan Supreme Court'
            WHEN office_code=90 THEN 'Statewide Ballot Proposal'
            ELSE 'Unknown' END AS office_code_description
    FROM raw_vote
)

SELECT * FROM output
