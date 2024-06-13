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
        CASE WHEN district_code=0 THEN 'Pollbook'
            WHEN district_code=100 THEN 'President'
            WHEN district_code=200 THEN 'Governor'
            WHEN district_code=300 THEN 'SOS'
            WHEN district_code=400 THEN 'AG'
            WHEN district_code=500 THEN 'US Senator'
            WHEN district_code=600 THEN 'US Congress'
            WHEN district_code=700 THEN 'State Senator'
            WHEN district_code=800 THEN 'State Representative'
            WHEN district_code=900 THEN 'State BOE'
            WHEN district_code=1000 THEN 'University of Michigan Board of Regents'
            WHEN district_code=1100 THEN 'Michigan State University Board of Trustees'
            WHEN district_code=1200 THEN 'Wayne State University Board of Governors'
            WHEN district_code=1300 THEN 'Michigan Supreme Court'
            WHEN district_code=9000 THEN 'Statewide Ballot Proposal'
            ELSE 'Unknown' END AS district_code_description
    FROM raw_vote
)

SELECT * FROM output
