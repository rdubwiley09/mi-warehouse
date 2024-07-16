{{ config(materialized='external', location='../data/mart/mi_elections/int_vote_by_candidate.parquet', format='parquet') }}

WITH vote AS (
    SELECT
        election_year,
        election_type,
        office_code,
        district_code,
        status_code,
        candidate_id,
        county_code,
        city_town_code,
        ward_number,
        precinct_number,
        precinct_label,
        precinct_votes,
        office_code_description
    FROM {{ ref('stg_vote')}}
    WHERE candidate_id != 0 AND precinct_votes!=0
),
candidate AS (
    SELECT
        election_year,
        candidate_id,
        candidate_last_name,
        candidate_first_name,
        candidate_middle_name,
        candidate_party_name
    FROM {{ ref('stg_name')}}
    GROUP BY ALL
),
output AS (
    SELECT
        vote.election_year,
        election_type,
        office_code,
        office_code_description,
        district_code,
        status_code,
        vote.candidate_id,
        candidate_last_name,
        candidate_first_name,
        candidate_middle_name,
        candidate_party_name,
        county_code,
        city_town_code,
        ward_number,
        precinct_number,
        precinct_label,
        precinct_votes
    FROM  vote
    JOIN candidate
    ON vote.election_year = candidate.election_year AND vote.candidate_id = candidate.candidate_id
)
SELECT * FROM output