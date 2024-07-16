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