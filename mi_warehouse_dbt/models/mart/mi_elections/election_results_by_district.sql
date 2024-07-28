{{ config(materialized='external', location='../data/mart/mi_elections/election_results_by_district.parquet', format='parquet') }}

WITH votes_by_candidate AS (
    SELECT
        election_year,
        election_type,
        office_code,
        office_code_description,
        district_code,
        district_code/100 AS district,
        status_code,
        candidate_id,
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
    FROM  {{ ref('int_vote_by_candidate') }}
    WHERE status_code=0
),
bad_district_eliminator AS (
    SELECT
        election_year,
        office_code,
        office_code_description,
        district,
        candidate_party_name,
        MIN(candidate_id) AS good_candidate_id
    FROM votes_by_candidate
    GROUP BY ALL
),
district_grouped AS (
    SELECT
        votes_by_candidate.election_year,
        votes_by_candidate.office_code,
        votes_by_candidate.office_code_description,
        votes_by_candidate.district,
        candidate_id,
        candidate_first_name,
        candidate_last_name,
        votes_by_candidate.candidate_party_name,
        SUM(precinct_votes) AS votes
    FROM votes_by_candidate
    JOIN bad_district_eliminator
    ON 
        votes_by_candidate.election_year = bad_district_eliminator.election_year AND
        votes_by_candidate.office_code = bad_district_eliminator.office_code AND
        votes_by_candidate.district = bad_district_eliminator.district AND
        votes_by_candidate.candidate_id = bad_district_eliminator.good_candidate_id
    WHERE votes_by_candidate.candidate_party_name IN ('DEM', 'REP') AND votes_by_candidate.office_code BETWEEN 1 and 8
    GROUP BY ALL
),
pivoted AS (
    PIVOT district_grouped
    ON candidate_party_name
    USING SUM(votes)
    GROUP BY election_year, office_code, office_code_description, district
),
democratic_candidates AS (
    SELECT
        election_year,
        office_code_description,
        district,
        candidate_id AS democrat_candidate_id,
        candidate_first_name AS democrat_first_name,
        candidate_last_name AS democrat_last_name
    FROM district_grouped
    WHERE candidate_party_name='DEM'
),
republican_candidates AS (
    SELECT
        election_year,
        office_code_description,
        district,
        candidate_id AS republican_candidate_id,
        candidate_first_name AS republican_first_name,
        candidate_last_name AS republican_last_name
    FROM district_grouped
    WHERE candidate_party_name='REP'
),
output AS (
    SELECT
        pivoted.election_year,
        pivoted.office_code,
        pivoted.office_code_description,
        pivoted.district,
        democrat_candidate_id,
        democrat_first_name,
        democrat_last_name,
        republican_candidate_id,
        republican_first_name,
        republican_last_name,
        Dem AS democratic_votes,
        Rep AS republican_votes,
        Dem+Rep AS two_way_votes,
        1.0*Dem/(Dem+Rep) AS two_way_dem_percent,
        CASE WHEN 1.0*Dem/(Dem+Rep) < 0.4 THEN 'Republican Safe'
            WHEN 1.0*Dem/(Dem+Rep) < 0.45 THEN 'Strong Republican'
            WHEN 1.0*Dem/(Dem+Rep) < 0.475 THEN 'Lean Republican'
            WHEN 1.0*Dem/(Dem+Rep) BETWEEN 0.475 AND 0.525 THEN 'Toss-up'
            WHEN 1.0*Dem/(Dem+Rep) < 0.55 THEN 'Lean Democrat'
            WHEN 1.0*Dem/(Dem+Rep) < 0.6 THEN 'Strong Democrat'
            ELSE 'Democratic Safe' END AS district_targeting,
        CASE WHEN Rep>Dem THEN 'Rep' ELSE 'Dem' END AS winning_party,
        CASE WHEN Rep>Dem THEN republican_first_name ELSE democrat_first_name END AS winning_first_name,
        CASE WHEN Rep>Dem THEN republican_last_name ELSE democrat_last_name END AS winning_last_name
    FROM pivoted
    LEFT JOIN democratic_candidates
    ON 
        pivoted.election_year = democratic_candidates.election_year AND
        pivoted.office_code_description = democratic_candidates.office_code_description AND
        pivoted.district = democratic_candidates.district
    LEFT JOIN republican_candidates
    ON 
        pivoted.election_year = republican_candidates.election_year AND
        pivoted.office_code_description = republican_candidates.office_code_description AND
        pivoted.district = republican_candidates.district
)
SELECT * FROM output