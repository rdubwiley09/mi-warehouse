{{ config(materialized='external', location='../data/mart/mi_elections/int_precinct_two_way_results.parquet', format='parquet') }}

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
precinct_grouped AS (
    SELECT
        votes_by_candidate.election_year,
        votes_by_candidate.office_code,
        votes_by_candidate.office_code_description,
        votes_by_candidate.district,
        candidate_id,
        candidate_first_name,
        candidate_last_name,
        votes_by_candidate.candidate_party_name,
        county_code,
        city_town_code,
        ward_number,
        precinct_number,
        precinct_label,
        precinct_votes
    FROM votes_by_candidate
    JOIN bad_district_eliminator
    ON 
        votes_by_candidate.election_year = bad_district_eliminator.election_year AND
        votes_by_candidate.office_code = bad_district_eliminator.office_code AND
        votes_by_candidate.district = bad_district_eliminator.district AND
        votes_by_candidate.candidate_id = bad_district_eliminator.good_candidate_id
    WHERE votes_by_candidate.candidate_party_name IN ('DEM', 'REP') AND votes_by_candidate.office_code BETWEEN 1 and 8
),
pivoted AS (
    PIVOT precinct_grouped
    ON candidate_party_name
    USING SUM(precinct_votes)
    GROUP BY election_year, office_code, office_code_description, district, county_code, city_town_code, ward_number, precinct_number, precinct_label
),
output AS (
    SELECT
        pivoted.election_year,
        pivoted.office_code,
        pivoted.office_code_description,
        pivoted.district,
        pivoted.county_code,
        pivoted.city_town_code,
        pivoted.ward_number,
        pivoted.precinct_number,
        pivoted.precinct_label,
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
            ELSE 'Democratic Safe' END AS precinct_targeting,
        CASE WHEN Rep>Dem THEN 'Rep' ELSE 'Dem' END AS winning_party,
    FROM pivoted
)
SELECT * FROM output