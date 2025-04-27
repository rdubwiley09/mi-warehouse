{{ config(materialized='external', location='../data/mart/mi_elections/int_historical_state_house_trends_by_precinct.parquet', format='parquet') }}

WITH precinct_two_way_results AS (
    SELECT
        election_year,
        office_code,
        office_code_description,
        district,
        county_code,
        city_town_code,
        ward_number,
        precinct_number,
        SUM(democratic_votes) AS democratic_votes,
        SUM(republican_votes) AS republican_votes,
        SUM(two_way_votes) AS two_way_votes,
    FROM {{ ref('int_precinct_two_way_results') }}
    WHERE office_code=8
    GROUP BY ALL
),
results_2024 AS (
    SELECT 
        election_year,
        office_code,
        office_code_description,
        district,
        county_code,
        city_town_code,
        ward_number,
        precinct_number,
        democratic_votes AS democratic_votes_2024,
        two_way_votes AS two_way_votes_2024,
        1.0*democratic_votes/two_way_votes AS two_way_dem_percent_2024
    FROM precinct_two_way_results
    WHERE election_year=2022
),
results_2022 AS (
    SELECT 
        election_year,
        office_code,
        office_code_description,
        district,
        county_code,
        city_town_code,
        ward_number,
        precinct_number,
        democratic_votes AS democratic_votes_2022,
        two_way_votes AS two_way_votes_2022,
        1.0*democratic_votes/two_way_votes AS two_way_dem_percent_2022
    FROM precinct_two_way_results
    WHERE election_year=2022
),
output AS (
    SELECT
        results_2024.office_code,
        results_2024.office_code_description,
        results_2024.district,
        results_2024.county_code,
        results_2024.city_town_code,
        results_2024.ward_number,
        results_2024.precinct_number,
        two_way_votes_2024,
        democratic_votes_2024,
        two_way_dem_percent_2024,
        two_way_votes_2022,
        democratic_votes_2022,
        two_way_dem_percent_2022
    FROM results_2024
    LEFT JOIN results_2022
    ON results_2024.county_code = results_2022.county_code
        AND results_2024.city_town_code = results_2022.city_town_code
        AND results_2024.ward_number = results_2022.ward_number
        AND results_2024.precinct_number = results_2022.precinct_number
)
SELECT * FROM output