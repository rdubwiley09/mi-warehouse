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
results_2020 AS (
    SELECT 
        election_year,
        office_code,
        office_code_description,
        district,
        county_code,
        city_town_code,
        ward_number,
        precinct_number,
        democratic_votes AS democratic_votes_2020,
        two_way_votes AS two_way_votes_2020,
        1.0*democratic_votes/two_way_votes AS two_way_dem_percent_2020
    FROM precinct_two_way_results
    WHERE election_year=2020
),
results_2018 AS (
    SELECT 
        election_year,
        office_code,
        office_code_description,
        district,
        county_code,
        city_town_code,
        ward_number,
        precinct_number,
        democratic_votes AS democratic_votes_2018,
        two_way_votes AS two_way_votes_2018,
        1.0*democratic_votes/two_way_votes AS two_way_dem_percent_2018
    FROM precinct_two_way_results
    WHERE election_year=2018
),
results_2016 AS (
    SELECT 
        election_year,
        office_code,
        office_code_description,
        district,
        county_code,
        city_town_code,
        ward_number,
        precinct_number,
        democratic_votes AS democratic_votes_2016,
        two_way_votes AS two_way_votes_2016,
        1.0*democratic_votes/two_way_votes AS two_way_dem_percent_2016
    FROM precinct_two_way_results
    WHERE election_year=2016
),
results_2014 AS (
    SELECT 
        election_year,
        office_code,
        office_code_description,
        district,
        county_code,
        city_town_code,
        ward_number,
        precinct_number,
        democratic_votes AS democratic_votes_2014,
        two_way_votes AS two_way_votes_2014,
        1.0*democratic_votes/two_way_votes AS two_way_dem_percent_2014
    FROM precinct_two_way_results
    WHERE election_year=2014
),
output AS (
    SELECT
        results_2022.office_code,
        results_2022.office_code_description,
        results_2022.district,
        results_2022.county_code,
        results_2022.city_town_code,
        results_2022.ward_number,
        results_2022.precinct_number,
        two_way_votes_2022,
        democratic_votes_2022,
        two_way_dem_percent_2022,
        two_way_votes_2020,
        democratic_votes_2020,
        two_way_dem_percent_2020,
        two_way_votes_2018,
        democratic_votes_2018,
        two_way_dem_percent_2018,
        two_way_votes_2016,
        democratic_votes_2016,
        two_way_dem_percent_2016,
        two_way_votes_2014,
        democratic_votes_2014,
        two_way_dem_percent_2014
    FROM results_2022
    LEFT JOIN results_2020
    ON results_2022.county_code = results_2020.county_code
        AND results_2022.city_town_code = results_2020.city_town_code
        AND results_2022.ward_number = results_2020.ward_number
        AND results_2022.precinct_number = results_2020.precinct_number
    LEFT JOIN results_2018
    ON results_2022.county_code = results_2018.county_code
        AND results_2022.city_town_code = results_2018.city_town_code
        AND results_2022.ward_number = results_2018.ward_number
        AND results_2022.precinct_number = results_2018.precinct_number
    LEFT JOIN results_2016
    ON results_2022.county_code = results_2016.county_code
        AND results_2022.city_town_code = results_2016.city_town_code
        AND results_2022.ward_number = results_2016.ward_number
        AND results_2022.precinct_number = results_2016.precinct_number
    LEFT JOIN results_2014
    ON results_2022.county_code = results_2014.county_code
        AND results_2022.city_town_code = results_2014.city_town_code
        AND results_2022.ward_number = results_2014.ward_number
        AND results_2022.precinct_number = results_2014.precinct_number
)
SELECT * FROM output