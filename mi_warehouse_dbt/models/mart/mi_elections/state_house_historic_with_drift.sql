{{ config(materialized='external', location='../data/mart/mi_elections/state_house_historic_with_drift.parquet', format='parquet') }}
/*
WITH historical AS (
    SELECT 
        district,
        SUM(two_way_votes_2022) AS two_way_votes_2022,
        SUM(democratic_votes_2022) AS democratic_votes_2022,
        SUM(two_way_votes_2020) AS two_way_votes_2020,
        SUM(democratic_votes_2020) AS democratic_votes_2020,
        SUM(two_way_votes_2018) AS two_way_votes_2018,
        SUM(democratic_votes_2018) AS democratic_votes_2018,
        SUM(two_way_votes_2016) AS two_way_votes_2016,
        SUM(democratic_votes_2016) AS democratic_votes_2016,
        SUM(two_way_votes_2014) AS two_way_votes_2014,
        SUM(democratic_votes_2014) AS democratic_votes_2014
    FROM {{ ref('int_historical_state_house_trends_by_precinct') }}
    GROUP BY district
),
historical_with_percentages AS (
    SELECT *,
        1.0*democratic_votes_2022/two_way_votes_2022 AS dem_two_way_percent_2022,
        1.0*democratic_votes_2020/two_way_votes_2020 AS dem_two_way_percent_2020,
        1.0*democratic_votes_2018/two_way_votes_2018 AS dem_two_way_percent_2018,
        1.0*democratic_votes_2016/two_way_votes_2016 AS dem_two_way_percent_2016,
        1.0*democratic_votes_2014/two_way_votes_2014 AS dem_two_way_percent_2014,
        1.0*two_way_votes_2018/two_way_votes_2020 AS percentage_turnout_retained_2018_2022
    FROM historical
),
with_drift AS (
    SELECT *,
        (dem_two_way_percent_2018+dem_two_way_percent_2020+dem_two_way_percent_2022)/3.0 AS last_three_average_dem_two_way_result,
        (dem_two_way_percent_2018+dem_two_way_percent_2020+dem_two_way_percent_2022)/3.0-0.5 AS partisan_voting_index,
        dem_two_way_percent_2022 - dem_two_way_percent_2020 AS drift_2022_2020,
        dem_two_way_percent_2022 - dem_two_way_percent_2018 AS drift_2022_2018,
        dem_two_way_percent_2022 - dem_two_way_percent_2016 AS drift_2022_2016,
        dem_two_way_percent_2022 - dem_two_way_percent_2014 AS drift_2022_2014,
        CASE WHEN percentage_turnout_retained_2018_2022 > 0.8 THEN (dem_two_way_percent_2018+dem_two_way_percent_2020+dem_two_way_percent_2022)/3.0
            ELSE dem_two_way_percent_2022 END AS dem_two_way_percent_analytics
    FROM historical_with_percentages
),
targeting_output AS (
    SELECT *,
        CASE WHEN dem_two_way_percent_analytics >= 0.6 THEN 'Safe Dem'
            WHEN dem_two_way_percent_analytics > 0.55 THEN 'Strong Dem'
            WHEN dem_two_way_percent_analytics > 0.525 THEN 'Lean Dem'
            WHEN dem_two_way_percent_analytics > 0.475 THEN 'Tossup'
            WHEN dem_two_way_percent_analytics > 0.45 THEN 'Lean Rep'
            WHEN dem_two_way_percent_analytics > 0.40 THEN 'Strong Rep'
            WHEN dem_two_way_percent_analytics <= 0.40 THEN 'Safe Rep'
            Else 'Safe Dem' END AS district_targeting
    FROM with_drift
),
results AS (
    SELECT
        district,
        winning_party AS winning_party_2024,
        winning_first_name AS winning_first_name_2024,
        winning_last_name AS winning_last_name_2024
    FROM {{ ref('election_results_by_district') }}
    WHERE election_year = 2024 AND office_code = 8
),
output AS (
    SELECT
        targeting_output.district,
        winning_party_2022,
        district_targeting,
        dem_two_way_percent_analytics,
        percentage_turnout_retained_2018_2022,
        partisan_voting_index,
        winning_first_name_2022,
        winning_last_name_2022,
        two_way_votes_2022,
        democratic_votes_2022,
        two_way_votes_2020,
        democratic_votes_2020,
        two_way_votes_2018,
        democratic_votes_2018,
        two_way_votes_2016,
        democratic_votes_2016,
        two_way_votes_2014,
        democratic_votes_2014,
        dem_two_way_percent_2022,
        dem_two_way_percent_2020,
        dem_two_way_percent_2018,
        dem_two_way_percent_2016,
        dem_two_way_percent_2014,
        last_three_average_dem_two_way_result,
        drift_2022_2020,
        drift_2022_2018,
        drift_2022_2016,
        drift_2022_2014
    FROM targeting_output
    JOIN results
    ON targeting_output.district = results.district
)

SELECT * FROM output
*/