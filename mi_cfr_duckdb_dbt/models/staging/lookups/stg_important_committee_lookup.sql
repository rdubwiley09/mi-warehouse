WITH output AS (
    SELECT
        cfr_committee_id, 
        organization, 
        branch, 
        party
    FROM '../data/staging/lookups/important_committee_lookup.csv'
)
SELECT * FROM output