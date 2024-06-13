WITH grouped AS (
    SELECT
        unique_line_identifier,
        COUNT(*) AS contribution_count
    FROM {{ ref('stg_contributions')}}
    GROUP BY unique_line_identifier
)

SELECT
    unique_line_identifier
FROM grouped
WHERE contribution_count > 1