-- Data quality test: Ensure minimum match rate
-- Fails if match rate drops below 20%

WITH stats AS (
    SELECT * FROM {{ ref('fct_match_statistics') }}
)

SELECT *
FROM stats
WHERE match_rate_percent < 20

