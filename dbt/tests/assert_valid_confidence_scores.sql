-- Data quality test: Ensure confidence scores are within valid range

SELECT *
FROM {{ ref('dim_companies') }}
WHERE confidence_score < 0 
   OR confidence_score > 1
   OR confidence_score IS NULL

