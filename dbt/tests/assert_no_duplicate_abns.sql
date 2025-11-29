-- Data quality test: Ensure no duplicate ABNs in unified companies

SELECT
    abn,
    COUNT(*) AS duplicate_count
FROM {{ ref('dim_companies') }}
GROUP BY abn
HAVING COUNT(*) > 1

