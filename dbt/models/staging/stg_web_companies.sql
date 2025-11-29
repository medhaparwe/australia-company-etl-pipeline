-- Staging model for Common Crawl web companies
-- Applies basic cleaning and standardization

{{ config(
    materialized='view',
    tags=['staging', 'commoncrawl']
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'web_companies') }}
),

cleaned AS (
    SELECT
        id,
        url,
        domain,
        
        -- Clean company name
        TRIM(company_name) AS company_name,
        
        -- Normalized name (uppercase, no punctuation)
        UPPER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(company_name, '[^\w\s]', ' ', 'g'),
                '\s+', ' ', 'g'
            )
        ) AS normalized_name,
        
        -- Block key for matching
        LOWER(LEFT(normalized_name, 4)) AS block_key,
        
        -- Industry standardization
        CASE
            WHEN LOWER(industry) LIKE '%tech%' OR LOWER(industry) LIKE '%software%' 
                THEN 'Information Technology'
            WHEN LOWER(industry) LIKE '%financ%' OR LOWER(industry) LIKE '%bank%' 
                THEN 'Financial Services'
            WHEN LOWER(industry) LIKE '%health%' OR LOWER(industry) LIKE '%medical%' 
                THEN 'Healthcare'
            WHEN LOWER(industry) LIKE '%retail%' OR LOWER(industry) LIKE '%shop%' 
                THEN 'Retail'
            WHEN LOWER(industry) LIKE '%construct%' OR LOWER(industry) LIKE '%build%' 
                THEN 'Construction'
            WHEN LOWER(industry) LIKE '%mining%' 
                THEN 'Mining & Resources'
            ELSE industry
        END AS industry,
        
        -- Text preview (first 500 chars)
        LEFT(raw_text, 500) AS text_preview,
        
        created_at,
        
        -- Data quality flags
        CASE 
            WHEN company_name IS NULL OR LENGTH(TRIM(company_name)) < 3 THEN FALSE
            ELSE TRUE
        END AS is_valid_name,
        
        CASE 
            WHEN url IS NULL OR url NOT LIKE 'http%' THEN FALSE
            ELSE TRUE
        END AS is_valid_url
        
    FROM source
    WHERE company_name IS NOT NULL
      AND LENGTH(TRIM(company_name)) >= 3
)

SELECT * FROM cleaned

