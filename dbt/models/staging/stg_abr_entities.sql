-- Staging model for ABR entities
-- Applies validation and standardization

{{ config(
    materialized='view',
    tags=['staging', 'abr']
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'abr_entities') }}
),

cleaned AS (
    SELECT
        abn,
        
        -- Clean entity name
        TRIM(entity_name) AS entity_name,
        
        -- Normalized name for matching
        UPPER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(entity_name, '(PTY|LTD|LIMITED|PROPRIETARY)', '', 'gi'),
                    '[^\w\s]', ' ', 'g'
                ),
                '\s+', ' ', 'g'
            )
        ) AS normalized_name,
        
        -- Block key for matching
        LOWER(LEFT(normalized_name, 4)) AS block_key,
        
        -- Entity type description
        CASE entity_type
            WHEN 'PRV' THEN 'Private Company'
            WHEN 'PUB' THEN 'Public Company'
            WHEN 'IND' THEN 'Individual/Sole Trader'
            WHEN 'TRT' THEN 'Trust'
            WHEN 'PNR' THEN 'Partnership'
            ELSE entity_type
        END AS entity_type_desc,
        
        entity_type AS entity_type_code,
        
        -- Status standardization
        CASE 
            WHEN LOWER(entity_status) LIKE '%active%' THEN 'Active'
            WHEN LOWER(entity_status) LIKE '%cancel%' THEN 'Cancelled'
            ELSE entity_status
        END AS entity_status,
        
        -- State validation
        CASE 
            WHEN UPPER(state) IN ('NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT') 
                THEN UPPER(state)
            ELSE NULL
        END AS state,
        
        -- Postcode validation (4 digits)
        CASE 
            WHEN postcode ~ '^\d{4}$' THEN postcode
            ELSE NULL
        END AS postcode,
        
        start_date,
        
        -- ABN validation (basic check)
        CASE 
            WHEN LENGTH(REGEXP_REPLACE(abn, '\D', '', 'g')) = 11 THEN TRUE
            ELSE FALSE
        END AS is_valid_abn,
        
        created_at
        
    FROM source
    WHERE entity_name IS NOT NULL
      AND LENGTH(TRIM(entity_name)) >= 3
)

SELECT * FROM cleaned

