-- Intermediate model: Matched companies with enriched data
-- Joins match results with source data

{{ config(
    materialized='table',
    tags=['intermediate', 'matching']
) }}

WITH matches AS (
    SELECT * FROM {{ source('raw', 'entity_match_results') }}
    WHERE final_score >= {{ var('fuzzy_match_threshold') }}
),

web_companies AS (
    SELECT * FROM {{ ref('stg_web_companies') }}
),

abr_entities AS (
    SELECT * FROM {{ ref('stg_abr_entities') }}
),

enriched_matches AS (
    SELECT
        m.id AS match_id,
        m.abn,
        m.final_score,
        m.fuzzy_score,
        m.llm_score,
        m.match_method,
        
        -- Common Crawl data
        wc.url AS website_url,
        wc.domain,
        wc.company_name AS web_company_name,
        wc.industry AS web_industry,
        
        -- ABR data
        ae.entity_name AS abr_entity_name,
        ae.entity_type_desc AS entity_type,
        ae.entity_status,
        ae.state,
        ae.postcode,
        ae.start_date,
        
        -- Match quality indicators
        CASE 
            WHEN m.final_score >= {{ var('high_confidence_threshold') }} THEN 'High'
            WHEN m.final_score >= {{ var('fuzzy_match_threshold') }} THEN 'Medium'
            ELSE 'Low'
        END AS match_confidence,
        
        -- Data completeness score
        (
            CASE WHEN wc.url IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN wc.industry IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN ae.state IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN ae.postcode IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN ae.start_date IS NOT NULL THEN 1 ELSE 0 END
        )::FLOAT / 5 AS completeness_score,
        
        CURRENT_TIMESTAMP AS processed_at
        
    FROM matches m
    LEFT JOIN web_companies wc 
        ON m.crawl_url = wc.url
    LEFT JOIN abr_entities ae 
        ON m.abn = ae.abn
)

SELECT * FROM enriched_matches

