-- Dimension table: Unified company records (Golden Record)
-- Final mart model for analytics

{{ config(
    materialized='table',
    tags=['marts', 'dimension'],
    unique_key='abn'
) }}

WITH matched AS (
    SELECT * FROM {{ ref('int_matched_companies') }}
),

-- Get best match per ABN (highest score)
best_matches AS (
    SELECT DISTINCT ON (abn)
        abn,
        abr_entity_name,
        web_company_name,
        website_url,
        domain,
        web_industry,
        entity_type,
        entity_status,
        state,
        postcode,
        start_date,
        final_score,
        match_method,
        match_confidence,
        completeness_score
    FROM matched
    ORDER BY abn, final_score DESC
),

-- Include unmatched ABR entities
abr_only AS (
    SELECT
        ae.abn,
        ae.entity_name AS abr_entity_name,
        NULL AS web_company_name,
        NULL AS website_url,
        NULL AS domain,
        NULL AS web_industry,
        ae.entity_type_desc AS entity_type,
        ae.entity_status,
        ae.state,
        ae.postcode,
        ae.start_date,
        1.0 AS final_score,  -- ABR is authoritative
        'abr_only' AS match_method,
        'High' AS match_confidence,
        (
            CASE WHEN ae.state IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN ae.postcode IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN ae.start_date IS NOT NULL THEN 1 ELSE 0 END
        )::FLOAT / 3 AS completeness_score
    FROM {{ ref('stg_abr_entities') }} ae
    WHERE ae.abn NOT IN (SELECT abn FROM best_matches WHERE abn IS NOT NULL)
      AND ae.entity_status = 'Active'
),

final AS (
    SELECT * FROM best_matches
    UNION ALL
    SELECT * FROM abr_only
)

SELECT
    abn,
    
    -- Canonical name: prefer ABR name
    COALESCE(abr_entity_name, web_company_name) AS canonical_name,
    
    -- Trading name: from web if different
    CASE 
        WHEN web_company_name IS NOT NULL 
             AND web_company_name != abr_entity_name 
        THEN web_company_name
        ELSE NULL
    END AS trading_name,
    
    website_url,
    domain,
    
    -- Industry from web extraction
    web_industry AS industry,
    
    entity_type,
    entity_status,
    state,
    postcode,
    start_date AS registration_date,
    
    -- Source tracking
    CASE 
        WHEN web_company_name IS NOT NULL AND abr_entity_name IS NOT NULL THEN 'MERGED'
        WHEN web_company_name IS NOT NULL THEN 'WEB_ONLY'
        ELSE 'ABR_ONLY'
    END AS data_source,
    
    final_score AS confidence_score,
    match_method,
    match_confidence,
    completeness_score,
    
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at

FROM final

