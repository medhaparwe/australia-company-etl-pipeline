-- Fact table: Match statistics and metrics
-- Aggregated view of matching performance

{{ config(
    materialized='table',
    tags=['marts', 'facts', 'metrics']
) }}

WITH matched AS (
    SELECT * FROM {{ ref('int_matched_companies') }}
),

web_stats AS (
    SELECT
        COUNT(*) AS total_web_companies,
        COUNT(CASE WHEN is_valid_name THEN 1 END) AS valid_web_companies,
        COUNT(DISTINCT domain) AS unique_domains
    FROM {{ ref('stg_web_companies') }}
),

abr_stats AS (
    SELECT
        COUNT(*) AS total_abr_entities,
        COUNT(CASE WHEN is_valid_abn THEN 1 END) AS valid_abn_count,
        COUNT(CASE WHEN entity_status = 'Active' THEN 1 END) AS active_entities,
        COUNT(DISTINCT state) AS states_represented
    FROM {{ ref('stg_abr_entities') }}
),

match_stats AS (
    SELECT
        COUNT(*) AS total_matches,
        COUNT(CASE WHEN match_confidence = 'High' THEN 1 END) AS high_confidence_matches,
        COUNT(CASE WHEN match_confidence = 'Medium' THEN 1 END) AS medium_confidence_matches,
        COUNT(CASE WHEN match_method = 'hybrid' THEN 1 END) AS llm_verified_matches,
        AVG(final_score) AS avg_match_score,
        MIN(final_score) AS min_match_score,
        MAX(final_score) AS max_match_score,
        AVG(completeness_score) AS avg_completeness
    FROM matched
),

state_breakdown AS (
    SELECT
        state,
        COUNT(*) AS match_count
    FROM matched
    WHERE state IS NOT NULL
    GROUP BY state
)

SELECT
    -- Web extraction stats
    ws.total_web_companies,
    ws.valid_web_companies,
    ws.unique_domains,
    
    -- ABR stats
    abr.total_abr_entities,
    abr.valid_abn_count,
    abr.active_entities,
    abr.states_represented,
    
    -- Match stats
    ms.total_matches,
    ms.high_confidence_matches,
    ms.medium_confidence_matches,
    ms.llm_verified_matches,
    ROUND(ms.avg_match_score::NUMERIC, 4) AS avg_match_score,
    ROUND(ms.min_match_score::NUMERIC, 4) AS min_match_score,
    ROUND(ms.max_match_score::NUMERIC, 4) AS max_match_score,
    ROUND(ms.avg_completeness::NUMERIC, 4) AS avg_completeness,
    
    -- Calculated metrics
    ROUND(
        (ms.total_matches::FLOAT / NULLIF(ws.total_web_companies, 0) * 100)::NUMERIC, 
        2
    ) AS match_rate_percent,
    
    ROUND(
        (ms.high_confidence_matches::FLOAT / NULLIF(ms.total_matches, 0) * 100)::NUMERIC, 
        2
    ) AS high_confidence_rate_percent,
    
    -- State breakdown (as JSON)
    (
        SELECT jsonb_object_agg(state, match_count)
        FROM state_breakdown
    ) AS matches_by_state,
    
    CURRENT_TIMESTAMP AS generated_at

FROM web_stats ws
CROSS JOIN abr_stats abr
CROSS JOIN match_stats ms

