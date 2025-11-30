-- Australia Company ETL Pipeline - Database Schema
-- PostgreSQL 14+

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- For fuzzy text matching

-- ============================================
-- Source Tables (Raw Data)
-- ============================================

-- Common Crawl extracted companies
CREATE TABLE IF NOT EXISTS web_companies (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    domain TEXT,
    company_name TEXT,
    normalized_name TEXT,
    industry TEXT,
    raw_text TEXT,
    block_key VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for web_companies
CREATE INDEX IF NOT EXISTS idx_web_companies_domain ON web_companies(domain);
CREATE INDEX IF NOT EXISTS idx_web_companies_block_key ON web_companies(block_key);
CREATE INDEX IF NOT EXISTS idx_web_companies_normalized_name ON web_companies USING gin(normalized_name gin_trgm_ops);

-- ABR entities
CREATE TABLE IF NOT EXISTS abr_entities (
    abn VARCHAR(11) PRIMARY KEY,
    entity_name TEXT NOT NULL,
    normalized_name TEXT,
    entity_type VARCHAR(500),
    entity_type_code VARCHAR(10),
    entity_status VARCHAR(20),
    state VARCHAR(10),
    postcode VARCHAR(10),
    start_date DATE,
    block_key VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for abr_entities
CREATE INDEX IF NOT EXISTS idx_abr_entities_block_key ON abr_entities(block_key);
CREATE INDEX IF NOT EXISTS idx_abr_entities_state ON abr_entities(state);
CREATE INDEX IF NOT EXISTS idx_abr_entities_status ON abr_entities(entity_status);
CREATE INDEX IF NOT EXISTS idx_abr_entities_normalized_name ON abr_entities USING gin(normalized_name gin_trgm_ops);

-- ============================================
-- Matching Results Table
-- ============================================

CREATE TABLE IF NOT EXISTS entity_match_results (
    id BIGSERIAL PRIMARY KEY,
    crawl_name TEXT,
    crawl_url TEXT,
    abr_name TEXT,
    abn VARCHAR(11) REFERENCES abr_entities(abn),
    fuzzy_score DECIMAL(5,4),
    llm_score DECIMAL(5,4),
    final_score DECIMAL(5,4),
    match_method VARCHAR(20),  -- 'fuzzy', 'llm', 'hybrid'
    state VARCHAR(10),
    postcode VARCHAR(10),
    start_date DATE,
    is_verified BOOLEAN DEFAULT FALSE,
    verified_by VARCHAR(100),
    verified_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(crawl_url, abn)
);

-- Create indexes for match results
CREATE INDEX IF NOT EXISTS idx_match_results_abn ON entity_match_results(abn);
CREATE INDEX IF NOT EXISTS idx_match_results_score ON entity_match_results(final_score DESC);
CREATE INDEX IF NOT EXISTS idx_match_results_method ON entity_match_results(match_method);

-- ============================================
-- Unified Company Master Table (Golden Record)
-- ============================================

CREATE TABLE IF NOT EXISTS unified_companies (
    abn VARCHAR(11) PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    trading_name TEXT,
    url TEXT,
    domain TEXT,
    industry TEXT,
    entity_type VARCHAR(500),
    entity_status VARCHAR(20),
    state VARCHAR(10),
    postcode VARCHAR(10),
    start_date DATE,
    source VARCHAR(20),  -- 'ABR', 'CC', 'MERGED'
    confidence_score DECIMAL(5,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for unified companies
CREATE INDEX IF NOT EXISTS idx_unified_domain ON unified_companies(domain);
CREATE INDEX IF NOT EXISTS idx_unified_state ON unified_companies(state);
CREATE INDEX IF NOT EXISTS idx_unified_industry ON unified_companies(industry);
CREATE INDEX IF NOT EXISTS idx_unified_status ON unified_companies(entity_status);
CREATE INDEX IF NOT EXISTS idx_unified_name ON unified_companies USING gin(canonical_name gin_trgm_ops);

-- ============================================
-- Audit & Pipeline Tracking Tables
-- ============================================

-- Pipeline run history
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_id UUID DEFAULT gen_random_uuid(),
    pipeline_name VARCHAR(100),
    status VARCHAR(20),  -- 'running', 'completed', 'failed'
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    records_processed INTEGER DEFAULT 0,
    records_matched INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0,
    error_message TEXT,
    config JSONB
);

-- Data quality metrics
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id SERIAL PRIMARY KEY,
    run_id UUID,
    table_name VARCHAR(100),
    metric_name VARCHAR(100),
    metric_value DECIMAL(10,4),
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Views
-- ============================================

-- High-confidence matches view
CREATE OR REPLACE VIEW v_high_confidence_matches AS
SELECT 
    emr.id,
    emr.crawl_name,
    emr.crawl_url,
    emr.abr_name,
    emr.abn,
    emr.final_score,
    emr.match_method,
    ae.entity_type,
    ae.entity_status,
    ae.state,
    ae.postcode
FROM entity_match_results emr
JOIN abr_entities ae ON emr.abn = ae.abn
WHERE emr.final_score >= 0.85;

-- Active Australian companies view
CREATE OR REPLACE VIEW v_active_companies AS
SELECT 
    uc.abn,
    uc.canonical_name,
    uc.trading_name,
    uc.url,
    uc.industry,
    uc.entity_type,
    uc.state,
    uc.postcode,
    uc.start_date,
    uc.confidence_score
FROM unified_companies uc
WHERE uc.entity_status = 'Active';

-- Match summary by state view
CREATE OR REPLACE VIEW v_matches_by_state AS
SELECT 
    state,
    COUNT(*) as total_matches,
    AVG(final_score) as avg_score,
    COUNT(CASE WHEN match_method = 'hybrid' THEN 1 END) as llm_verified,
    COUNT(CASE WHEN final_score >= 0.90 THEN 1 END) as high_confidence
FROM entity_match_results
GROUP BY state
ORDER BY total_matches DESC;

-- ============================================
-- Functions
-- ============================================

-- Function to update timestamps
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply timestamp trigger to tables
DROP TRIGGER IF EXISTS update_web_companies_timestamp ON web_companies;
CREATE TRIGGER update_web_companies_timestamp
    BEFORE UPDATE ON web_companies
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();

DROP TRIGGER IF EXISTS update_abr_entities_timestamp ON abr_entities;
CREATE TRIGGER update_abr_entities_timestamp
    BEFORE UPDATE ON abr_entities
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();

DROP TRIGGER IF EXISTS update_unified_companies_timestamp ON unified_companies;
CREATE TRIGGER update_unified_companies_timestamp
    BEFORE UPDATE ON unified_companies
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();

-- ============================================
-- Sample Queries
-- ============================================

-- Find potential duplicates in ABR
-- SELECT entity_name, COUNT(*) as cnt
-- FROM abr_entities
-- GROUP BY normalized_name
-- HAVING COUNT(*) > 1;

-- Search companies by name (fuzzy)
-- SELECT * FROM unified_companies
-- WHERE canonical_name % 'ACME'
-- ORDER BY similarity(canonical_name, 'ACME') DESC
-- LIMIT 10;

-- Match statistics
-- SELECT 
--     match_method,
--     COUNT(*) as count,
--     AVG(final_score) as avg_score,
--     MIN(final_score) as min_score,
--     MAX(final_score) as max_score
-- FROM entity_match_results
-- GROUP BY match_method;

COMMENT ON TABLE web_companies IS 'Companies extracted from Common Crawl WET files';
COMMENT ON TABLE abr_entities IS 'Australian Business Register bulk extract entities';
COMMENT ON TABLE entity_match_results IS 'Results of entity matching between CC and ABR';
COMMENT ON TABLE unified_companies IS 'Unified golden record of matched companies';

-- ============================================
-- Roles and Permissions
-- ============================================

-- Create roles (run as superuser)
DO $$
BEGIN
    -- ETL Pipeline role (read/write access)
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'etl_pipeline') THEN
        CREATE ROLE etl_pipeline WITH LOGIN PASSWORD 'etl_secure_password';
    END IF;
    
    -- Analyst role (read-only access)
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'data_analyst') THEN
        CREATE ROLE data_analyst WITH LOGIN PASSWORD 'analyst_password';
    END IF;
    
    -- Admin role (full access)
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'data_admin') THEN
        CREATE ROLE data_admin WITH LOGIN PASSWORD 'admin_secure_password';
    END IF;
END
$$;

-- Grant permissions to ETL Pipeline role
GRANT CONNECT ON DATABASE companydb TO etl_pipeline;
GRANT USAGE ON SCHEMA public TO etl_pipeline;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl_pipeline;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO etl_pipeline;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_pipeline;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO etl_pipeline;

-- Grant permissions to Analyst role (read-only)
GRANT CONNECT ON DATABASE companydb TO data_analyst;
GRANT USAGE ON SCHEMA public TO data_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO data_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO data_analyst;

-- Grant permissions to Admin role (full access)
GRANT CONNECT ON DATABASE companydb TO data_admin;
GRANT ALL PRIVILEGES ON SCHEMA public TO data_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO data_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO data_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO data_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO data_admin;

-- ============================================
-- Row-Level Security (Optional)
-- ============================================

-- Enable RLS on sensitive tables
ALTER TABLE entity_match_results ENABLE ROW LEVEL SECURITY;

-- Drop existing policies before creating (idempotent)
DROP POLICY IF EXISTS analyst_view_policy ON entity_match_results;
DROP POLICY IF EXISTS etl_full_access ON entity_match_results;

-- Policy: Analysts can only see high-confidence matches
CREATE POLICY analyst_view_policy ON entity_match_results
    FOR SELECT
    TO data_analyst
    USING (final_score >= 0.75);

-- Policy: ETL pipeline has full access
CREATE POLICY etl_full_access ON entity_match_results
    FOR ALL
    TO etl_pipeline
    USING (true)
    WITH CHECK (true);

-- ============================================
-- Audit Logging
-- ============================================

-- Create audit log table
CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,  -- INSERT, UPDATE, DELETE
    old_data JSONB,
    new_data JSONB,
    changed_by TEXT DEFAULT current_user,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Audit trigger function
CREATE OR REPLACE FUNCTION audit_trigger_func()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit_log (table_name, operation, new_data)
        VALUES (TG_TABLE_NAME, TG_OP, to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_log (table_name, operation, old_data, new_data)
        VALUES (TG_TABLE_NAME, TG_OP, to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit_log (table_name, operation, old_data)
        VALUES (TG_TABLE_NAME, TG_OP, to_jsonb(OLD));
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Apply audit trigger to unified_companies (for compliance)
DROP TRIGGER IF EXISTS audit_unified_companies ON unified_companies;
CREATE TRIGGER audit_unified_companies
    AFTER INSERT OR UPDATE OR DELETE ON unified_companies
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_func();

