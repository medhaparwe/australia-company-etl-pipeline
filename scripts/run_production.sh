#!/bin/bash
# =============================================================================
# Australia Company ETL Pipeline - Production Run Script
# =============================================================================
# This script runs the complete ETL pipeline in production mode
# =============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   Australia Company ETL Pipeline${NC}"
echo -e "${BLUE}   Production Deployment${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# =============================================================================
# Configuration
# =============================================================================
MAX_RECORDS=${MAX_RECORDS:-200000}
ENABLE_LLM=${ENABLE_LLM:-true}
FUZZY_THRESHOLD=${FUZZY_THRESHOLD:-0.75}
CC_MAX_FILES=${CC_MAX_FILES:-50}

# =============================================================================
# Step 1: Environment Check
# =============================================================================
echo -e "${YELLOW}Step 1: Checking environment...${NC}"

# Check Python
if ! command -v python &> /dev/null; then
    echo -e "${RED}Error: Python not found${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Python found: $(python --version)${NC}"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker not found${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker found: $(docker --version)${NC}"

# Check virtual environment
if [[ -z "${VIRTUAL_ENV}" ]]; then
    echo -e "${YELLOW}Warning: Virtual environment not activated${NC}"
    if [[ -d "venv" ]]; then
        echo "Activating virtual environment..."
        source venv/bin/activate
    fi
fi

# Load environment variables
if [[ -f ".env" ]]; then
    echo "Loading .env file..."
    export $(cat .env | grep -v '^#' | xargs)
fi

# Check OpenAI API key
if [[ -z "${OPENAI_API_KEY}" ]] && [[ "${ENABLE_LLM}" == "true" ]]; then
    echo -e "${YELLOW}Warning: OPENAI_API_KEY not set. LLM matching will be disabled.${NC}"
    ENABLE_LLM=false
fi

echo ""

# =============================================================================
# Step 2: Create Directories
# =============================================================================
echo -e "${YELLOW}Step 2: Creating directories...${NC}"
mkdir -p data/raw/commoncrawl
mkdir -p data/raw/abr
mkdir -p data/processed
mkdir -p data/output
mkdir -p logs
echo -e "${GREEN}✓ Directories created${NC}"
echo ""

# =============================================================================
# Step 3: Start Database
# =============================================================================
echo -e "${YELLOW}Step 3: Starting PostgreSQL...${NC}"

# Check if already running
if docker-compose ps postgres | grep -q "Up"; then
    echo -e "${GREEN}✓ PostgreSQL already running${NC}"
else
    docker-compose up -d postgres
    echo "Waiting for PostgreSQL to be ready..."
    sleep 10
    
    # Wait for database to be ready
    until docker-compose exec -T postgres pg_isready -U postgres; do
        echo "Waiting for PostgreSQL..."
        sleep 2
    done
    echo -e "${GREEN}✓ PostgreSQL started${NC}"
fi
echo ""

# =============================================================================
# Step 4: Download Data
# =============================================================================
echo -e "${YELLOW}Step 4: Downloading data...${NC}"

# Check if data already exists
CC_COUNT=$(find data/raw/commoncrawl -name "*.wet.gz" 2>/dev/null | wc -l)
ABR_COUNT=$(find data/raw/abr -name "*.xml" 2>/dev/null | wc -l)

if [[ $CC_COUNT -gt 0 ]]; then
    echo -e "${GREEN}✓ Found $CC_COUNT Common Crawl files${NC}"
else
    echo "Downloading Common Crawl data..."
    python -c "
from src.ingest.download_commoncrawl import download_wet_files
files = download_wet_files(
    crawl_id='CC-MAIN-2025-10',
    output_dir='data/raw/commoncrawl',
    max_files=${CC_MAX_FILES},
    partial=False
)
print(f'Downloaded {len(files)} WET files')
"
fi

if [[ $ABR_COUNT -gt 0 ]]; then
    echo -e "${GREEN}✓ Found $ABR_COUNT ABR files${NC}"
else
    echo "Creating sample ABR data..."
    python -c "
from src.ingest.download_abr import create_sample_abr_data
create_sample_abr_data('data/raw/abr/abr_bulk.xml', num_records=500000)
print('Created ABR data with 500,000 records')
"
fi
echo ""

# =============================================================================
# Step 5: Run ETL Pipeline
# =============================================================================
echo -e "${YELLOW}Step 5: Running ETL Pipeline...${NC}"
echo "Configuration:"
echo "  - Max Records: ${MAX_RECORDS}"
echo "  - LLM Enabled: ${ENABLE_LLM}"
echo "  - Fuzzy Threshold: ${FUZZY_THRESHOLD}"
echo ""

START_TIME=$(date +%s)

if [[ "${ENABLE_LLM}" == "true" ]]; then
    python src/pipeline.py --max-records ${MAX_RECORDS} --llm 2>&1 | tee logs/pipeline_$(date +%Y%m%d_%H%M%S).log
else
    python src/pipeline.py --max-records ${MAX_RECORDS} 2>&1 | tee logs/pipeline_$(date +%Y%m%d_%H%M%S).log
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo -e "${GREEN}✓ ETL Pipeline completed in ${DURATION} seconds${NC}"
echo ""

# =============================================================================
# Step 6: Run dbt Transformations
# =============================================================================
echo -e "${YELLOW}Step 6: Running dbt transformations...${NC}"
cd dbt

# Run dbt
dbt run --profiles-dir .
echo -e "${GREEN}✓ dbt models created${NC}"

# Run tests
echo "Running dbt tests..."
dbt test --profiles-dir . || echo -e "${YELLOW}Warning: Some dbt tests failed${NC}"

cd ..
echo ""

# =============================================================================
# Step 7: Generate Statistics
# =============================================================================
echo -e "${YELLOW}Step 7: Generating statistics...${NC}"

docker-compose exec -T postgres psql -U postgres -d companydb << 'EOF'
\echo '=== Record Counts ==='
SELECT 'web_companies' as table_name, COUNT(*) as count FROM web_companies
UNION ALL SELECT 'abr_entities', COUNT(*) FROM abr_entities
UNION ALL SELECT 'entity_match_results', COUNT(*) FROM entity_match_results
UNION ALL SELECT 'unified_companies', COUNT(*) FROM unified_companies;

\echo ''
\echo '=== Match Statistics ==='
SELECT 
    COUNT(*) as total_matches,
    ROUND(AVG(final_score)::numeric, 4) as avg_score,
    COUNT(CASE WHEN final_score >= 0.90 THEN 1 END) as high_confidence,
    COUNT(CASE WHEN match_method = 'hybrid' THEN 1 END) as llm_verified
FROM entity_match_results;

\echo ''
\echo '=== Matches by State ==='
SELECT 
    COALESCE(state, 'Unknown') as state,
    COUNT(*) as match_count
FROM entity_match_results
GROUP BY state
ORDER BY match_count DESC
LIMIT 10;
EOF

echo ""

# =============================================================================
# Step 8: Export Results
# =============================================================================
echo -e "${YELLOW}Step 8: Exporting results...${NC}"

# Export to CSV
docker-compose exec -T postgres psql -U postgres -d companydb -c \
    "COPY (SELECT * FROM unified_companies ORDER BY abn) TO STDOUT WITH CSV HEADER" \
    > data/output/unified_companies.csv

echo -e "${GREEN}✓ Exported to data/output/unified_companies.csv${NC}"

# Count exported records
EXPORT_COUNT=$(wc -l < data/output/unified_companies.csv)
echo "  Exported $((EXPORT_COUNT - 1)) records"
echo ""

# =============================================================================
# Summary
# =============================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   Pipeline Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Results:"
echo "  - Duration: ${DURATION} seconds"
echo "  - Exported: $((EXPORT_COUNT - 1)) unified companies"
echo "  - Output: data/output/unified_companies.csv"
echo ""
echo "Next steps:"
echo "  - View dbt docs: cd dbt && dbt docs serve"
echo "  - Query database: docker-compose exec postgres psql -U postgres -d companydb"
echo "  - View logs: cat logs/pipeline.log"
echo ""

