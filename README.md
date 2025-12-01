# 🇦🇺 Australia Company ETL Pipeline

> **Entity Resolution Pipeline**: Common Crawl + Australian Business Register (ABR) + LLM-Enhanced Matching

## 📋 Overview

### Problem Statement

This pipeline solves the challenge of matching company information from:
- **Common Crawl** (March 2025 Index) - Extracting ~200,000 Australian company websites
- **Australian Business Register (ABR)** - Official government registry with ~3M businesses

### What This Pipeline Does

| Step | Description | Output |
|------|-------------|--------|
| **1. Extract** | Download and parse WET files from Common Crawl; Parse ABR XML bulk extracts | Raw company data |
| **2. Transform** | Clean names, normalize text, standardize addresses | Cleaned datasets |
| **3. Match** | Fuzzy matching + LLM verification for entity resolution | Match pairs with scores |
| **4. Load** | Upsert to PostgreSQL with unified golden records | Production database |

### Key Features

✅ **Hybrid Entity Matching** - Combines fuzzy string matching with LLM semantic understanding  
✅ **Blocking Strategy** - Reduces comparison space from billions to thousands  
✅ **ABN Validation** - Validates Australian Business Numbers using official checksum  
✅ **Scalable** - Supports PySpark for distributed processing  
✅ **Docker Ready** - One-command deployment with PostgreSQL  

---

## 🚀 Quick Start

### Large-Scale Processing (TB-scale CommonCrawl)

For processing large datasets (100K+ records):

```bash
# Install dependencies
pip install pyspark rapidfuzz

# Large dataset with specific number of workers
python src/pipeline.py --workers 16 --max-records 1000000

# Use existing downloaded files (skip download)
python src/pipeline.py --skip-download --workers 8
```

**CLI Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `--workers` | Number of parallel workers | Auto-detected |
| `--max-records` | Maximum records to process | All |
| `--skip-download` | Use existing downloaded files | `False` |
| `--skip-load` | Skip loading to database | `False` |
| `--llm` | Enable LLM-based matching | `False` |
| `--config` | Path to configuration file | `config/pipeline_config.yaml` |

### Option 4: Using Make (Linux/Mac)

```bash
make install      # Install dependencies
make run          # Run pipeline
make test         # Run tests
make docker-up    # Start Docker services
```

---

## 🏭 Production Deployment

### Prerequisites

Before running in production, ensure you have:

- **Python 3.10+** installed
- **Docker & Docker Compose** installed
- **OpenAI API Key** (for LLM matching)
- **50GB+ disk space** for data files
- **16GB+ RAM** recommended for large datasets

### Step 1: Environment Setup

```bash
# Clone repository
git clone https://github.com/medhaparwe/australia-company-etl-pipeline
cd australia-company-etl-pipeline

# Create and activate virtual environment
python -m venv venv

# Linux/Mac
source venv/bin/activate

# Install all dependencies
pip install -r requirements.txt

# Create data directories
mkdir -p data/raw/commoncrawl data/raw/abr data/processed data/output logs
```

### Step 3: Start Database

```bash
# Start PostgreSQL with Docker
docker-compose up -d postgres

# Wait for database to be ready (check logs)
docker-compose logs -f postgres

# Verify connection
docker-compose exec postgres psql -U postgres -d companydb -c "SELECT version();"

# Create tables and indexes
docker-compose exec postgres psql -U postgres -d companydb -f /docker-entrypoint-initdb.d/init.sql
```

### Step 4: Run Full Production Pipeline

```bash
# Run full ETL pipeline with all data
python src/pipeline.py  --config config/pipeline_config.yaml --max-records 200000 --llm

# Monitor progress in real-time
tail -f logs/pipeline.log
```

**Expected Production Output:**
```
2025-11-29 10:00:00 - pipeline - INFO - Starting pipeline run: prod-abc123
==================================================
STEP 1: EXTRACT
==================================================
2025-11-29 10:00:05 - INFO - Parsing 50 WET files...
2025-11-29 10:15:00 - INFO - Extracted 200,000 Common Crawl records
2025-11-29 10:15:30 - INFO - Parsing ABR XML bulk extract...
2025-11-29 10:25:00 - INFO - Extracted 3,000,000 ABR records
==================================================
STEP 2: TRANSFORM
==================================================
2025-11-29 10:25:05 - INFO - Cleaning Common Crawl data...
2025-11-29 10:30:00 - INFO - Cleaned CC data: 185,000 records
2025-11-29 10:30:05 - INFO - Cleaning ABR data...
2025-11-29 10:40:00 - INFO - Cleaned ABR data: 2,850,000 records
==================================================
STEP 3: ENTITY MATCHING
==================================================
2025-11-29 10:40:05 - INFO - Generating blocking keys...
2025-11-29 10:42:00 - INFO - Matching 185,000 CC with 2,850,000 ABR records
2025-11-29 11:30:00 - INFO - Fuzzy matching complete: 160,000 candidates
2025-11-29 11:30:05 - INFO - LLM verification for 8,000 edge cases...
2025-11-29 11:45:00 - INFO - LLM verification complete
2025-11-29 11:45:01 - INFO - Found 152,000 matches
2025-11-29 11:45:01 - INFO - Average match score: 86.50%
==================================================
STEP 4: LOAD
==================================================
2025-11-29 11:45:05 - INFO - Loading to PostgreSQL...
2025-11-29 11:55:00 - INFO - Loaded 185,000 web_companies
2025-11-29 12:15:00 - INFO - Loaded 2,850,000 abr_entities
2025-11-29 12:20:00 - INFO - Loaded 152,000 match_results
2025-11-29 12:25:00 - INFO - Created 152,000 unified_companies
==================================================
PIPELINE COMPLETED SUCCESSFULLY
==================================================
Duration: 2 hours 25 minutes
Total matches: 152,000
Match rate: 82.2%
High confidence matches: 128,000 (84.2%)
LLM verified: 8,000 (5.3%)
==================================================
```

### Step 5: Run dbt Transformations

```bash
# Navigate to dbt directory
cd dbt

# Install dbt dependencies
dbt deps

# Test database connection
dbt debug

# Run all dbt models
dbt run

# Run data quality tests
dbt test

# Generate and view documentation
dbt docs generate
dbt docs serve --port 8080
```

**Expected dbt Output:**
```
Running with dbt=1.7.0
Found 5 models, 12 tests, 3 sources

Concurrency: 4 threads (target='prod')

1 of 5 START sql view model staging.stg_web_companies .................. [RUN]
2 of 5 START sql view model staging.stg_abr_entities ................... [RUN]
1 of 5 OK created sql view model staging.stg_web_companies ............. [OK in 2.34s]
2 of 5 OK created sql view model staging.stg_abr_entities .............. [OK in 2.56s]
3 of 5 START sql table model intermediate.int_matched_companies ........ [RUN]
3 of 5 OK created sql table model intermediate.int_matched_companies ... [OK in 45.23s]
4 of 5 START sql table model marts.dim_companies ....................... [RUN]
5 of 5 START sql table model marts.fct_match_statistics ................ [RUN]
4 of 5 OK created sql table model marts.dim_companies .................. [OK in 32.45s]
5 of 5 OK created sql table model marts.fct_match_statistics ........... [OK in 5.67s]

Completed successfully

Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5
```

### Step 6: Verify Results

```bash
# Connect to database
docker-compose exec postgres psql -U postgres -d companydb

# Check record counts
SELECT 'web_companies' as table_name, COUNT(*) as count FROM web_companies
UNION ALL
SELECT 'abr_entities', COUNT(*) FROM abr_entities
UNION ALL
SELECT 'entity_match_results', COUNT(*) FROM entity_match_results
UNION ALL
SELECT 'unified_companies', COUNT(*) FROM unified_companies;

# View match statistics
SELECT * FROM fct_match_statistics;

# View sample matched companies
SELECT 
    abn,
    canonical_name,
    trading_name,
    website_url,
    state,
    confidence_score,
    data_source
FROM dim_companies 
WHERE data_source = 'MERGED'
ORDER BY confidence_score DESC
LIMIT 20;

# Check match distribution by state
SELECT 
    state, 
    COUNT(*) as match_count,
    ROUND(AVG(confidence_score)::numeric, 3) as avg_score
FROM dim_companies
WHERE state IS NOT NULL
GROUP BY state
ORDER BY match_count DESC;

# Exit psql
\q
```

### Step 7: Export Results

```bash
# Export unified companies to CSV
docker-compose exec postgres psql -U postgres -d companydb -c \
    "COPY (SELECT * FROM dim_companies) TO STDOUT WITH CSV HEADER" \
    > data/output/unified_companies.csv

# Export match statistics to JSON
docker-compose exec postgres psql -U postgres -d companydb -c \
    "SELECT row_to_json(t) FROM fct_match_statistics t" \
    > data/output/match_statistics.json

# Generate Parquet files for analytics
python -c "
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres123@localhost:5432/companydb')

# Export to Parquet
pd.read_sql('SELECT * FROM dim_companies', engine).to_parquet('data/output/dim_companies.parquet')
pd.read_sql('SELECT * FROM entity_match_results', engine).to_parquet('data/output/match_results.parquet')
print('Exported to Parquet files')
"
```

### Production Commands Summary

```bash
# Complete production workflow in one script
#!/bin/bash
set -e

echo "=== PRODUCTION PIPELINE ==="

# 1. Setup
source venv/bin/activate
source .env

# 2. Start database
docker-compose up -d postgres
sleep 10

# 3. Run ETL pipeline
python src/pipeline.py --max-records 200000 --llm

# 4. Run dbt transformations
cd dbt && dbt run && dbt test && cd ..

# 5. Generate statistics
docker-compose exec postgres psql -U postgres -d companydb -c \
    "SELECT * FROM fct_match_statistics;"

# 6. Export results
python -c "
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:postgres123@localhost:5432/companydb')
df = pd.read_sql('SELECT * FROM dim_companies', engine)
df.to_csv('data/output/unified_companies.csv', index=False)
print(f'Exported {len(df)} companies')
"

echo "=== PIPELINE COMPLETE ==="
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                   │
├───────────────────────────────────┬─────────────────────────────────────┤
│        Common Crawl (WET)         │        ABR XML Bulk Extract         │
│     ~200k Australian Websites     │      ~3M Business Registrations     │
│  https://commoncrawl.org/         │      https://data.gov.au/           │
└─────────────────┬─────────────────┴─────────────────┬───────────────────┘
                  │                                    │
                  ▼                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         EXTRACTION LAYER                                 │
│  ┌───────────────────────────┐      ┌───────────────────────────────┐   │
│  │   parse_commoncrawl.py    │      │        parse_abr.py           │   │
│  │   • Stream WET.gz files   │      │   • Parse XML with iterparse  │   │
│  │   • Filter .au domains    │      │   • Extract ABN, Name, State  │   │
│  │   • Extract URL + Text    │      │   • Handle large files        │   │
│  └───────────────────────────┘      └───────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                  │                                    │
                  ▼                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       TRANSFORMATION LAYER                               │
│  ┌───────────────────────────┐      ┌───────────────────────────────┐   │
│  │   clean_commoncrawl.py    │      │        clean_abr.py           │   │
│  │   • Normalize names       │      │   • Validate ABN checksum     │   │
│  │   • Remove PTY/LTD/etc    │      │   • Standardize state codes   │   │
│  │   • Extract domain        │      │   • Handle null values        │   │
│  │   • Generate block_key    │      │   • Generate block_key        │   │
│  └───────────────────────────┘      └───────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                  │                                    │
                  └────────────────┬───────────────────┘
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       ENTITY MATCHING LAYER                              │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                      entity_match.py                               │  │
│  │                                                                    │  │
│  │   Step 1: BLOCKING                                                 │  │
│  │   └── Group by first 4 chars of normalized name                   │  │
│  │   └── Reduces: 200K × 3M → ~10K pairs per block                   │  │
│  │                                                                    │  │
│  │   Step 2: FUZZY MATCHING                                           │  │
│  │   └── RapidFuzz token_sort_ratio                                  │  │
│  │   └── Score range: 0.0 - 1.0                                       │  │
│  │                                                                    │  │
│  │   Step 3: LLM VERIFICATION (Optional, for scores 0.6-0.85)        │  │
│  │   └── GPT-4o-mini semantic comparison                              │  │
│  │   └── Returns: {"match": true, "score": 0.92, "reason": "..."}    │  │
│  │                                                                    │  │
│  │   FINAL SCORE = 0.7 × fuzzy_score + 0.3 × llm_score               │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            LOAD LAYER                                    │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                     load_postgres.py                               │  │
│  │   • Create tables with proper indexes                              │  │
│  │   • Batch insert with ON CONFLICT upsert                          │  │
│  │   • Generate unified_companies golden records                      │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           PostgreSQL                                     │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────────┐ │
│  │ web_companies  │  │  abr_entities  │  │  entity_match_results      │ │
│  │ (from CC)      │  │  (from ABR)    │  │  (match pairs + scores)    │ │
│  └────────────────┘  └────────────────┘  └────────────────────────────┘ │
│                              │                                           │
│                              ▼                                           │
│                    ┌─────────────────────┐                              │
│                    │  unified_companies  │  ← Golden Record             │
│                    │  (merged records)   │                              │
│                    └─────────────────────┘                              │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
australia-company-etl/
│
├── 📄 README.md                  # This documentation
├── 📄 requirements.txt           # Python dependencies
├── 📄 docker-compose.yml         # Docker services (PostgreSQL, pgAdmin)
├── 📄 Dockerfile                 # Container build instructions
├── 📄 .gitignore                 # Git ignore patterns
│
├── 📁 config/
│   ├── pipeline_config.yaml      # Main configuration
│   └── logging.conf              # Logging settings
│
├── 📁 src/
│   ├── __init__.py
│   ├── pipeline.py               # 🎯 Main orchestrator
│   │
│   ├── 📁 common/                # Shared utilities
│   │   ├── __init__.py
│   │   ├── utils.py              # Text normalization, ABN validation
│   │   ├── spark_session.py      # PySpark session management
│   │   └── llm_matcher.py        # OpenAI GPT integration
│   │
│   ├── 📁 ingest/                # Data extraction
│   │   ├── __init__.py
│   │   ├── download_commoncrawl.py   # Download WET files
│   │   ├── parse_commoncrawl.py      # Parse WET to extract companies
│   │   ├── download_abr.py           # Download ABR XML
│   │   └── parse_abr.py              # Parse ABR XML
│   │
│   ├── 📁 transform/             # Data transformation
│   │   ├── __init__.py
│   │   ├── clean_commoncrawl.py      # Clean CC data
│   │   ├── clean_abr.py              # Clean ABR data
│   │   ├── entity_match.py           # 🎯 Entity matching logic
│   │   └── feature_engineering.py    # Match features
│   │
│   └── 📁 load/                  # Database loading
│       ├── __init__.py
│       ├── create_tables.sql         # PostgreSQL schema
│       ├── load_postgres.py          # Database loader
│       └── upsert_logic.py           # Upsert operations
│
├── 📁 tests/                     # Unit tests
│   ├── __init__.py
│   ├── conftest.py               # Pytest fixtures
│   ├── test_parsing.py           # Parsing tests
│   └── test_matching.py          # Matching tests
│
├── 📁 dbt/                       # dbt transformations
│   ├── dbt_project.yml           # dbt configuration
│   ├── profiles.yml              # Connection profiles
│   ├── 📁 models/
│   │   ├── staging/              # stg_web_companies, stg_abr_entities
│   │   ├── intermediate/         # int_matched_companies
│   │   └── marts/                # dim_companies, fct_match_statistics
│   └── 📁 tests/                 # Data quality tests
│
└── 📁 data/                      # Data directories (gitignored)
    ├── raw/                      # Downloaded files
    ├── processed/                # Cleaned data
    └── output/                   # Final results
```

---

## 📊 Database Schema

### Tables Overview

| Table | Description | Primary Key |
|-------|-------------|-------------|
| `web_companies` | Common Crawl extracted data | `id` (auto) |
| `abr_entities` | ABR registry data | `abn` |
| `entity_match_results` | Match pairs with scores | `id` (auto) |
| `unified_companies` | Golden merged records | `abn` |

### `web_companies` (from Common Crawl)

```sql
CREATE TABLE web_companies (
    id              BIGSERIAL PRIMARY KEY,
    url             TEXT NOT NULL,
    domain          TEXT,
    company_name    TEXT,
    normalized_name TEXT,
    industry        TEXT,
    raw_text        TEXT,
    block_key       VARCHAR(10),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### `abr_entities` (from ABR XML)

```sql
CREATE TABLE abr_entities (
    abn             VARCHAR(11) PRIMARY KEY,
    entity_name     TEXT NOT NULL,
    normalized_name TEXT,
    entity_type     VARCHAR(50),    -- PRV, PUB, IND, TRT
    entity_status   VARCHAR(20),    -- Active, Cancelled
    state           VARCHAR(10),    -- NSW, VIC, QLD, etc.
    postcode        VARCHAR(10),
    start_date      DATE,
    block_key       VARCHAR(10)
);
```

### `entity_match_results` (Matching Output)

```sql
CREATE TABLE entity_match_results (
    id              BIGSERIAL PRIMARY KEY,
    crawl_name      TEXT,
    crawl_url       TEXT,
    abr_name        TEXT,
    abn             VARCHAR(11) REFERENCES abr_entities(abn),
    fuzzy_score     DECIMAL(5,4),   -- 0.0000 - 1.0000
    llm_score       DECIMAL(5,4),
    final_score     DECIMAL(5,4),
    match_method    VARCHAR(20),    -- 'fuzzy', 'llm', 'hybrid'
    UNIQUE(crawl_url, abn)
);
```

### `unified_companies` (Golden Record)

```sql
CREATE TABLE unified_companies (
    abn              VARCHAR(11) PRIMARY KEY,
    canonical_name   TEXT NOT NULL,
    trading_name     TEXT,
    url              TEXT,
    domain           TEXT,
    industry         TEXT,
    entity_type      VARCHAR(50),
    entity_status    VARCHAR(20),
    state            VARCHAR(10),
    postcode         VARCHAR(10),
    start_date       DATE,
    source           VARCHAR(20),    -- 'ABR', 'CC', 'MERGED'
    confidence_score DECIMAL(5,4)
);
```

---

## 🔧 Entity Matching Algorithm

### Three-Stage Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 1: BLOCKING                                               │
│                                                                 │
│   Purpose: Reduce comparison space                              │
│   Method:  Group records by first 4 chars of normalized name   │
│   Result:  200K × 3M → ~10K pairs per block                    │
│                                                                 │
│   Example:                                                      │
│   "ACME Corp" → block_key = "acme"                             │
│   "ACME Holdings Pty Ltd" → block_key = "acme"                 │
│   → These will be compared                                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 2: FUZZY MATCHING                                         │
│                                                                 │
│   Algorithm: RapidFuzz token_sort_ratio                        │
│   Threshold: 0.75 minimum for match                            │
│                                                                 │
│   Example:                                                      │
│   "ACME CORP" vs "ACME CORPORATION PTY LTD"                    │
│   → Fuzzy Score: 0.82 ✓                                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 3: LLM VERIFICATION (Optional)                            │
│                                                                 │
│   Trigger: Fuzzy score between 0.60 - 0.85                     │
│   Model:   GPT-4o-mini                                          │
│   Cost:    ~$0.001 per comparison                              │
│                                                                 │
│   Prompt:                                                       │
│   "Are these the same company?                                  │
│    1. ACME Digital Services                                     │
│    2. ACME DIGITAL SERVICES PTY LTD                            │
│    Return: {match: true/false, score: 0-1, reason: '...'}"    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ FINAL SCORE CALCULATION                                         │
│                                                                 │
│   Formula: final_score = 0.7 × fuzzy + 0.3 × llm               │
│   Decision: Match if final_score ≥ 0.75                        │
└─────────────────────────────────────────────────────────────────┘


## 🙏 Acknowledgments

- [Common Crawl](https://commoncrawl.org/) - Web archive data
- [data.gov.au](https://data.gov.au/) - ABR bulk data
- [RapidFuzz](https://github.com/maxbachmann/RapidFuzz) - Fast fuzzy matching
- [OpenAI](https://openai.com/) - GPT models for semantic matching
