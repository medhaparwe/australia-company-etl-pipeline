# 🇦🇺 Australia Company ETL Pipeline

Entity resolution pipeline matching **Common Crawl** web data with **Australian Business Register (ABR)** using fuzzy matching + LLM verification.

---

## 📊 Dataset Statistics

| Metric | Value |
|--------|-------|
| **Common Crawl Records** | ~200,000 Australian websites |
| **ABR Entities** | ~3,000,000 business registrations |
| **Match Rate** | ~82% |
| **High Confidence Matches** | ~84% of matches (score ≥ 0.85) |
| **LLM Verified** | ~5% (edge cases between 0.60-0.85) |

---

## 🏗️ Architecture

```
┌──────────────────────────┐     ┌──────────────────────────┐
│     Common Crawl WET     │     │      ABR XML Bulk        │
│   (~200K .au websites)   │     │    (~3M businesses)      │
└───────────┬──────────────┘     └───────────┬──────────────┘
            │                                 │
            ▼                                 ▼
┌─────────────────────────────────────────────────────────────┐
│  EXTRACT: parse_commoncrawl.py  │  parse_abr.py            │
├─────────────────────────────────────────────────────────────┤
│  TRANSFORM: clean_commoncrawl.py │ clean_abr.py            │
│            (normalize, block_key generation)               │
├─────────────────────────────────────────────────────────────┤
│  MATCH: entity_match.py                                    │
│  ├─ Blocking (first 4 chars) → reduces 600B to ~10K pairs  │
│  ├─ Fuzzy (RapidFuzz token_sort_ratio)                     │
│  └─ LLM (GPT-4o-mini for uncertain matches)                │
├─────────────────────────────────────────────────────────────┤
│  LOAD: load_postgres.py → PostgreSQL                       │
├─────────────────────────────────────────────────────────────┤
│  TRANSFORM (dbt): staging → intermediate → marts           │
└─────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL: web_companies │ abr_entities │ unified_companies│
└─────────────────────────────────────────────────────────────┘
```

---

## 🗄️ Database Schema (DDL)

```sql
-- Source: Common Crawl
CREATE TABLE web_companies (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    domain TEXT,
    company_name TEXT,
    normalized_name TEXT,
    block_key VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Source: ABR
CREATE TABLE abr_entities (
    abn VARCHAR(11) PRIMARY KEY,
    entity_name TEXT NOT NULL,
    normalized_name TEXT,
    entity_type VARCHAR(500),
    entity_status VARCHAR(20),
    state VARCHAR(10),
    postcode VARCHAR(10),
    block_key VARCHAR(10)
);

-- Matching Results
CREATE TABLE entity_match_results (
    id BIGSERIAL PRIMARY KEY,
    crawl_url TEXT,
    abn VARCHAR(11) REFERENCES abr_entities(abn),
    fuzzy_score DECIMAL(5,4),
    llm_score DECIMAL(5,4),
    final_score DECIMAL(5,4),
    match_method VARCHAR(20),  -- 'fuzzy', 'llm', 'hybrid'
    UNIQUE(crawl_url, abn)
);

-- Golden Record
CREATE TABLE unified_companies (
    abn VARCHAR(11) PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    url TEXT,
    state VARCHAR(10),
    source VARCHAR(20),  -- 'ABR', 'CC', 'MERGED'
    confidence_score DECIMAL(5,4)
);
```

Full DDL: [`src/load/create_tables.sql`](src/load/create_tables.sql)

---

## ⚙️ Technology Stack

| Component | Technology | Justification |
|-----------|------------|---------------|
| **Processing** | PySpark 3.5 | Distributed processing for TB-scale data |
| **Fuzzy Matching** | RapidFuzz | 10x faster than python-Levenshtein |
| **LLM** | OpenAI GPT-4o-mini | Cost-effective ($0.001/match), semantic understanding |
| **Database** | PostgreSQL 14 | ACID compliance, pg_trgm for fuzzy search |
| **Transformations** | dbt 1.7 | Version-controlled SQL, data quality tests |
| **Containerization** | Docker | Reproducible deployments |

---

## 🤖 AI Model: GPT-4o-mini

**Why GPT-4o-mini?**
- **Cost**: ~$0.15/1M input tokens (vs $30 for GPT-4)
- **Speed**: 3x faster than GPT-4
- **Accuracy**: Sufficient for entity resolution (not complex reasoning)
- **Use Case**: Only called for uncertain matches (fuzzy score 0.60-0.85)

**Matching Formula**: `final_score = 0.7 × fuzzy + 0.3 × llm`

---

## 🚀 Setup & Run

```bash
# 1. Clone & setup
git clone https://github.com/medhaparwe/australia-company-etl-pipeline
cd australia-company-etl-pipeline
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Configure
update config/pipeline_config.yaml
mkdir -p data/{raw,processed,output}

# 3. Start Docker containers
docker-compose up -d postgres spark-master spark-worker pgadmin

# 4. Run pipeline
python src/pipeline.py --max-records 200000 --llm --workers 8

# 5. Run dbt transformations
cd dbt && dbt run && dbt test
```

**CLI Options:**
| Flag | Description |
|------|-------------|
| `--workers N` | Parallel workers (default: auto) |
| `--max-records N` | Limit records processed |
| `--llm` | Enable LLM matching |
| `--skip-download` | Use existing files |
| `--skip-load` | Skip loading to database |
| `--config` | Add custom config file (default: config/pipeline_config.yaml) |
---

## 📂 Code Structure

```
src/
├── pipeline.py              # Main orchestrator
├── ingest/
│   ├── parse_commoncrawl.py # Extract .au domains from WET files
│   └── parse_abr.py         # Parse ABR XML bulk extract
├── transform/
│   ├── clean_*.py           # Normalize, generate block_key
│   └── entity_match.py      # Fuzzy + LLM matching
├── common/
│   └── llm_matcher.py       # OpenAI GPT integration
└── load/
    ├── create_tables.sql    # PostgreSQL DDL
    └── load_postgres.py     # Batch upserts

dbt/models/
├── staging/                 # stg_web_companies, stg_abr_entities
├── intermediate/            # int_matched_companies
└── marts/                   # dim_companies, fct_match_statistics

dbt/tests/
├── assert_minimum_match_rate.sql   # Match rate ≥ 20%
├── assert_no_duplicate_abns.sql    # ABN uniqueness
└── assert_valid_confidence_scores.sql  # Score 0-1 range
```

---

## 🔧 Entity Matching Approach

**3-Stage Pipeline:**

1. **Blocking**: Group by first 4 chars of normalized name  
   → Reduces 200K × 3M = 600B comparisons to ~10K pairs/block

2. **Fuzzy Matching**: RapidFuzz `token_sort_ratio`  
   → Score ≥ 0.85: Direct match  
   → Score 0.60-0.85: Send to LLM  
   → Score < 0.60: No match

3. **LLM Verification**: GPT-4o-mini for semantic comparison  
   → Handles abbreviations, trading vs legal names, typos

**Why Blocking?** Without it: 600 billion comparisons. With it: <1M comparisons.

---

## 🖥️ Development Environment

- **IDE**: Cursor with Python extensions
- **Python**: 3.10+
- **OS**: macOS / Linux

---
