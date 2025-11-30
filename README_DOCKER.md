# 🐳 Docker Production Deployment Guide

> **Production-ready Docker deployment** for the Australia Company ETL Pipeline

This guide covers deploying and running the ETL pipeline in production using Docker and Docker Compose.

---

## 📋 Table of Contents

- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Production Setup](#-production-setup)
- [Configuration](#-configuration)
- [Running the Pipeline](#-running-the-pipeline)
- [Monitoring & Logging](#-monitoring--logging)
- [Data Persistence](#-data-persistence)
- [Scaling & Performance](#-scaling--performance)
- [Troubleshooting](#-troubleshooting)
- [Production Best Practices](#-production-best-practices)

---

## ✅ Prerequisites

### System Requirements

| Component | Requirement | Notes |
|-----------|-------------|-------|
| **Docker** | 20.10+ | [Install Docker](https://docs.docker.com/get-docker/) |
| **Docker Compose** | 2.0+ | Included with Docker Desktop |
| **Disk Space** | 100GB+ | For data files and database |
| **RAM** | 16GB+ | Recommended for large datasets |
| **CPU** | 4+ cores | For parallel processing |

### Required Credentials

- **OpenAI API Key** (for LLM matching) - Get from [OpenAI Platform](https://platform.openai.com/api-keys)
- **PostgreSQL Password** (set in environment variables)

---

## 🚀 Quick Start

### 1. Clone and Navigate

```bash
git clone https://github.com/yourusername/australia-company-etl.git
cd australia-company-etl
```

### 2. Set Environment Variables

```bash
# Create .env file
cat > .env << 'EOF'
# OpenAI API Key (required for LLM matching)
OPENAI_API_KEY=sk-your-openai-api-key-here

# Optional: Override default PostgreSQL password
POSTGRES_PASSWORD=your_secure_password_here
EOF
```

**Windows PowerShell:**
```powershell
# Create .env file
@"
OPENAI_API_KEY=sk-your-openai-api-key-here
POSTGRES_PASSWORD=your_secure_password_here
"@ | Out-File -FilePath .env -Encoding utf8
```

### 3. Start All Services

```bash
# Start PostgreSQL, Spark, and ETL pipeline
docker-compose up -d

# View logs
docker-compose logs -f
```

### 4. Verify Services

```bash
# Check all services are running
docker-compose ps

# Expected output:
# NAME                        STATUS
# company_etl_postgres        Up (healthy)
# company_etl_spark_master    Up (healthy)
# company_etl_spark_worker    Up
# company_etl_pipeline        Up
```

### 5. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Spark Web UI** | http://localhost:8080 | N/A |
| **pgAdmin** | http://localhost:8081 | admin@admin.com / admin123 |
| **PostgreSQL** | localhost:5432 | postgres / postgres123 |

---

## 🏭 Production Setup

### Step 1: Configure Environment Variables

Create a `.env` file in the project root:

```bash
# Database Configuration
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=companydb
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_secure_production_password

# OpenAI API (Required for LLM matching)
OPENAI_API_KEY=sk-your-production-api-key

# Spark Configuration
SPARK_MASTER_URL=spark://spark-master:7077

# Pipeline Configuration
MAX_RECORDS=200000
LLM_ENABLED=true
FUZZY_THRESHOLD=0.75
```

**Security Note:** Never commit `.env` files to version control. Add `.env` to `.gitignore`.

### Step 2: Prepare Data Directories

```bash
# Create data directories with proper permissions
mkdir -p data/raw/commoncrawl \
         data/raw/abr \
         data/processed \
         data/output \
         logs

# Set permissions (Linux/Mac)
chmod -R 755 data logs
```

### Step 3: Download Source Data

#### Download Common Crawl Data

```bash
# Option 1: Download via Docker container
docker-compose run --rm etl python -c "
from src.ingest.download_commoncrawl import download_wet_files, get_wet_paths

paths = get_wet_paths(crawl_id='CC-MAIN-2025-43', limit=50)
print(f'Found {len(paths)} WET files')

files = download_wet_files(
    crawl_id='CC-MAIN-2025-43',
    output_dir='data/raw/commoncrawl',
    max_files=50,
    partial=False
)
print(f'Downloaded {len(files)} files')
"

# Option 2: Download manually and place in data/raw/commoncrawl/
```

#### Download ABR Data

```bash
# ABR bulk extract must be downloaded manually from data.gov.au
# 1. Visit: https://data.gov.au/data/dataset/abn-bulk-extract
# 2. Download the latest XML bulk extract ZIP file
# 3. Extract XML files to: data/raw/abr/

# Verify files are present
docker-compose exec etl ls -lh data/raw/abr/
```

### Step 4: Start Infrastructure Services

```bash
# Start only database and Spark (not ETL pipeline yet)
docker-compose up -d postgres spark-master spark-worker

# Wait for services to be healthy
docker-compose ps

# Verify PostgreSQL is ready
docker-compose exec postgres psql -U postgres -d companydb -c "SELECT version();"

# Verify Spark is ready
curl http://localhost:8080
```

### Step 5: Initialize Database Schema

```bash
# Database schema is automatically created on first startup
# Verify tables exist
docker-compose exec postgres psql -U postgres -d companydb -c "\dt"

# Expected tables:
# - web_companies
# - abr_entities
# - entity_match_results
# - unified_companies
```

---

## ⚙️ Configuration

### Docker Compose Services

The `docker-compose.yml` defines four main services:

1. **postgres** - PostgreSQL 15 database
2. **spark-master** - Apache Spark master node
3. **spark-worker** - Apache Spark worker node
4. **etl** - ETL pipeline container

### Customizing Resources

Edit `docker-compose.yml` to adjust resources:

```yaml
services:
  spark-worker:
    environment:
      - SPARK_WORKER_MEMORY=8g  # Increase for larger datasets
      - SPARK_WORKER_CORES=4    # Increase for more parallelism

  etl:
    deploy:
      resources:
        limits:
          memory: 8G
          cpus: '4'
```

### Pipeline Configuration

Edit `config/pipeline_config.yaml` for production settings:

```yaml
# Increase for production
paths:
  commoncrawl:
    max_files: 100  # Process more files

# Adjust matching thresholds
matching:
  fuzzy:
    threshold: 0.75
  llm:
    enabled: true
    batch_size: 100  # Larger batches for production
```

---

## 🏃 Running the Pipeline

### Option 1: Run via Docker Compose (Recommended)

```bash
# Start ETL pipeline (runs automatically)
docker-compose up -d etl

# Follow logs in real-time
docker-compose logs -f etl

# Run with custom parameters
docker-compose run --rm etl python src/pipeline.py \
  --max-records 200000 \
  --llm \
  --workers 8
```

### Option 2: Run Pipeline Manually

```bash
# Execute pipeline in running container
docker-compose exec etl python src/pipeline.py \
  --config config/pipeline_config.yaml \
  --max-records 200000 \
  --llm

# Run with skip options
docker-compose exec etl python src/pipeline.py \
  --skip-download \
  --skip-load \
  --max-records 1000
```

### Option 3: One-Time Execution

```bash
# Run pipeline once and remove container
docker-compose run --rm etl python src/pipeline.py \
  --max-records 200000 \
  --llm
```

### CLI Options

| Option | Description | Example |
|--------|-------------|---------|
| `--max-records` | Limit records processed | `--max-records 100000` |
| `--workers` | Number of parallel workers | `--workers 8` |
| `--skip-download` | Use existing files | `--skip-download` |
| `--skip-load` | Skip database loading | `--skip-load` |
| `--llm` | Enable LLM matching | `--llm` |
| `--config` | Config file path | `--config config/pipeline_config.yaml` |

---

## 📊 Monitoring & Logging

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f etl
docker-compose logs -f postgres
docker-compose logs -f spark-master

# Last 100 lines
docker-compose logs --tail=100 etl

# Logs with timestamps
docker-compose logs -f --timestamps etl
```

### Monitor Spark

```bash
# Access Spark Web UI
# Open browser: http://localhost:8080

# View Spark application status
docker-compose exec spark-master spark-submit --version

# Check worker status
docker-compose exec spark-worker spark-class org.apache.spark.deploy.worker.Worker --help
```

### Monitor Database

```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U postgres -d companydb

# Check table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

# Check record counts
SELECT 
    'web_companies' as table_name, 
    COUNT(*) as count 
FROM web_companies
UNION ALL
SELECT 'abr_entities', COUNT(*) FROM abr_entities
UNION ALL
SELECT 'entity_match_results', COUNT(*) FROM entity_match_results
UNION ALL
SELECT 'unified_companies', COUNT(*) FROM unified_companies;

# Monitor active connections
SELECT count(*) FROM pg_stat_activity WHERE datname = 'companydb';

# Exit psql
\q
```

### Monitor Resource Usage

```bash
# Container resource usage
docker stats

# Specific container
docker stats company_etl_etl

# Disk usage
docker system df

# Volume sizes
docker volume ls
docker volume inspect task_postgres_data
```

---

## 💾 Data Persistence

### Volumes

Docker Compose creates persistent volumes:

- `postgres_data` - PostgreSQL database files
- `spark_warehouse` - Spark warehouse directory
- `./data` - Host-mounted data directory (raw, processed, output)

### Backup Database

```bash
# Create backup
docker-compose exec postgres pg_dump -U postgres companydb > backup_$(date +%Y%m%d).sql

# Restore from backup
docker-compose exec -T postgres psql -U postgres companydb < backup_20250101.sql

# Backup with compression
docker-compose exec postgres pg_dump -U postgres -Fc companydb > backup_$(date +%Y%m%d).dump

# Restore compressed backup
docker-compose exec -T postgres pg_restore -U postgres -d companydb < backup_20250101.dump
```

### Export Results

```bash
# Export to CSV
docker-compose exec postgres psql -U postgres -d companydb -c \
    "COPY (SELECT * FROM unified_companies) TO STDOUT WITH CSV HEADER" \
    > data/output/unified_companies.csv

# Export to JSON
docker-compose exec postgres psql -U postgres -d companydb -c \
    "SELECT json_agg(row_to_json(t)) FROM (SELECT * FROM unified_companies LIMIT 1000) t" \
    > data/output/unified_companies.json

# Export via Python in container
docker-compose exec etl python -c "
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres123@postgres:5432/companydb')
df = pd.read_sql('SELECT * FROM unified_companies', engine)
df.to_parquet('data/output/unified_companies.parquet', index=False)
print(f'Exported {len(df)} records')
"
```

---

## 📈 Scaling & Performance

### Increase Spark Resources

```yaml
# docker-compose.yml
services:
  spark-worker:
    environment:
      - SPARK_WORKER_MEMORY=16g  # Increase memory
      - SPARK_WORKER_CORES=8     # Increase CPU cores
    deploy:
      resources:
        limits:
          memory: 16G
          cpus: '8'
```

### Run Multiple Workers

```yaml
# docker-compose.yml
services:
  spark-worker-1:
    # ... existing config ...
    
  spark-worker-2:
    image: bitnami/spark:3.5
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      # ... same config as worker-1 ...
```

### Optimize PostgreSQL

```bash
# Connect to database
docker-compose exec postgres psql -U postgres -d companydb

# Increase work_mem for large queries
ALTER SYSTEM SET work_mem = '256MB';
ALTER SYSTEM SET shared_buffers = '4GB';
ALTER SYSTEM SET effective_cache_size = '12GB';

# Reload configuration
SELECT pg_reload_conf();

# Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_web_companies_block_key ON web_companies(block_key);
CREATE INDEX IF NOT EXISTS idx_abr_entities_block_key ON abr_entities(block_key);
CREATE INDEX IF NOT EXISTS idx_match_results_abn ON entity_match_results(abn);
```

### Parallel Processing

```bash
# Run with more workers
docker-compose run --rm etl python src/pipeline.py \
  --workers 16 \
  --max-records 200000
```

---

## 🔧 Troubleshooting

### Common Issues

#### Issue: Container fails to start

```bash
# Check logs
docker-compose logs etl

# Common causes:
# - Missing .env file
# - Invalid OPENAI_API_KEY
# - Port conflicts
```

**Solution:**
```bash
# Verify environment variables
docker-compose config

# Check port availability
netstat -an | grep 5432  # PostgreSQL
netstat -an | grep 8080  # Spark UI
```

#### Issue: Out of memory

```bash
# Check memory usage
docker stats

# Increase Docker memory limit
# Docker Desktop → Settings → Resources → Memory → Increase to 16GB+
```

**Solution:**
```yaml
# Reduce Spark memory in docker-compose.yml
services:
  spark-worker:
    environment:
      - SPARK_WORKER_MEMORY=4g  # Reduce if needed
```

#### Issue: Database connection refused

```bash
# Check PostgreSQL is running
docker-compose ps postgres

# Check health status
docker-compose exec postgres pg_isready -U postgres

# View PostgreSQL logs
docker-compose logs postgres
```

**Solution:**
```bash
# Restart PostgreSQL
docker-compose restart postgres

# Wait for health check
docker-compose ps postgres  # Should show "healthy"
```

#### Issue: Spark connection timeout

```bash
# Check Spark master is running
docker-compose ps spark-master

# Check Spark UI
curl http://localhost:8080

# View Spark logs
docker-compose logs spark-master
```

**Solution:**
```bash
# Restart Spark cluster
docker-compose restart spark-master spark-worker

# Verify connection
docker-compose exec etl python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('spark://spark-master:7077').getOrCreate()
print('Spark connected:', spark.sparkContext.appName)
"
```

#### Issue: LLM API errors

```bash
# Check API key is set
docker-compose exec etl env | grep OPENAI_API_KEY

# Test API key
docker-compose exec etl python -c "
import os
from openai import OpenAI
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
print('API key valid:', client.models.list() is not None)
"
```

**Solution:**
```bash
# Update .env file with correct API key
echo "OPENAI_API_KEY=sk-your-key" >> .env

# Restart ETL container
docker-compose restart etl
```

### Debug Mode

```bash
# Run with verbose logging
docker-compose run --rm etl python src/pipeline.py \
  --max-records 100 \
  --skip-load \
  2>&1 | tee debug.log

# Interactive shell in container
docker-compose exec etl /bin/bash

# Test individual components
docker-compose exec etl python -c "
from src.ingest.parse_abr import parse_abr_xml
# Test parsing...
"
```

### Clean Restart

```bash
# Stop all services
docker-compose down

# Remove volumes (WARNING: deletes data)
docker-compose down -v

# Rebuild containers
docker-compose build --no-cache

# Start fresh
docker-compose up -d
```

---

## 🎯 Production Best Practices

### 1. Security

```bash
# Use strong passwords
POSTGRES_PASSWORD=$(openssl rand -base64 32)

# Never commit .env files
echo ".env" >> .gitignore

# Use secrets management (Docker Swarm/Kubernetes)
# For Docker Compose, consider using Docker secrets
```

### 2. Resource Limits

```yaml
# docker-compose.yml
services:
  etl:
    deploy:
      resources:
        limits:
          memory: 8G
          cpus: '4'
        reservations:
          memory: 4G
          cpus: '2'
```

### 3. Health Checks

All services include health checks. Monitor with:

```bash
# Check health status
docker-compose ps

# Manual health check
docker inspect company_etl_postgres | grep -A 10 Health
```

### 4. Logging Strategy

```bash
# Configure log rotation
# Add to docker-compose.yml:
services:
  etl:
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
```

### 5. Backup Strategy

```bash
# Automated backup script
#!/bin/bash
# backup.sh

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="./backups"

mkdir -p $BACKUP_DIR

# Database backup
docker-compose exec -T postgres pg_dump -U postgres companydb \
    > $BACKUP_DIR/db_backup_$DATE.sql

# Compress
gzip $BACKUP_DIR/db_backup_$DATE.sql

# Keep only last 7 days
find $BACKUP_DIR -name "*.sql.gz" -mtime +7 -delete

echo "Backup completed: $BACKUP_DIR/db_backup_$DATE.sql.gz"
```

### 6. Monitoring

```bash
# Use monitoring tools
# - Prometheus + Grafana for metrics
# - ELK stack for log aggregation
# - Docker stats for resource monitoring
```

### 7. CI/CD Integration

```yaml
# Example GitHub Actions workflow
name: Deploy Pipeline
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Deploy
        run: |
          docker-compose up -d --build
```

---

## 📝 Production Checklist

Before deploying to production:

- [ ] Set strong PostgreSQL password in `.env`
- [ ] Configure OpenAI API key
- [ ] Allocate sufficient disk space (100GB+)
- [ ] Configure resource limits in `docker-compose.yml`
- [ ] Set up automated backups
- [ ] Configure log rotation
- [ ] Test with small dataset first (`--max-records 1000`)
- [ ] Verify all services are healthy
- [ ] Set up monitoring and alerting
- [ ] Document deployment process
- [ ] Create rollback plan

---

## 🚀 Quick Reference

### Essential Commands

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# View logs
docker-compose logs -f etl

# Restart service
docker-compose restart etl

# Rebuild container
docker-compose build etl

# Execute command in container
docker-compose exec etl python src/pipeline.py --help

# Access database
docker-compose exec postgres psql -U postgres -d companydb

# Check service status
docker-compose ps

# View resource usage
docker stats
```

### Service URLs

- **Spark Web UI**: http://localhost:8080
- **pgAdmin**: http://localhost:8081
- **PostgreSQL**: localhost:5432

### Important Files

- `docker-compose.yml` - Service definitions
- `Dockerfile` - ETL container image
- `.env` - Environment variables (create this)
- `config/pipeline_config.yaml` - Pipeline configuration

---

## 📚 Additional Resources

- [Main README](README.md) - Full project documentation
- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/)

---

## 🆘 Support

For issues or questions:

1. Check [Troubleshooting](#-troubleshooting) section
2. Review logs: `docker-compose logs -f`
3. Check service health: `docker-compose ps`
4. Consult [Main README](README.md) for detailed documentation

---

**Last Updated**: 2025-01-01
