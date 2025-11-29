# =============================================================================
# Australia Company ETL Pipeline - Production Run Script (Windows PowerShell)
# =============================================================================

param(
    [int]$MaxRecords = 200000,
    [bool]$EnableLLM = $true,
    [double]$FuzzyThreshold = 0.75,
    [int]$CCMaxFiles = 50
)

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Blue
Write-Host "   Australia Company ETL Pipeline" -ForegroundColor Blue
Write-Host "   Production Deployment (Windows)" -ForegroundColor Blue
Write-Host "========================================" -ForegroundColor Blue
Write-Host ""

# =============================================================================
# Step 1: Environment Check
# =============================================================================
Write-Host "Step 1: Checking environment..." -ForegroundColor Yellow

# Check Python
try {
    $pythonVersion = python --version
    Write-Host "✓ Python found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "Error: Python not found" -ForegroundColor Red
    exit 1
}

# Check Docker
try {
    $dockerVersion = docker --version
    Write-Host "✓ Docker found: $dockerVersion" -ForegroundColor Green
} catch {
    Write-Host "Error: Docker not found" -ForegroundColor Red
    exit 1
}

# Activate virtual environment
if (Test-Path "venv\Scripts\Activate.ps1") {
    Write-Host "Activating virtual environment..."
    . .\venv\Scripts\Activate.ps1
}

# Load environment variables from .env
if (Test-Path ".env") {
    Write-Host "Loading .env file..."
    Get-Content ".env" | ForEach-Object {
        if ($_ -match '^([^#][^=]*)=(.*)$') {
            [Environment]::SetEnvironmentVariable($matches[1], $matches[2], "Process")
        }
    }
}

# Check OpenAI API key
if (-not $env:OPENAI_API_KEY -and $EnableLLM) {
    Write-Host "Warning: OPENAI_API_KEY not set. LLM matching will be disabled." -ForegroundColor Yellow
    $EnableLLM = $false
}

Write-Host ""

# =============================================================================
# Step 2: Create Directories
# =============================================================================
Write-Host "Step 2: Creating directories..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "data\raw\commoncrawl" | Out-Null
New-Item -ItemType Directory -Force -Path "data\raw\abr" | Out-Null
New-Item -ItemType Directory -Force -Path "data\processed" | Out-Null
New-Item -ItemType Directory -Force -Path "data\output" | Out-Null
New-Item -ItemType Directory -Force -Path "logs" | Out-Null
Write-Host "✓ Directories created" -ForegroundColor Green
Write-Host ""

# =============================================================================
# Step 3: Start Database
# =============================================================================
Write-Host "Step 3: Starting PostgreSQL..." -ForegroundColor Yellow

$postgresRunning = docker-compose ps postgres 2>&1 | Select-String "Up"
if ($postgresRunning) {
    Write-Host "✓ PostgreSQL already running" -ForegroundColor Green
} else {
    docker-compose up -d postgres
    Write-Host "Waiting for PostgreSQL to be ready..."
    Start-Sleep -Seconds 10
    
    # Wait for database
    $ready = $false
    $attempts = 0
    while (-not $ready -and $attempts -lt 30) {
        try {
            docker-compose exec -T postgres pg_isready -U postgres 2>&1 | Out-Null
            $ready = $true
        } catch {
            Start-Sleep -Seconds 2
            $attempts++
        }
    }
    Write-Host "✓ PostgreSQL started" -ForegroundColor Green
}
Write-Host ""

# =============================================================================
# Step 4: Download/Create Data
# =============================================================================
Write-Host "Step 4: Preparing data..." -ForegroundColor Yellow

# Check for existing data
$ccFiles = Get-ChildItem -Path "data\raw\commoncrawl" -Filter "*.wet.gz" -ErrorAction SilentlyContinue
$abrFiles = Get-ChildItem -Path "data\raw\abr" -Filter "*.xml" -ErrorAction SilentlyContinue

if ($abrFiles.Count -eq 0) {
    Write-Host "Creating sample ABR data..."
    python -c @"
from src.ingest.download_abr import create_sample_abr_data
create_sample_abr_data('data/raw/abr/abr_bulk.xml', num_records=500000)
print('Created ABR data with 500,000 records')
"@
}

Write-Host "✓ Data prepared" -ForegroundColor Green
Write-Host ""

# =============================================================================
# Step 5: Run ETL Pipeline
# =============================================================================
Write-Host "Step 5: Running ETL Pipeline..." -ForegroundColor Yellow
Write-Host "Configuration:"
Write-Host "  - Max Records: $MaxRecords"
Write-Host "  - LLM Enabled: $EnableLLM"
Write-Host "  - Fuzzy Threshold: $FuzzyThreshold"
Write-Host ""

$startTime = Get-Date
$logFile = "logs\pipeline_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

if ($EnableLLM) {
    python src/pipeline.py --max-records $MaxRecords --llm 2>&1 | Tee-Object -FilePath $logFile
} else {
    python src/pipeline.py --max-records $MaxRecords 2>&1 | Tee-Object -FilePath $logFile
}

$endTime = Get-Date
$duration = ($endTime - $startTime).TotalSeconds
Write-Host "✓ ETL Pipeline completed in $([math]::Round($duration, 2)) seconds" -ForegroundColor Green
Write-Host ""

# =============================================================================
# Step 6: Run dbt Transformations
# =============================================================================
Write-Host "Step 6: Running dbt transformations..." -ForegroundColor Yellow
Push-Location dbt
dbt run --profiles-dir .
Write-Host "✓ dbt models created" -ForegroundColor Green

Write-Host "Running dbt tests..."
dbt test --profiles-dir .
Pop-Location
Write-Host ""

# =============================================================================
# Step 7: Generate Statistics
# =============================================================================
Write-Host "Step 7: Generating statistics..." -ForegroundColor Yellow

docker-compose exec -T postgres psql -U postgres -d companydb -c @"
SELECT 'web_companies' as table_name, COUNT(*) as count FROM web_companies
UNION ALL SELECT 'abr_entities', COUNT(*) FROM abr_entities
UNION ALL SELECT 'entity_match_results', COUNT(*) FROM entity_match_results
UNION ALL SELECT 'unified_companies', COUNT(*) FROM unified_companies;
"@

Write-Host ""

# =============================================================================
# Step 8: Export Results
# =============================================================================
Write-Host "Step 8: Exporting results..." -ForegroundColor Yellow

docker-compose exec -T postgres psql -U postgres -d companydb -c `
    "COPY (SELECT * FROM unified_companies ORDER BY abn) TO STDOUT WITH CSV HEADER" `
    > data\output\unified_companies.csv

$exportCount = (Get-Content "data\output\unified_companies.csv" | Measure-Object -Line).Lines - 1
Write-Host "✓ Exported $exportCount records to data\output\unified_companies.csv" -ForegroundColor Green
Write-Host ""

# =============================================================================
# Summary
# =============================================================================
Write-Host "========================================" -ForegroundColor Blue
Write-Host "   Pipeline Complete!" -ForegroundColor Blue
Write-Host "========================================" -ForegroundColor Blue
Write-Host ""
Write-Host "Results:"
Write-Host "  - Duration: $([math]::Round($duration, 2)) seconds"
Write-Host "  - Exported: $exportCount unified companies"
Write-Host "  - Output: data\output\unified_companies.csv"
Write-Host ""
Write-Host "Next steps:"
Write-Host "  - View dbt docs: cd dbt; dbt docs serve"
Write-Host "  - Query database: docker-compose exec postgres psql -U postgres -d companydb"
Write-Host "  - View logs: Get-Content logs\pipeline.log"
Write-Host ""

