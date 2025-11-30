"""
Main ETL Pipeline Orchestrator for Australia Company Data.

This script orchestrates the full ETL pipeline:
1. Extract data from Common Crawl and ABR
2. Transform and clean the data
3. Perform entity matching
4. Load results into PostgreSQL

Uses Apache Spark for scalable distributed processing with parallel downloading/parsing.

Usage:
    python src/pipeline.py [--config config/pipeline_config.yaml]
    python src/pipeline.py --workers 8 --max-records 100000
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
import uuid

import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    before_log,
    after_log,
    retry_if_exception_type
)

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.utils import load_config
from src.ingest.parse_commoncrawl import CommonCrawlParser
from src.ingest.parse_abr import ABRParser
from src.ingest.download_commoncrawl import download_wet_files
from src.ingest.download_abr import download_abr_bulk
from src.load.load_postgres import PostgresLoader
from src.load.upsert_logic import upsert_unified_companies

# Spark imports for scalable processing
from src.common.spark_session import get_spark_session
from src.transform.clean_commoncrawl import clean_commoncrawl_spark
from src.transform.clean_abr import clean_abr_spark
from src.transform.entity_match import match_companies_spark

# Parallel processing utilities
from src.common.parallel import (
    parse_wet_files_parallel,
    parse_abr_files_parallel,
    get_optimal_workers,
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# Retry configuration constants
MAX_RETRY_ATTEMPTS = 3
RETRY_MIN_WAIT = 4  # seconds
RETRY_MAX_WAIT = 10  # seconds


def log_retry_attempt(retry_state):
    """Log retry attempts for debugging."""
    logger.warning(
        f"Retrying {retry_state.fn.__name__} "
        f"(attempt {retry_state.attempt_number}/{MAX_RETRY_ATTEMPTS}) "
        f"after {retry_state.outcome.exception() if retry_state.outcome else 'unknown error'}"
    )


# Retry decorator for network/IO operations (extract, load)
retry_with_backoff = retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
    before=before_log(logger, logging.WARNING),
    after=after_log(logger, logging.WARNING),
    retry=retry_if_exception_type((IOError, ConnectionError, TimeoutError, OSError)),
    reraise=True
)

# Retry decorator for database operations
retry_database = retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=2, max=8),
    before=before_log(logger, logging.WARNING),
    after=after_log(logger, logging.WARNING),
    retry=retry_if_exception_type((
        ConnectionError,
        TimeoutError,
        ConnectionResetError,
        BrokenPipeError,
        OSError
    )),
    reraise=True
)


class ETLPipeline:
    """
    Main ETL Pipeline for Australia Company Data.
    
    Orchestrates data extraction, transformation, matching, and loading.
    Uses Apache Spark for scalable distributed processing with parallel downloading/parsing.
    """
    
    def __init__(
        self,
        config_path: str = "config/pipeline_config.yaml",
        max_workers: Optional[int] = None
    ):
        """
        Initialize the pipeline.
        
        Args:
            config_path: Path to configuration file
            max_workers: Number of parallel workers (auto-detected if None)
        """
        try:
            self.config = load_config(config_path)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}, using defaults")
            self.config = self._default_config()
        
        self.max_workers = max_workers or get_optimal_workers("cpu")
        
        self.run_id = str(uuid.uuid4())[:8]
        self.start_time = None
        
        # Initialize components
        self.cc_parser = CommonCrawlParser()
        self.abr_parser = ABRParser()
        
        # Initialize loader (lazy)
        self._loader = None
        
        # Spark session (lazy initialization)
        self._spark = None
        
        # Data storage (Spark DataFrames)
        self.cc_data = None
        self.abr_data = None
        self.matches = None
        
        logger.info(f"Pipeline initialized with run_id: {self.run_id}")
        logger.info(f"Workers: {self.max_workers}")
    
    @property
    def spark(self):
        """Get Spark session (lazy initialization)."""
        if self._spark is None:
            spark_config = self.config.get("spark", {})
            print("spark config", spark_config)
            # Get master URL from config, or use environment variable, or default to local
            master = spark_config.get("master")
            # Remove master from config dict before passing to get_spark_session
            spark_config_without_master = {k: v for k, v in spark_config.items() if k != "master"}
            self._spark = get_spark_session(
                app_name=f"AustraliaCompanyETL-{self.run_id}",
                master=master,
                config=spark_config_without_master
            )
        return self._spark
    
    def _default_config(self):
        """Return default configuration."""
        return {
            'postgres': {
                'host': 'localhost',
                'port': 5432,
                'database': 'companydb',
                'user': 'postgres',
                'password': 'postgres'
            },
            'matching': {
                'fuzzy': {'threshold': 0.75}
            },
            'paths': {
                'commoncrawl': {'crawl_id': 'CC-MAIN-2024-18', 'max_files': 2},
                'abr': {'output_path': 'data/raw/abr'}
            }
        }
    
    @property
    def loader(self) -> PostgresLoader:
        """Get PostgreSQL loader (lazy initialization)."""
        if self._loader is None:
            pg_config = self.config.get('postgres', {})
            self._loader = PostgresLoader(
                host=pg_config.get('host', 'localhost'),
                port=pg_config.get('port', 5432),
                database=pg_config.get('database', 'companydb'),
                user=pg_config.get('user', 'postgres'),
                password=pg_config.get('password', 'postgres')
            )
        return self._loader
    
    def run(
        self,
        skip_download: bool = False,
        use_llm: bool = False,
        max_records: Optional[int] = None,
        skip_load: bool = False
    ) -> dict:
        """
        Run the full ETL pipeline.
        
        Args:
            skip_download: Skip downloading new data (use existing files)
            use_llm: Enable LLM-based matching
            max_records: Maximum records to process (None for all)
            skip_load: Skip loading to database
            
        Returns:
            Pipeline run statistics
        """
        self.start_time = datetime.now()
        logger.info(f"Starting pipeline run: {self.run_id}")
        
        stats = {
            'run_id': self.run_id,
            'started_at': self.start_time.isoformat(),
            'status': 'running'
        }
        
        try:
            # Step 1: Extract
            logger.info("=" * 50)
            logger.info("STEP 1: EXTRACT")
            logger.info("=" * 50)
            
            self.extract(
                skip_download=skip_download,
                max_records=max_records
            )
            
            stats['cc_extracted'] = self._get_row_count(self.cc_data)
            stats['abr_extracted'] = self._get_row_count(self.abr_data)
            
            # Step 2: Transform
            logger.info("=" * 50)
            logger.info("STEP 2: TRANSFORM")
            logger.info("=" * 50)
            
            self.transform()
            
            stats['cc_cleaned'] = self._get_row_count(self.cc_data)
            stats['abr_cleaned'] = self._get_row_count(self.abr_data)
            
            # Step 3: Match
            logger.info("=" * 50)
            logger.info("STEP 3: ENTITY MATCHING")
            logger.info("=" * 50)
            
            self.match(use_llm=use_llm)
            
            stats['matches_found'] = self._get_row_count(self.matches)
            
            # Step 4: Load
            if not skip_load:
                logger.info("=" * 50)
                logger.info("STEP 4: LOAD")
                logger.info("=" * 50)
                
                load_stats = self.load()
                stats.update(load_stats)
            else:
                logger.info("Skipping database load (skip_load=True)")
            
            # Finalize
            stats['status'] = 'completed'
            stats['completed_at'] = datetime.now().isoformat()
            stats['duration_seconds'] = (datetime.now() - self.start_time).total_seconds()
            
            logger.info("=" * 50)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"Duration: {stats['duration_seconds']:.2f} seconds")
            logger.info(f"Matches found: {stats.get('matches_found', 0)}")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            stats['status'] = 'failed'
            stats['error'] = str(e)
            raise
        
        finally:
            if self._loader:
                self._loader.close()
        
        return stats
    
    @retry_with_backoff
    def extract(
        self,
        skip_download: bool = False,
        max_records: Optional[int] = None
    ):
        """
        Extract data from Common Crawl and ABR using parallel processing.
        
        Retries up to 3 times with exponential backoff on network/IO errors.
        
        Args:
            skip_download: Skip downloading new data (use existing files)
            max_records: Maximum records to process (None for all)
        """
        # Extract Common Crawl data
        logger.info("Extracting Common Crawl data...")
        cc_config = self.config.get('paths', {}).get('commoncrawl', {})
        
        if not skip_download:
            # Download WET files in parallel
            wet_files = download_wet_files(
                crawl_id=cc_config.get('crawl_id', 'CC-MAIN-2025-43'),
                max_files=cc_config.get('max_files', 2),
                partial=True,
                parallel=True,
                max_workers=self.max_workers
            )
        else:
            wet_dir = Path(cc_config.get('output_path', 'data/raw/commoncrawl'))
            wet_files = list(wet_dir.glob('*.wet.gz'))
            logger.info(f"Using existing WET files: {len(wet_files)} found")
        
        # Parse WET files in parallel
        if wet_files:
            logger.info(f"Parsing {len(wet_files)} WET files in parallel with {self.max_workers} workers")
            cc_records = list(parse_wet_files_parallel(
                [str(f) for f in wet_files],
                max_records_per_file=max_records,
                max_workers=self.max_workers
            ))
        else:
            logger.warning("No WET files found")
            cc_records = []
        
        self.cc_data = self._records_to_dataframe(cc_records, "cc")
        logger.info(f"Extracted {self._get_row_count(self.cc_data)} Common Crawl records")
        
        # Extract ABR data
        logger.info("Extracting ABR data...")
        abr_config = self.config.get('paths', {}).get('abr', {})
        
        if not skip_download:
            xml_files = download_abr_bulk(
                output_dir=abr_config.get('output_path', 'data/raw/abr'),
                urls=abr_config.get('urls')
            )
        else:
            abr_dir = Path(abr_config.get('output_path', 'data/raw/abr'))
            xml_files = list(abr_dir.glob('*.xml'))
            logger.info(f"Using existing ABR files: {len(xml_files)} found")
        
        # Parse ABR files in parallel
        if xml_files:
            logger.info(f"Parsing {len(xml_files)} ABR files in parallel with {self.max_workers} workers")
            abr_records = list(parse_abr_files_parallel(
                [str(f) for f in xml_files],
                max_records_per_file=max_records,
                max_workers=self.max_workers
            ))
        else:
            logger.warning("No ABR XML files found")
            abr_records = []
        
        self.abr_data = self._records_to_dataframe(abr_records, "abr")
        logger.info(f"Extracted {self._get_row_count(self.abr_data)} ABR records")
    
    def _records_to_dataframe(self, records: list, source: str):
        """Convert list of records to Spark DataFrame."""
        if not records:
            return self.spark.createDataFrame([], schema=None)
        return self.spark.createDataFrame(records)
    
    def _get_row_count(self, df) -> int:
        """Get row count from Spark DataFrame."""
        if df is None:
            return 0
        return df.count()
    
    def _is_empty(self, df) -> bool:
        """Check if Spark DataFrame is empty."""
        if df is None:
            return True
        return df.count() == 0
    
    @retry_with_backoff
    def transform(self):
        """
        Transform and clean extracted data using Spark.
        
        Retries up to 3 times with exponential backoff on processing errors.
        """
        if self.cc_data is not None and not self._is_empty(self.cc_data):
            logger.info("Cleaning Common Crawl data with Spark...")
            self.cc_data = clean_commoncrawl_spark(self.cc_data, self.spark)
            logger.info(f"Cleaned CC data: {self._get_row_count(self.cc_data)} records")
        
        if self.abr_data is not None and not self._is_empty(self.abr_data):
            logger.info("Cleaning ABR data with Spark...")
            self.abr_data = clean_abr_spark(self.abr_data, self.spark)
            logger.info(f"Cleaned ABR data: {self._get_row_count(self.abr_data)} records")
    
    @retry_with_backoff
    def match(self, use_llm: bool = False):
        """
        Perform entity matching between CC and ABR data using Spark.
        
        Uses distributed join with broadcast for TB-scale processing.
        
        Retries up to 3 times with exponential backoff on matching errors.
        """
        if self.cc_data is None or self.abr_data is None:
            logger.warning("No data to match")
            self.matches = self._empty_dataframe()
            return
        
        if self._is_empty(self.cc_data) or self._is_empty(self.abr_data):
            logger.warning("Empty data, skipping matching")
            self.matches = self._empty_dataframe()
            return
        
        matching_config = self.config.get('matching', {})
        fuzzy_threshold = matching_config.get('fuzzy', {}).get('threshold', 0.75)
        
        cc_count = self._get_row_count(self.cc_data)
        abr_count = self._get_row_count(self.abr_data)
        logger.info(f"Matching {cc_count} CC records with {abr_count} ABR records using Spark")
        
        # Spark: Distributed matching
        self.matches = match_companies_spark(
            self.cc_data,
            self.abr_data,
            self.spark,
            fuzzy_threshold=fuzzy_threshold
        )
        
        match_count = self._get_row_count(self.matches)
        logger.info(f"Found {match_count} matches")
        
        if match_count > 0:
            avg_score = self._get_avg_score(self.matches)
            logger.info(f"Average match score: {avg_score:.2%}")
    
    def _empty_dataframe(self):
        """Create empty Spark DataFrame."""
        return self.spark.createDataFrame([], schema=None)
    
    def _get_avg_score(self, df) -> float:
        """Get average final_score from Spark DataFrame."""
        if self._is_empty(df):
            return 0.0
        return df.select("final_score").agg({"final_score": "avg"}).collect()[0][0]
    
    @retry_database
    def load(self) -> dict:
        """
        Load results into PostgreSQL.
        
        Converts Spark DataFrames to pandas for database loading.
        Retries up to 3 times with exponential backoff on database connection errors.
        """
        stats = {}
        
        try:
            logger.info("Creating database tables...")
            self.loader.create_tables()
            
            # Convert to pandas for database loading
            cc_pandas = self._to_pandas(self.cc_data)
            abr_pandas = self._to_pandas(self.abr_data)
            matches_pandas = self._to_pandas(self.matches)
            
            if cc_pandas is not None and not cc_pandas.empty:
                logger.info("Loading Common Crawl data...")
                stats['cc_loaded'] = self.loader.load_web_companies(cc_pandas)
            
            if abr_pandas is not None and not abr_pandas.empty:
                logger.info("Loading ABR data...")
                stats['abr_loaded'] = self.loader.load_abr_entities(abr_pandas)
            
            if matches_pandas is not None and not matches_pandas.empty:
                logger.info("Loading match results...")
                stats['matches_loaded'] = self.loader.load_match_results(matches_pandas)
                
                logger.info("Creating unified company records...")
                stats['unified_loaded'] = upsert_unified_companies(
                    matches_pandas.to_dict('records'),
                    abr_pandas,
                    self.loader
                )
            
        except Exception as e:
            logger.error(f"Load failed: {e}")
            stats['load_error'] = str(e)
        
        return stats
    
    def _to_pandas(self, df):
        """Convert Spark DataFrame to pandas."""
        if df is None:
            return None
        return df.toPandas() if df.count() > 0 else pd.DataFrame()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Australia Company ETL Pipeline - Spark-based parallel processing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline with default settings
  python src/pipeline.py

  # Run with specific number of workers and max records
  python src/pipeline.py --workers 8 --max-records 100000

  # Use existing downloaded files (skip download)
  python src/pipeline.py --skip-download

  # Skip database loading
  python src/pipeline.py --skip-load
        """
    )
    parser.add_argument('--config', default='config/pipeline_config.yaml',
                        help='Path to configuration file')
    parser.add_argument('--llm', action='store_true',
                        help='Enable LLM-based matching')
    parser.add_argument('--max-records', type=int, default=None,
                        help='Maximum records to process (default: all)')
    parser.add_argument('--skip-download', action='store_true',
                        help='Skip downloading data (use existing files)')
    parser.add_argument('--skip-load', action='store_true',
                        help='Skip loading to database')
    parser.add_argument('--workers', type=int, default=None,
                        help='Number of parallel workers (auto-detected if not specified)')
    
    args = parser.parse_args()
    
    # Create directories
    Path('logs').mkdir(exist_ok=True)
    Path('data/raw/commoncrawl').mkdir(parents=True, exist_ok=True)
    Path('data/raw/abr').mkdir(parents=True, exist_ok=True)
    Path('data/processed').mkdir(parents=True, exist_ok=True)
    Path('data/output').mkdir(parents=True, exist_ok=True)
    
    # Run pipeline
    pipeline = ETLPipeline(
        config_path=args.config,
        max_workers=args.workers
    )
    
    stats = pipeline.run(
        skip_download=args.skip_download,
        use_llm=args.llm,
        max_records=args.max_records,
        skip_load=args.skip_load
    )
    
    print("\n" + "=" * 50)
    print("PIPELINE STATISTICS")
    print("=" * 50)
    print(f"  Workers: {args.workers or 'auto'}")
    for key, value in stats.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
