"""
Main ETL Pipeline Orchestrator for Australia Company Data.

This script orchestrates the full ETL pipeline:
1. Extract data from Common Crawl and ABR
2. Transform and clean the data
3. Perform entity matching
4. Load results into PostgreSQL

Usage:
    python src/pipeline.py [--config config/pipeline_config.yaml]
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
import uuid

import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.utils import load_config
from src.ingest.parse_commoncrawl import CommonCrawlParser
from src.ingest.parse_abr import ABRParser
from src.ingest.download_commoncrawl import download_wet_files
from src.ingest.download_abr import create_sample_abr_data, download_abr_bulk
from src.transform.clean_commoncrawl import clean_commoncrawl_data
from src.transform.clean_abr import clean_abr_data
from src.transform.entity_match import match_companies
from src.load.load_postgres import PostgresLoader
from src.load.upsert_logic import upsert_matches, upsert_unified_companies

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Main ETL Pipeline for Australia Company Data.
    
    Orchestrates data extraction, transformation, matching, and loading.
    """
    
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        """
        Initialize the pipeline.
        
        Args:
            config_path: Path to configuration file
        """
        try:
            self.config = load_config(config_path)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}, using defaults")
            self.config = self._default_config()
        
        self.run_id = str(uuid.uuid4())[:8]
        self.start_time = None
        
        # Initialize components
        self.cc_parser = CommonCrawlParser()
        self.abr_parser = ABRParser()
        
        # Initialize loader (lazy)
        self._loader = None
        
        # Data storage
        self.cc_data = None
        self.abr_data = None
        self.matches = None
        
        logger.info(f"Pipeline initialized with run_id: {self.run_id}")
    
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
                'commoncrawl': {'crawl_id': 'CC-MAIN-2024-18', 'max_files': 2}
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
        use_sample_data: bool = True,
        use_llm: bool = False,
        max_records: Optional[int] = 1000,
        skip_load: bool = False
    ) -> dict:
        """
        Run the full ETL pipeline.
        
        Args:
            skip_download: Skip downloading new data
            use_sample_data: Use sample data for testing
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
                use_sample_data=use_sample_data,
                max_records=max_records
            )
            
            stats['cc_extracted'] = len(self.cc_data) if self.cc_data is not None else 0
            stats['abr_extracted'] = len(self.abr_data) if self.abr_data is not None else 0
            
            # Step 2: Transform
            logger.info("=" * 50)
            logger.info("STEP 2: TRANSFORM")
            logger.info("=" * 50)
            
            self.transform()
            
            stats['cc_cleaned'] = len(self.cc_data) if self.cc_data is not None else 0
            stats['abr_cleaned'] = len(self.abr_data) if self.abr_data is not None else 0
            
            # Step 3: Match
            logger.info("=" * 50)
            logger.info("STEP 3: ENTITY MATCHING")
            logger.info("=" * 50)
            
            self.match(use_llm=use_llm)
            
            stats['matches_found'] = len(self.matches) if self.matches is not None and not self.matches.empty else 0
            
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
    
    def extract(
        self,
        skip_download: bool = False,
        use_sample_data: bool = True,
        max_records: Optional[int] = 1000
    ):
        """Extract data from Common Crawl and ABR."""
        # Extract Common Crawl data
        logger.info("Extracting Common Crawl data...")
        
        if use_sample_data:
            self.cc_data = self._create_sample_cc_data(max_records or 100)
        else:
            cc_config = self.config.get('paths', {}).get('commoncrawl', {})
            
            if not skip_download:
                wet_files = download_wet_files(
                    crawl_id=cc_config.get('crawl_id', 'CC-MAIN-2025-43'),
                    max_files=cc_config.get('max_files', 2),
                    partial=True
                )
            else:
                wet_dir = Path(cc_config.get('output_path', 'data/raw/commoncrawl'))
                wet_files = list(wet_dir.glob('*.wet.gz'))
            
            records = []
            for wet_file in wet_files:
                for company in self.cc_parser.parse_wet_file(str(wet_file), max_records):
                    records.append(company.to_dict())
                    if max_records and len(records) >= max_records:
                        break
                if max_records and len(records) >= max_records:
                    break
            
            self.cc_data = pd.DataFrame(records)
        
        logger.info(f"Extracted {len(self.cc_data)} Common Crawl records")
        
        # Extract ABR data
        logger.info("Extracting ABR data...")
        
        if use_sample_data:
            # Create sample ABR data for testing
            Path('data/raw/abr').mkdir(parents=True, exist_ok=True)
            abr_file = create_sample_abr_data(
                output_path="data/raw/abr/sample_abr.xml",
                num_records=max_records or 500
            )
            records = list(self.abr_parser.parse_file(abr_file, max_records))
            self.abr_data = pd.DataFrame([r.to_dict() for r in records])
        else:
            abr_config = self.config.get('paths', {}).get('abr', {})
            
            if not skip_download:
                xml_files = download_abr_bulk(
                    output_dir=abr_config.get('output_path', 'data/raw/abr'),
                    urls=abr_config.get('urls')
                )
            else:
                abr_dir = Path(abr_config.get('output_path', 'data/raw/abr'))
                xml_files = list(abr_dir.glob('*.xml'))
            
            records = []
            for xml_file in xml_files:
                for entity in self.abr_parser.parse_file(str(xml_file), max_records):
                    records.append(entity.to_dict())
                    if max_records and len(records) >= max_records:
                        break
                if max_records and len(records) >= max_records:
                    break
            
            self.abr_data = pd.DataFrame(records)
        
        logger.info(f"Extracted {len(self.abr_data)} ABR records")
    
    def transform(self):
        """Transform and clean extracted data."""
        if self.cc_data is not None and not self.cc_data.empty:
            logger.info("Cleaning Common Crawl data...")
            self.cc_data = clean_commoncrawl_data(self.cc_data)
            logger.info(f"Cleaned CC data: {len(self.cc_data)} records")
        
        if self.abr_data is not None and not self.abr_data.empty:
            logger.info("Cleaning ABR data...")
            self.abr_data = clean_abr_data(self.abr_data, validate_abns=False)
            logger.info(f"Cleaned ABR data: {len(self.abr_data)} records")
    
    def match(self, use_llm: bool = False):
        """Perform entity matching between CC and ABR data."""
        if self.cc_data is None or self.abr_data is None:
            logger.warning("No data to match")
            self.matches = pd.DataFrame()
            return
        
        if self.cc_data.empty or self.abr_data.empty:
            logger.warning("Empty data, skipping matching")
            self.matches = pd.DataFrame()
            return
        
        matching_config = self.config.get('matching', {})
        
        logger.info(f"Matching {len(self.cc_data)} CC records with {len(self.abr_data)} ABR records")
        
        self.matches = match_companies(
            self.cc_data,
            self.abr_data,
            fuzzy_threshold=matching_config.get('fuzzy', {}).get('threshold', 0.75),
            use_llm=use_llm
        )
        
        logger.info(f"Found {len(self.matches)} matches")
        
        if not self.matches.empty:
            avg_score = self.matches['final_score'].mean()
            logger.info(f"Average match score: {avg_score:.2%}")
    
    def load(self) -> dict:
        """Load results into PostgreSQL."""
        stats = {}
        
        try:
            logger.info("Creating database tables...")
            self.loader.create_tables()
            
            if self.cc_data is not None and not self.cc_data.empty:
                logger.info("Loading Common Crawl data...")
                stats['cc_loaded'] = self.loader.load_web_companies(self.cc_data)
            
            if self.abr_data is not None and not self.abr_data.empty:
                logger.info("Loading ABR data...")
                stats['abr_loaded'] = self.loader.load_abr_entities(self.abr_data)
            
            if self.matches is not None and not self.matches.empty:
                logger.info("Loading match results...")
                stats['matches_loaded'] = self.loader.load_match_results(self.matches)
                
                logger.info("Creating unified company records...")
                stats['unified_loaded'] = upsert_unified_companies(
                    self.matches.to_dict('records'),
                    self.abr_data,
                    self.loader
                )
            
        except Exception as e:
            logger.error(f"Load failed: {e}")
            stats['load_error'] = str(e)
        
        return stats
    
    def _create_sample_cc_data(self, num_records: int = 100) -> pd.DataFrame:
        """Create sample Common Crawl data for testing."""
        import random
        
        companies = [
            ("ACME Corporation", "https://www.acme.com.au", "Technology"),
            ("Tech Solutions Australia", "https://techsolutions.com.au", "IT Services"),
            ("Green Energy Partners", "https://greenenergy.com.au", "Energy"),
            ("Pacific Mining Group", "https://pacificmining.com.au", "Mining"),
            ("Sydney Consulting", "https://sydneyconsulting.com.au", "Consulting"),
            ("Melbourne Financial", "https://melbournefinancial.com.au", "Finance"),
            ("Brisbane Health Services", "https://brisbanehealth.com.au", "Healthcare"),
            ("Perth Construction", "https://perthconstruction.com.au", "Construction"),
            ("Adelaide Manufacturing", "https://adelaideman.com.au", "Manufacturing"),
            ("Hobart Logistics", "https://hobartlogistics.com.au", "Logistics"),
        ]
        
        records = []
        for i in range(min(num_records, len(companies) * 10)):
            base = companies[i % len(companies)]
            suffix = " Pty Ltd" if random.random() > 0.3 else ""
            
            records.append({
                'url': base[1],
                'company_name': f"{base[0]}{suffix}",
                'industry': base[2],
                'raw_text': f"Welcome to {base[0]}. We are a leading {base[2]} company."
            })
        
        return pd.DataFrame(records)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Australia Company ETL Pipeline')
    parser.add_argument('--config', default='config/pipeline_config.yaml',
                        help='Path to configuration file')
    parser.add_argument('--sample', action='store_true', default=False,
                        help='Use sample data for testing')
    parser.add_argument('--llm', action='store_true',
                        help='Enable LLM-based matching')
    parser.add_argument('--max-records', type=int, default=100,
                        help='Maximum records to process')
    parser.add_argument('--skip-load', action='store_true',
                        help='Skip loading to database')
    
    args = parser.parse_args()
    
    # Create directories
    Path('logs').mkdir(exist_ok=True)
    Path('data/raw/commoncrawl').mkdir(parents=True, exist_ok=True)
    Path('data/raw/abr').mkdir(parents=True, exist_ok=True)
    Path('data/processed').mkdir(parents=True, exist_ok=True)
    Path('data/output').mkdir(parents=True, exist_ok=True)
    
    # Run pipeline
    pipeline = ETLPipeline(config_path=args.config)
    
    stats = pipeline.run(
        use_sample_data=args.sample,
        use_llm=args.llm,
        max_records=args.max_records,
        skip_load=args.skip_load
    )
    
    print("\n" + "=" * 50)
    print("PIPELINE STATISTICS")
    print("=" * 50)
    for key, value in stats.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()

