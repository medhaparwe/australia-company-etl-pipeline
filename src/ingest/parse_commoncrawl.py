"""
Parse Common Crawl WET files and extract Australian company information.
"""

import gzip
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Generator
from dataclasses import dataclass, asdict
import logging

try:
    from warcio.archiveiterator import ArchiveIterator
except ImportError:
    ArchiveIterator = None

from ..common.utils import (
    is_australian_domain,
    extract_company_from_text,
    extract_industry_from_text,
    clean_text,
    extract_domain
)

logger = logging.getLogger(__name__)


@dataclass
class WebCompany:
    """Extracted company data from a webpage."""
    url: str
    domain: str
    company_name: Optional[str]
    industry: Optional[str]
    raw_text: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class CommonCrawlParser:
    """
    Parser for Common Crawl WET files.
    
    Extracts company information from Australian websites.
    """
    
    def __init__(self, domain_filter: str = ".au"):
        """
        Initialize the parser.
        
        Args:
            domain_filter: Domain suffix to filter (e.g., ".au" for Australian)
        """
        self.domain_filter = domain_filter
    
    def parse_wet_file(
        self,
        file_path: str,
        max_records: Optional[int] = None
    ) -> Generator[WebCompany, None, None]:
        """
        Parse a WET file and yield company records.
        
        Args:
            file_path: Path to WET.gz file
            max_records: Maximum records to parse (None for all)
            
        Yields:
            WebCompany records
        """
        logger.info(f"Parsing WET file: {file_path}")
        
        if ArchiveIterator is not None:
            yield from self._parse_with_warcio(file_path, max_records)
        else:
            yield from self._parse_manual(file_path, max_records)
    
    def _parse_with_warcio(
        self,
        file_path: str,
        max_records: Optional[int] = None
    ) -> Generator[WebCompany, None, None]:
        """Parse using warcio library (preferred method)."""
        count = 0
        
        try:
            with gzip.open(file_path, 'rb') as stream:
                try:
                    for record in ArchiveIterator(stream):
                        if max_records and count >= max_records:
                            break
                        
                        # WET files use 'conversion' record type
                        if record.rec_type != 'conversion':
                            continue
                        
                        url = record.rec_headers.get_header('WARC-Target-URI')
                        
                        # Filter for Australian domains
                        if not self._should_process(url):
                            continue
                        
                        # Read content
                        try:
                            text = record.content_stream().read().decode('utf-8', errors='ignore')
                        except Exception:
                            continue
                        
                        # Extract company info
                        company = self._extract_company_info(url, text)
                        if company:
                            count += 1
                            yield company
                except EOFError:
                    # Handle truncated gzip files (partial downloads)
                    logger.warning(f"Truncated gzip file detected (partial download?): {file_path}")
                    logger.info(f"Successfully parsed {count} records before truncation")
                    return
                        
        except Exception as e:
            logger.error(f"Error parsing with warcio: {e}")
    
    def _parse_manual(
        self,
        file_path: str,
        max_records: Optional[int] = None
    ) -> Generator[WebCompany, None, None]:
        """Manual parsing without warcio (fallback method)."""
        count = 0
        current_url = None
        current_content = []
        
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8', errors='ignore') as f:
                try:
                    for line in f:
                        line = line.rstrip('\n')
                        
                        # Check for WARC header
                        if line.startswith('WARC-Target-URI:'):
                            # Process previous record
                            if current_url and current_content:
                                if max_records and count >= max_records:
                                    break
                                
                                text = '\n'.join(current_content)
                                company = self._extract_company_info(current_url, text)
                                if company:
                                    count += 1
                                    yield company
                            
                            # Start new record
                            current_url = line.replace('WARC-Target-URI:', '').strip()
                            current_content = []
                            continue
                        
                        # Skip other headers
                        if line.startswith('WARC-') or line.startswith('Content-'):
                            continue
                        
                        # Accumulate content
                        if current_url:
                            current_content.append(line)
                            
                except EOFError as e:
                    # Handle truncated gzip files gracefully - yield what we have so far
                    logger.warning(f"Truncated gzip file detected (partial download?): {file_path}")
                    if current_url and current_content:
                        text = '\n'.join(current_content)
                        company = self._extract_company_info(current_url, text)
                        if company:
                            yield company
                    logger.info(f"Successfully parsed {count} records before truncation")
                    return
                
                # Process last record
                if current_url and current_content:
                    text = '\n'.join(current_content)
                    company = self._extract_company_info(current_url, text)
                    if company:
                        yield company
                        
        except Exception as e:
            logger.error(f"Error parsing manually: {e}")
    
    def _should_process(self, url: Optional[str]) -> bool:
        """Check if URL should be processed."""
        if not url:
            return False
        
        if self.domain_filter:
            return is_australian_domain(url)
        
        return True
    
    def _extract_company_info(self, url: str, text: str) -> Optional[WebCompany]:
        """Extract company information from page content."""
        if not text or len(text) < 100:
            return None
        
        # Clean the text
        cleaned_text = clean_text(text)
        
        # Extract company name
        company_name = extract_company_from_text(text)
        
        # Extract industry
        industry = extract_industry_from_text(text)
        
        # Only return if we found something useful
        if not company_name and not industry:
            # Still return if we have URL (domain can be useful)
            pass
        
        return WebCompany(
            url=url,
            domain=extract_domain(url),
            company_name=company_name,
            industry=industry,
            raw_text=cleaned_text[:5000]  # Limit text size
        )
    
    def parse_multiple_files(
        self,
        file_paths: List[str],
        max_records_per_file: Optional[int] = None
    ) -> Generator[WebCompany, None, None]:
        """
        Parse multiple WET files.
        
        Args:
            file_paths: List of WET file paths
            max_records_per_file: Max records per file
            
        Yields:
            WebCompany records
        """
        for file_path in file_paths:
            try:
                yield from self.parse_wet_file(file_path, max_records_per_file)
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue


def parse_wet_to_dataframe(file_path: str, max_records: int = 1000):
    """
    Parse WET file and return as pandas DataFrame.
    
    Args:
        file_path: Path to WET file
        max_records: Maximum records to parse
        
    Returns:
        pandas DataFrame
    """
    import pandas as pd
    
    parser = CommonCrawlParser()
    records = list(parser.parse_wet_file(file_path, max_records))
    
    if not records:
        return pd.DataFrame()
    
    return pd.DataFrame([r.to_dict() for r in records])


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    parser = CommonCrawlParser()
    
    # Parse a sample file
    sample_file = "data/raw/commoncrawl/sample.wet.gz"
    
    for i, company in enumerate(parser.parse_wet_file(sample_file, max_records=10)):
        print(f"\n--- Record {i+1} ---")
        print(f"URL: {company.url}")
        print(f"Company: {company.company_name}")
        print(f"Industry: {company.industry}")

