"""
Download ABR (Australian Business Register) bulk data from data.gov.au.
"""

import os
import zipfile
import requests
from pathlib import Path
from typing import Optional, List
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)

# data.gov.au ABR bulk data endpoints
ABR_DATASET_URL = "https://data.gov.au/data/dataset/abn-bulk-extract"

# Example direct download URLs (these may change)
ABR_SAMPLE_URLS = [
    # These are example URLs - actual URLs need to be fetched from data.gov.au
    "https://data.gov.au/data/dataset/abn-bulk-extract/resource/ABR_PUBLIC_20250101.xml.zip"
]


def download_file(
    url: str,
    output_dir: str = "data/raw/abr",
    chunk_size: int = 1024 * 1024
) -> Optional[str]:
    """
    Download a file from URL.
    
    Args:
        url: File URL
        output_dir: Output directory
        chunk_size: Download chunk size
        
    Returns:
        Local file path if successful
    """
    filename = os.path.basename(url.split('?')[0])
    output_path = Path(output_dir) / filename
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if output_path.exists():
        logger.info(f"File already exists: {output_path}")
        return str(output_path)
    
    logger.info(f"Downloading: {url}")
    
    try:
        response = requests.get(url, stream=True, timeout=600)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(output_path, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=filename) as pbar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        
        logger.info(f"Downloaded: {output_path}")
        return str(output_path)
        
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        if output_path.exists():
            output_path.unlink()
        return None


def extract_zip(
    zip_path: str,
    output_dir: Optional[str] = None
) -> List[str]:
    """
    Extract a ZIP file.
    
    Args:
        zip_path: Path to ZIP file
        output_dir: Output directory (defaults to same as ZIP)
        
    Returns:
        List of extracted file paths
    """
    if output_dir is None:
        output_dir = os.path.dirname(zip_path)
    
    logger.info(f"Extracting: {zip_path}")
    
    extracted = []
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            for name in z.namelist():
                if name.endswith('.xml'):
                    z.extract(name, output_dir)
                    extracted.append(os.path.join(output_dir, name))
                    logger.info(f"Extracted: {name}")
        
        return extracted
        
    except Exception as e:
        logger.error(f"Error extracting {zip_path}: {e}")
        return []


def download_abr_bulk(
    output_dir: str = "data/raw/abr",
    urls: Optional[List[str]] = None
) -> List[str]:
    """
    Download and extract ABR bulk data files.
    
    Args:
        output_dir: Output directory
        urls: List of download URLs (uses defaults if not provided)
        
    Returns:
        List of extracted XML file paths
    """
    if urls is None:
        urls = ABR_SAMPLE_URLS
    
    all_xml_files = []
    
    for url in urls:
        # Download
        zip_file = download_file(url, output_dir)
        
        if zip_file and zip_file.endswith('.zip'):
            # Extract
            xml_files = extract_zip(zip_file, output_dir)
            all_xml_files.extend(xml_files)
        elif zip_file and zip_file.endswith('.xml'):
            all_xml_files.append(zip_file)
    
    logger.info(f"Total XML files: {len(all_xml_files)}")
    return all_xml_files


def create_sample_abr_data(
    output_path: str = "data/raw/abr/sample_abr.xml",
    num_records: int = 100
) -> str:
    """
    Create sample ABR data for testing.
    
    Args:
        output_path: Output file path
        num_records: Number of sample records
        
    Returns:
        Path to created file
    """
    import random
    from datetime import datetime, timedelta
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    states = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]
    entity_types = ["PRV", "PUB", "IND", "TRT", "OIE"]
    statuses = ["Active", "Cancelled"]
    
    company_names = [
        "Tech Solutions", "Digital Services", "Construction Group",
        "Mining Corp", "Consulting Partners", "Financial Services",
        "Healthcare Systems", "Retail Holdings", "Manufacturing Industries",
        "Transport Logistics", "Energy Solutions", "Agricultural Enterprises"
    ]
    
    xml_content = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml_content.append('<ABRBulkExtract>')
    
    for i in range(num_records):
        # Generate random ABN (11 digits)
        abn = ''.join([str(random.randint(0, 9)) for _ in range(11)])
        
        # Random company name
        base_name = random.choice(company_names)
        name = f"{base_name} {random.choice(['Australia', 'AU', ''])} Pty Ltd".strip()
        
        # Random state and postcode
        state = random.choice(states)
        postcode = str(random.randint(1000, 9999))
        
        # Random entity type and status
        entity_type = random.choice(entity_types)
        status = random.choice(statuses)
        
        # Random start date
        start_date = datetime.now() - timedelta(days=random.randint(365, 3650))
        
        xml_content.append(f'''  <ABRRecord>
    <ABN>{abn}</ABN>
    <EntityName>{name}</EntityName>
    <EntityType>{entity_type}</EntityType>
    <EntityStatus>{status}</EntityStatus>
    <MainBusinessAddress>
      <State>{state}</State>
      <Postcode>{postcode}</Postcode>
    </MainBusinessAddress>
    <ABNStatusEffectiveFrom>{start_date.strftime("%Y-%m-%d")}</ABNStatusEffectiveFrom>
  </ABRRecord>''')
    
    xml_content.append('</ABRBulkExtract>')
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(xml_content))
    
    logger.info(f"Created sample ABR data: {output_path}")
    return output_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data for testing
    sample_file = create_sample_abr_data(num_records=100)
    print(f"Created sample file: {sample_file}")

