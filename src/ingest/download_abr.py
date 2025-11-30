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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Download ABR bulk data
    xml_files = download_abr_bulk()
    print(f"Downloaded {len(xml_files)} XML files")

