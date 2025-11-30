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
    "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip"
]


def is_valid_zip(file_path: str) -> bool:
    """
    Check if a file is a valid ZIP archive by verifying its magic bytes.
    
    Args:
        file_path: Path to the file to check
        
    Returns:
        True if the file appears to be a valid ZIP file
    """
    ZIP_MAGIC_BYTES = [
        b'PK\x03\x04',  # Standard ZIP
        b'PK\x05\x06',  # Empty ZIP
        b'PK\x07\x08',  # Spanned ZIP
    ]
    
    try:
        with open(file_path, 'rb') as f:
            header = f.read(4)
            return any(header.startswith(magic) for magic in ZIP_MAGIC_BYTES)
    except Exception:
        return False


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
        # Validate existing file is actually a valid ZIP
        if filename.endswith('.zip') and not is_valid_zip(str(output_path)):
            logger.warning(f"Existing file is not a valid ZIP, removing: {output_path}")
            output_path.unlink()
        else:
            logger.info(f"File already exists: {output_path}")
            return str(output_path)
    
    logger.info(f"Downloading: {url}")
    
    try:
        response = requests.get(url, stream=True, timeout=600)
        response.raise_for_status()
        
        # Check content-type to detect HTML error pages
        content_type = response.headers.get('content-type', '')
        if 'text/html' in content_type.lower() and filename.endswith('.zip'):
            logger.error(f"Server returned HTML instead of ZIP file (likely a redirect or error page)")
            logger.error(f"URL may be invalid or requires authentication: {url}")
            return None
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(output_path, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=filename) as pbar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        
        # Validate the downloaded file is actually a ZIP
        if filename.endswith('.zip') and not is_valid_zip(str(output_path)):
            logger.error(f"Downloaded file is not a valid ZIP archive: {output_path}")
            logger.error("The server may have returned an error page instead of the actual file")
            # Log first 500 bytes to help diagnose
            try:
                with open(output_path, 'r', encoding='utf-8', errors='ignore') as f:
                    preview = f.read(500)
                    if '<html' in preview.lower() or '<!doctype' in preview.lower():
                        logger.error("Downloaded content appears to be HTML (likely an error page)")
            except Exception:
                pass
            output_path.unlink()
            return None
        
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

