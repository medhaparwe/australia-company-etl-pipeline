"""
Download Common Crawl WET files for Australian companies.
"""

import os
import gzip
import requests
from pathlib import Path
from typing import List, Optional, Generator
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)

# Common Crawl base URL
CC_BASE_URL = "https://data.commoncrawl.org"


def get_wet_paths(crawl_id: str = "CC-MAIN-2025-10", limit: int = 10) -> List[str]:
    """
    Get list of WET file paths for a specific crawl.
    
    Args:
        crawl_id: Common Crawl crawl identifier
        limit: Maximum number of files to return
        
    Returns:
        List of WET file paths
    """
    paths_url = f"{CC_BASE_URL}/crawl-data/{crawl_id}/wet.paths.gz"
    
    logger.info(f"Fetching WET paths from: {paths_url}")
    
    try:
        response = requests.get(paths_url, stream=True, timeout=60)
        response.raise_for_status()
        
        # Decompress and read paths
        paths = gzip.decompress(response.content).decode('utf-8')
        path_list = [p.strip() for p in paths.strip().split('\n') if p.strip()]
        
        logger.info(f"Found {len(path_list)} WET files")
        
        return path_list[:limit]
        
    except Exception as e:
        logger.error(f"Error fetching WET paths: {e}")
        return []


def download_wet_file(
    wet_path: str,
    output_dir: str = "data/raw/commoncrawl",
    chunk_size: int = 1024 * 1024  # 1MB chunks
) -> Optional[str]:
    """
    Download a single WET file.
    
    Args:
        wet_path: Path to WET file on Common Crawl
        output_dir: Local output directory
        chunk_size: Download chunk size in bytes
        
    Returns:
        Local file path if successful, None otherwise
    """
    url = f"{CC_BASE_URL}/{wet_path}"
    filename = os.path.basename(wet_path)
    output_path = Path(output_dir) / filename
    
    # Create output directory
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Skip if already downloaded
    if output_path.exists():
        logger.info(f"File already exists: {output_path}")
        return str(output_path)
    
    logger.info(f"Downloading: {url}")
    
    try:
        response = requests.get(url, stream=True, timeout=300)
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
        # Clean up partial download
        if output_path.exists():
            output_path.unlink()
        return None


def download_wet_partial(
    wet_path: str,
    bytes_range: int = 10 * 1024 * 1024,  # 10MB
    output_dir: str = "data/raw/commoncrawl"
) -> Optional[str]:
    """
    Download only the first N bytes of a WET file (for testing).
    
    Args:
        wet_path: Path to WET file on Common Crawl
        bytes_range: Number of bytes to download
        output_dir: Local output directory
        
    Returns:
        Local file path if successful, None otherwise
    """
    url = f"{CC_BASE_URL}/{wet_path}"
    filename = f"partial_{os.path.basename(wet_path)}"
    output_path = Path(output_dir) / filename
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Downloading first {bytes_range} bytes of: {url}")
    
    try:
        headers = {"Range": f"bytes=0-{bytes_range}"}
        response = requests.get(url, headers=headers, timeout=60)
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"Downloaded partial: {output_path} ({len(response.content)} bytes)")
        return str(output_path)
        
    except Exception as e:
        logger.error(f"Error downloading partial file: {e}")
        return None


def download_wet_files(
    crawl_id: str = "CC-MAIN-2025-10",
    output_dir: str = "data/raw/commoncrawl",
    max_files: int = 10,
    partial: bool = False
) -> List[str]:
    """
    Download multiple WET files from a Common Crawl release.
    
    Args:
        crawl_id: Common Crawl crawl identifier
        output_dir: Local output directory
        max_files: Maximum number of files to download
        partial: If True, download only partial files (for testing)
        
    Returns:
        List of downloaded file paths
    """
    paths = get_wet_paths(crawl_id, limit=max_files)
    
    if not paths:
        logger.warning("No WET paths found")
        return []
    
    downloaded = []
    
    for path in paths:
        if partial:
            result = download_wet_partial(path, output_dir=output_dir)
        else:
            result = download_wet_file(path, output_dir=output_dir)
        
        if result:
            downloaded.append(result)
    
    logger.info(f"Downloaded {len(downloaded)} files")
    return downloaded


def stream_wet_content(wet_path: str) -> Generator[bytes, None, None]:
    """
    Stream WET file content without full download.
    
    Args:
        wet_path: Path to WET file on Common Crawl
        
    Yields:
        Chunks of decompressed content
    """
    url = f"{CC_BASE_URL}/{wet_path}"
    
    try:
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        
        # Use gzip decompressor
        decompressor = gzip.GzipFile(fileobj=response.raw)
        
        while True:
            chunk = decompressor.read(1024 * 1024)  # 1MB
            if not chunk:
                break
            yield chunk
            
    except Exception as e:
        logger.error(f"Error streaming WET file: {e}")


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Download a few partial WET files for testing
    files = download_wet_files(
        crawl_id="CC-MAIN-2024-18",
        max_files=2,
        partial=True
    )
    
    print(f"Downloaded files: {files}")

