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


def validate_gzip_file(file_path: str) -> bool:
    """
    Validate that a gzip file is complete and not truncated.
    
    Args:
        file_path: Path to gzip file
        
    Returns:
        True if valid, False if truncated/corrupted
    """
    try:
        with gzip.open(file_path, 'rb') as f:
            # Read in chunks to avoid memory issues with large files
            while True:
                chunk = f.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
        return True
    except EOFError:
        logger.warning(f"Gzip file is truncated: {file_path}")
        return False
    except Exception as e:
        logger.warning(f"Gzip file validation failed: {file_path} - {e}")
        return False


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
    chunk_size: int = 1024 * 1024,  # 1MB chunks
    validate: bool = True,
    max_retries: int = 3
) -> Optional[str]:
    """
    Download a single WET file with validation and retry support.
    
    Args:
        wet_path: Path to WET file on Common Crawl
        output_dir: Local output directory
        chunk_size: Download chunk size in bytes
        validate: If True, validate gzip integrity after download
        max_retries: Maximum number of retry attempts on failure
        
    Returns:
        Local file path if successful, None otherwise
    """
    url = f"{CC_BASE_URL}/{wet_path}"
    filename = os.path.basename(wet_path)
    output_path = Path(output_dir) / filename
    
    # Create output directory
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Skip if already downloaded and valid
    if output_path.exists():
        if validate:
            if validate_gzip_file(str(output_path)):
                logger.info(f"File already exists and is valid: {output_path}")
                return str(output_path)
            else:
                logger.warning(f"Existing file is corrupted, re-downloading: {output_path}")
                output_path.unlink()
        else:
            logger.info(f"File already exists: {output_path}")
            return str(output_path)
    
    for attempt in range(1, max_retries + 1):
        logger.info(f"Downloading (attempt {attempt}/{max_retries}): {url}")
        
        try:
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            with open(output_path, 'wb') as f:
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=filename) as pbar:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            pbar.update(len(chunk))
            
            # Verify downloaded size matches expected
            if total_size > 0 and downloaded_size != total_size:
                logger.warning(f"Size mismatch: expected {total_size}, got {downloaded_size}")
                if output_path.exists():
                    output_path.unlink()
                continue
            
            # Validate gzip integrity
            if validate:
                if validate_gzip_file(str(output_path)):
                    logger.info(f"Downloaded and validated: {output_path}")
                    return str(output_path)
                else:
                    logger.warning(f"Downloaded file failed validation (attempt {attempt})")
                    if output_path.exists():
                        output_path.unlink()
                    continue
            else:
                logger.info(f"Downloaded: {output_path}")
                return str(output_path)
            
        except Exception as e:
            logger.error(f"Error downloading {url} (attempt {attempt}): {e}")
            # Clean up partial download
            if output_path.exists():
                output_path.unlink()
            
            if attempt < max_retries:
                import time
                wait_time = 2 ** attempt  # Exponential backoff
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
    
    logger.error(f"Failed to download after {max_retries} attempts: {url}")
    return None


def download_wet_partial(
    wet_path: str,
    bytes_range: int = 10 * 1024 * 1024,  # 10MB
    output_dir: str = "data/raw/commoncrawl"
) -> Optional[str]:
    """
    Download only the first N bytes of a WET file (for testing ONLY).
    
    WARNING: Partial downloads create TRUNCATED gzip files that will cause
    'Compressed file ended before the end-of-stream marker' errors during parsing.
    The parser will handle this gracefully and extract as many complete records
    as possible, but some data will be lost.
    
    Use this only for quick testing - for production, download complete files.
    
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
    
    logger.warning(
        f"Downloading PARTIAL file ({bytes_range} bytes). "
        "This will create a truncated gzip that cannot be fully parsed. "
        "Use full downloads for production."
    )
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
    partial: bool = False,
    parallel: bool = True,
    max_workers: Optional[int] = None
) -> List[str]:
    """
    Download multiple WET files from a Common Crawl release.
    
    Args:
        crawl_id: Common Crawl crawl identifier
        output_dir: Local output directory
        max_files: Maximum number of files to download
        partial: If True, download only partial files (for testing)
        parallel: If True, download files in parallel (recommended for TB-scale)
        max_workers: Number of parallel download workers (auto-detected if None)
        
    Returns:
        List of downloaded file paths
    """
    paths = get_wet_paths(crawl_id, limit=max_files)
    
    if not paths:
        logger.warning("No WET paths found")
        return []
    
    if parallel and len(paths) > 1:
        # Use parallel downloading for large batches
        return download_wet_files_parallel(
            paths, 
            output_dir=output_dir, 
            partial=partial,
            max_workers=max_workers
        )
    
    # Sequential download for small batches or when parallel is disabled
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


def download_wet_files_parallel(
    wet_paths: List[str],
    output_dir: str = "data/raw/commoncrawl",
    partial: bool = False,
    max_workers: Optional[int] = None
) -> List[str]:
    """
    Download multiple WET files in parallel using thread pool.
    
    Optimized for TB-scale CommonCrawl data downloads.
    Uses threads (I/O-bound) with configurable concurrency.
    
    Args:
        wet_paths: List of WET file paths from Common Crawl
        output_dir: Local output directory
        partial: If True, download only partial files
        max_workers: Number of parallel workers (default: 4x CPU cores, max 32)
        
    Returns:
        List of successfully downloaded file paths
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import multiprocessing as mp
    
    if max_workers is None:
        # I/O-bound: use more workers than CPU cores
        max_workers = min(mp.cpu_count() * 4, 32)
    
    total = len(wet_paths)
    downloaded = []
    failed = []
    
    logger.info(f"Starting parallel download of {total} WET files with {max_workers} workers")
    
    def download_single(wet_path: str) -> Optional[str]:
        """Download a single WET file."""
        if partial:
            return download_wet_partial(wet_path, output_dir=output_dir)
        else:
            return download_wet_file(wet_path, output_dir=output_dir)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_path = {
            executor.submit(download_single, path): path 
            for path in wet_paths
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_path):
            path = future_to_path[future]
            
            try:
                result = future.result()
                if result:
                    downloaded.append(result)
                else:
                    failed.append(path)
            except Exception as e:
                logger.error(f"Download failed for {path}: {e}")
                failed.append(path)
            
            # Progress logging
            completed = len(downloaded) + len(failed)
            if completed % 10 == 0 or completed == total:
                logger.info(f"Download progress: {completed}/{total} ({len(downloaded)} successful)")
    
    logger.info(f"Parallel download complete: {len(downloaded)}/{total} successful")
    
    if failed:
        logger.warning(f"Failed downloads: {len(failed)}")
    
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

