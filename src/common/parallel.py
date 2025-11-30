"""
Parallel processing utilities for handling TB-scale data.

Provides multiprocessing support for:
- Parallel file downloads
- Parallel file parsing  
- Chunked data processing
"""

import os
import logging
from typing import List, Callable, Any, Optional, Iterator, TypeVar, Generic
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
import multiprocessing as mp

logger = logging.getLogger(__name__)

T = TypeVar('T')
R = TypeVar('R')


@dataclass
class TaskResult(Generic[T]):
    """Result wrapper for parallel tasks."""
    success: bool
    result: Optional[T]
    error: Optional[str]
    task_id: Any


def get_optimal_workers(task_type: str = "io") -> int:
    """
    Get optimal number of workers based on task type.
    
    Args:
        task_type: "io" for I/O-bound, "cpu" for CPU-bound tasks
        
    Returns:
        Optimal number of workers
    """
    cpu_count = mp.cpu_count()
    
    if task_type == "io":
        # I/O-bound: use more workers (network latency allows concurrency)
        return min(cpu_count * 4, 32)
    elif task_type == "cpu":
        # CPU-bound: use number of cores (avoid oversubscription)
        return cpu_count
    else:
        return cpu_count


def parallel_download(
    urls: List[str],
    download_func: Callable[[str], Optional[str]],
    max_workers: Optional[int] = None,
    show_progress: bool = True
) -> List[TaskResult[str]]:
    """
    Download multiple files in parallel using thread pool.
    
    Uses threads (not processes) since downloads are I/O-bound.
    
    Args:
        urls: List of URLs to download
        download_func: Function that takes URL and returns local path
        max_workers: Number of parallel workers (auto-detected if None)
        show_progress: Whether to log progress
        
    Returns:
        List of TaskResult with downloaded file paths
    """
    if max_workers is None:
        max_workers = get_optimal_workers("io")
    
    results = []
    total = len(urls)
    completed = 0
    
    logger.info(f"Starting parallel download of {total} files with {max_workers} workers")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_url = {
            executor.submit(download_func, url): url 
            for url in urls
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            completed += 1
            
            try:
                result = future.result()
                results.append(TaskResult(
                    success=result is not None,
                    result=result,
                    error=None,
                    task_id=url
                ))
                
                if show_progress and completed % 10 == 0:
                    logger.info(f"Download progress: {completed}/{total}")
                    
            except Exception as e:
                logger.error(f"Download failed for {url}: {e}")
                results.append(TaskResult(
                    success=False,
                    result=None,
                    error=str(e),
                    task_id=url
                ))
    
    successful = sum(1 for r in results if r.success)
    logger.info(f"Download complete: {successful}/{total} successful")
    
    return results


def parallel_parse(
    file_paths: List[str],
    parse_func: Callable[[str], List[Any]],
    max_workers: Optional[int] = None,
    show_progress: bool = True
) -> Iterator[Any]:
    """
    Parse multiple files in parallel using process pool.
    
    Uses processes (not threads) since parsing is CPU-bound.
    
    Args:
        file_paths: List of file paths to parse
        parse_func: Function that takes file path and returns list of records
        max_workers: Number of parallel workers (auto-detected if None)
        show_progress: Whether to log progress
        
    Yields:
        Parsed records from all files
    """
    if max_workers is None:
        max_workers = get_optimal_workers("cpu")
    
    total = len(file_paths)
    completed = 0
    
    logger.info(f"Starting parallel parsing of {total} files with {max_workers} workers")
    
    # Use spawn context for Windows compatibility
    ctx = mp.get_context('spawn')
    
    with ProcessPoolExecutor(max_workers=max_workers, mp_context=ctx) as executor:
        # Submit all tasks
        future_to_path = {
            executor.submit(parse_func, path): path 
            for path in file_paths
        }
        
        # Yield results as they complete
        for future in as_completed(future_to_path):
            path = future_to_path[future]
            completed += 1
            
            try:
                records = future.result()
                
                if show_progress and completed % 5 == 0:
                    logger.info(f"Parse progress: {completed}/{total}")
                
                for record in records:
                    yield record
                    
            except Exception as e:
                logger.error(f"Parse failed for {path}: {e}")
                continue
    
    logger.info(f"Parsing complete: processed {completed}/{total} files")


def chunked_iterator(
    iterable: Iterator[T],
    chunk_size: int = 10000
) -> Iterator[List[T]]:
    """
    Break an iterator into chunks for batch processing.
    
    Useful for processing large datasets without loading all into memory.
    
    Args:
        iterable: Input iterator
        chunk_size: Number of items per chunk
        
    Yields:
        Lists of items (chunks)
    """
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    
    if chunk:
        yield chunk


def parallel_map(
    items: List[T],
    func: Callable[[T], R],
    max_workers: Optional[int] = None,
    use_processes: bool = False
) -> List[R]:
    """
    Apply a function to items in parallel.
    
    Args:
        items: List of items to process
        func: Function to apply to each item
        max_workers: Number of workers
        use_processes: Use ProcessPoolExecutor if True, ThreadPoolExecutor otherwise
        
    Returns:
        List of results
    """
    if max_workers is None:
        task_type = "cpu" if use_processes else "io"
        max_workers = get_optimal_workers(task_type)
    
    executor_class = ProcessPoolExecutor if use_processes else ThreadPoolExecutor
    
    results = []
    
    with executor_class(max_workers=max_workers) as executor:
        futures = [executor.submit(func, item) for item in items]
        
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                logger.error(f"Parallel map error: {e}")
                results.append(None)
    
    return results


class BatchProcessor:
    """
    Process large datasets in batches with configurable parallelism.
    
    Useful for ETL pipelines that need to handle TB-scale data.
    """
    
    def __init__(
        self,
        batch_size: int = 10000,
        max_workers: Optional[int] = None,
        use_processes: bool = True
    ):
        """
        Initialize batch processor.
        
        Args:
            batch_size: Number of records per batch
            max_workers: Parallel workers per batch
            use_processes: Use multiprocessing if True
        """
        self.batch_size = batch_size
        self.max_workers = max_workers or get_optimal_workers("cpu" if use_processes else "io")
        self.use_processes = use_processes
    
    def process(
        self,
        items: Iterator[T],
        transform_func: Callable[[T], R],
        aggregate_func: Optional[Callable[[List[R]], Any]] = None
    ) -> Iterator[Any]:
        """
        Process items in batches with optional aggregation.
        
        Args:
            items: Iterator of input items
            transform_func: Function to apply to each item
            aggregate_func: Optional function to aggregate batch results
            
        Yields:
            Processed results (individual items or aggregated batches)
        """
        for batch in chunked_iterator(items, self.batch_size):
            # Process batch in parallel
            results = parallel_map(
                batch,
                transform_func,
                max_workers=self.max_workers,
                use_processes=self.use_processes
            )
            
            # Filter None results
            results = [r for r in results if r is not None]
            
            if aggregate_func:
                yield aggregate_func(results)
            else:
                for result in results:
                    yield result


# Worker functions for multiprocessing (must be top-level for pickle)
def _parse_wet_file_worker(args: tuple) -> List[dict]:
    """Worker function for parsing WET files."""
    file_path, max_records = args
    
    # Import here to avoid circular imports
    from src.ingest.parse_commoncrawl import CommonCrawlParser
    
    parser = CommonCrawlParser()
    records = []
    
    try:
        for company in parser.parse_wet_file(file_path, max_records):
            records.append(company.to_dict())
    except Exception as e:
        logger.error(f"Error parsing {file_path}: {e}")
    
    return records


def _parse_abr_file_worker(args: tuple) -> List[dict]:
    """Worker function for parsing ABR XML files."""
    file_path, max_records = args
    
    # Import here to avoid circular imports
    from src.ingest.parse_abr import ABRParser
    
    parser = ABRParser()
    records = []
    
    try:
        for entity in parser.parse_file(file_path, max_records):
            records.append(entity.to_dict())
    except Exception as e:
        logger.error(f"Error parsing {file_path}: {e}")
    
    return records


def parse_wet_files_parallel(
    file_paths: List[str],
    max_records_per_file: Optional[int] = None,
    max_workers: Optional[int] = None
) -> Iterator[dict]:
    """
    Parse multiple WET files in parallel.
    
    Args:
        file_paths: List of WET file paths
        max_records_per_file: Max records to extract per file
        max_workers: Number of parallel workers
        
    Yields:
        Company record dictionaries
    """
    args = [(path, max_records_per_file) for path in file_paths]
    
    if max_workers is None:
        max_workers = get_optimal_workers("cpu")
    
    ctx = mp.get_context('spawn')
    
    with ProcessPoolExecutor(max_workers=max_workers, mp_context=ctx) as executor:
        for records in executor.map(_parse_wet_file_worker, args):
            for record in records:
                yield record


def parse_abr_files_parallel(
    file_paths: List[str],
    max_records_per_file: Optional[int] = None,
    max_workers: Optional[int] = None
) -> Iterator[dict]:
    """
    Parse multiple ABR XML files in parallel.
    
    Args:
        file_paths: List of XML file paths
        max_records_per_file: Max records to extract per file
        max_workers: Number of parallel workers
        
    Yields:
        Entity record dictionaries
    """
    args = [(path, max_records_per_file) for path in file_paths]
    
    if max_workers is None:
        max_workers = get_optimal_workers("cpu")
    
    ctx = mp.get_context('spawn')
    
    with ProcessPoolExecutor(max_workers=max_workers, mp_context=ctx) as executor:
        for records in executor.map(_parse_abr_file_worker, args):
            for record in records:
                yield record
