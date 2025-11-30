"""
Entity Matching between Common Crawl and ABR data.

Uses a hybrid approach combining:
1. Blocking - Reduce comparison space
2. Fuzzy matching - String similarity
3. LLM verification - Semantic matching for edge cases
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
import pandas as pd

try:
    from rapidfuzz import fuzz
except ImportError:
    fuzz = None

from ..common.utils import normalize_company_name, generate_blocking_key
from ..common.llm_matcher import LLMMatcher

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Result of entity matching."""
    crawl_name: str
    crawl_url: str
    abr_name: str
    abn: str
    fuzzy_score: float
    llm_score: Optional[float]
    final_score: float
    match_method: str  # "fuzzy", "llm", "hybrid"
    state: Optional[str] = None
    postcode: Optional[str] = None
    start_date: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class EntityMatcher:
    """
    Entity matcher for Common Crawl and ABR data.
    
    Implements a three-stage matching pipeline:
    1. Blocking - Group by first N characters of normalized name
    2. Fuzzy matching - Compute string similarity scores
    3. LLM verification - Use GPT for uncertain matches
    """
    
    def __init__(
        self,
        fuzzy_threshold: float = 0.75,
        llm_threshold_min: float = 0.60,
        llm_threshold_max: float = 0.85,
        use_llm: bool = True,
        llm_model: str = "gpt-4o-mini",
        fuzzy_weight: float = 0.70,
        llm_weight: float = 0.30
    ):
        """
        Initialize the entity matcher.
        
        Args:
            fuzzy_threshold: Minimum fuzzy score for a match
            llm_threshold_min: Minimum fuzzy score to trigger LLM
            llm_threshold_max: Maximum fuzzy score to skip LLM (already confident)
            use_llm: Whether to use LLM verification
            llm_model: OpenAI model to use
            fuzzy_weight: Weight for fuzzy score in hybrid
            llm_weight: Weight for LLM score in hybrid
        """
        self.fuzzy_threshold = fuzzy_threshold
        self.llm_threshold_min = llm_threshold_min
        self.llm_threshold_max = llm_threshold_max
        self.use_llm = use_llm
        self.fuzzy_weight = fuzzy_weight
        self.llm_weight = llm_weight
        
        if use_llm:
            self.llm_matcher = LLMMatcher(model=llm_model)
        else:
            self.llm_matcher = None
    
    def match(
        self,
        crawl_df: pd.DataFrame,
        abr_df: pd.DataFrame,
        max_matches: Optional[int] = None
    ) -> List[MatchResult]:
        """
        Match Common Crawl companies with ABR entities.
        
        Args:
            crawl_df: Common Crawl DataFrame with columns:
                - company_name, normalized_name, block_key, url
            abr_df: ABR DataFrame with columns:
                - entity_name, normalized_name, block_key, abn, state, postcode
            max_matches: Maximum matches to return
            
        Returns:
            List of MatchResult objects
        """
        logger.info(f"Matching {len(crawl_df)} CC records with {len(abr_df)} ABR records")
        
        matches = []
        
        # Group ABR by block key for efficient lookup
        abr_blocks = abr_df.groupby('block_key')
        
        processed = 0
        for _, cc_row in crawl_df.iterrows():
            if max_matches and len(matches) >= max_matches:
                break
            
            block_key = cc_row.get('block_key', '')
            if not block_key:
                continue
            
            # Get candidate ABR records in same block
            try:
                candidates = abr_blocks.get_group(block_key)
            except KeyError:
                # No matching block
                continue
            
            # Find best match
            best_match = self._find_best_match(cc_row, candidates)
            if best_match:
                matches.append(best_match)
            
            processed += 1
            if processed % 1000 == 0:
                logger.info(f"Processed {processed} records, found {len(matches)} matches")
        
        logger.info(f"Found {len(matches)} total matches")
        return matches
    
    def _find_best_match(
        self,
        cc_row: pd.Series,
        abr_candidates: pd.DataFrame
    ) -> Optional[MatchResult]:
        """Find the best matching ABR record for a CC record."""
        best_result = None
        best_score = 0.0
        
        cc_name = cc_row.get('normalized_name', '')
        if not cc_name:
            return None
        
        for _, abr_row in abr_candidates.iterrows():
            abr_name = abr_row.get('normalized_name', '')
            if not abr_name:
                continue
            
            # Compute fuzzy score
            fuzzy_score = self._compute_fuzzy_score(cc_name, abr_name)
            
            # Skip if below minimum threshold
            if fuzzy_score < self.llm_threshold_min:
                continue
            
            # Determine final score
            llm_score = None
            match_method = "fuzzy"
            
            if (self.use_llm and self.llm_matcher and 
                self.llm_threshold_min <= fuzzy_score <= self.llm_threshold_max):
                # Use LLM for uncertain matches
                llm_result = self.llm_matcher.match_companies(
                    {
                        'name': cc_row.get('company_name', ''),
                        'url': cc_row.get('url', ''),
                        'industry': cc_row.get('industry', '')
                    },
                    {
                        'entity_name': abr_row.get('entity_name', ''),
                        'abn': abr_row.get('abn', ''),
                        'state': abr_row.get('state', ''),
                        'postcode': abr_row.get('postcode', '')
                    }
                )
                llm_score = llm_result.score
                match_method = "hybrid"
            elif fuzzy_score > self.llm_threshold_max:
                # High confidence fuzzy match, no LLM needed
                match_method = "fuzzy"
            
            # Compute final score
            if llm_score is not None:
                final_score = (
                    self.fuzzy_weight * fuzzy_score + 
                    self.llm_weight * llm_score
                )
            else:
                final_score = fuzzy_score
            
            # Check if this is the best match
            if final_score > best_score and final_score >= self.fuzzy_threshold:
                best_score = final_score
                best_result = MatchResult(
                    crawl_name=cc_row.get('company_name', ''),
                    crawl_url=cc_row.get('url', ''),
                    abr_name=abr_row.get('entity_name', ''),
                    abn=abr_row.get('abn', ''),
                    fuzzy_score=fuzzy_score,
                    llm_score=llm_score,
                    final_score=final_score,
                    match_method=match_method,
                    state=abr_row.get('state'),
                    postcode=abr_row.get('postcode'),
                    start_date=str(abr_row.get('start_date', ''))
                )
        
        return best_result
    
    def _compute_fuzzy_score(self, name1: str, name2: str) -> float:
        """Compute fuzzy string similarity score."""
        if not name1 or not name2:
            return 0.0
        
        if fuzz is not None:
            # Use RapidFuzz
            score = fuzz.token_sort_ratio(name1, name2) / 100.0
        else:
            # Fallback to simple matching
            score = self._simple_similarity(name1, name2)
        
        return score
    
    def _simple_similarity(self, s1: str, s2: str) -> float:
        """Simple Jaccard similarity fallback."""
        if not s1 or not s2:
            return 0.0
        
        set1 = set(s1.lower().split())
        set2 = set(s2.lower().split())
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        if union == 0:
            return 0.0
        
        return intersection / union


def _match_block_worker(args: tuple) -> List[Dict[str, Any]]:
    """
    Worker function for parallel block matching.
    
    Args:
        args: Tuple of (block_key, cc_rows, abr_rows, fuzzy_threshold)
        
    Returns:
        List of match result dictionaries
    """
    block_key, cc_rows, abr_rows, fuzzy_threshold = args
    
    # Import fuzz inside worker to avoid pickling issues
    try:
        from rapidfuzz import fuzz
        use_rapidfuzz = True
    except ImportError:
        use_rapidfuzz = False
    
    def compute_fuzzy(name1: str, name2: str) -> float:
        if not name1 or not name2:
            return 0.0
        if use_rapidfuzz:
            return fuzz.token_sort_ratio(name1, name2) / 100.0
        # Jaccard fallback
        set1 = set(name1.lower().split())
        set2 = set(name2.lower().split())
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0
    
    matches = []
    
    for cc_row in cc_rows:
        cc_name = cc_row.get('normalized_name', '')
        if not cc_name:
            continue
        
        best_match = None
        best_score = 0.0
        
        for abr_row in abr_rows:
            abr_name = abr_row.get('normalized_name', '')
            if not abr_name:
                continue
            
            fuzzy_score = compute_fuzzy(cc_name, abr_name)
            
            if fuzzy_score >= fuzzy_threshold and fuzzy_score > best_score:
                best_score = fuzzy_score
                best_match = {
                    'crawl_name': cc_row.get('company_name', ''),
                    'crawl_url': cc_row.get('url', ''),
                    'abr_name': abr_row.get('entity_name', ''),
                    'abn': abr_row.get('abn', ''),
                    'fuzzy_score': fuzzy_score,
                    'llm_score': None,
                    'final_score': fuzzy_score,
                    'match_method': 'fuzzy',
                    'state': abr_row.get('state'),
                    'postcode': abr_row.get('postcode'),
                    'start_date': str(abr_row.get('start_date', ''))
                }
        
        if best_match:
            matches.append(best_match)
    
    return matches


def match_companies_parallel(
    crawl_df: pd.DataFrame,
    abr_df: pd.DataFrame,
    fuzzy_threshold: float = 0.75,
    max_workers: Optional[int] = None
) -> pd.DataFrame:
    """
    Match companies using parallel processing (multiprocessing).
    
    Much faster than sequential iterrows() for large datasets.
    Processes each block in parallel using ProcessPoolExecutor.
    
    Args:
        crawl_df: Common Crawl DataFrame with block_key, normalized_name
        abr_df: ABR DataFrame with block_key, normalized_name, abn
        fuzzy_threshold: Minimum match score
        max_workers: Number of parallel workers (auto-detected if None)
        
    Returns:
        DataFrame of matched records
    """
    import multiprocessing as mp
    from concurrent.futures import ProcessPoolExecutor
    
    if max_workers is None:
        max_workers = mp.cpu_count()
    
    logger.info(f"Matching {len(crawl_df)} CC records with {len(abr_df)} ABR records using {max_workers} workers")
    
    # Group by block key
    cc_blocks = crawl_df.groupby('block_key')
    abr_blocks = abr_df.groupby('block_key')
    
    # Prepare tasks for parallel processing
    tasks = []
    for block_key, cc_group in cc_blocks:
        try:
            abr_group = abr_blocks.get_group(block_key)
        except KeyError:
            continue
        
        # Convert to list of dicts for pickling
        cc_rows = cc_group.to_dict('records')
        abr_rows = abr_group.to_dict('records')
        
        tasks.append((block_key, cc_rows, abr_rows, fuzzy_threshold))
    
    logger.info(f"Processing {len(tasks)} blocks in parallel")
    
    # Process blocks in parallel
    all_matches = []
    ctx = mp.get_context('spawn')
    
    with ProcessPoolExecutor(max_workers=max_workers, mp_context=ctx) as executor:
        for block_matches in executor.map(_match_block_worker, tasks):
            all_matches.extend(block_matches)
    
    logger.info(f"Found {len(all_matches)} total matches")
    
    if not all_matches:
        return pd.DataFrame()
    
    return pd.DataFrame(all_matches)


def match_companies(
    crawl_df: pd.DataFrame,
    abr_df: pd.DataFrame,
    fuzzy_threshold: float = 0.75,
    use_llm: bool = False,
    parallel: bool = True,
    max_workers: Optional[int] = None
) -> pd.DataFrame:
    """
    Match companies and return DataFrame.
    
    For large datasets (>10K records), uses parallel processing automatically.
    
    Args:
        crawl_df: Common Crawl DataFrame
        abr_df: ABR DataFrame
        fuzzy_threshold: Minimum match score
        use_llm: Whether to use LLM verification (disables parallel)
        parallel: Enable parallel processing (default: True)
        max_workers: Number of parallel workers (auto-detected if None)
        
    Returns:
        DataFrame of matched records
    """
    # Use parallel processing for large datasets when LLM is not needed
    PARALLEL_THRESHOLD = 1000  # Use parallel if > 1000 CC records
    
    if parallel and not use_llm and len(crawl_df) > PARALLEL_THRESHOLD:
        logger.info("Using parallel matching for large dataset")
        return match_companies_parallel(
            crawl_df, abr_df,
            fuzzy_threshold=fuzzy_threshold,
            max_workers=max_workers
        )
    
    # Fall back to sequential matching (supports LLM)
    matcher = EntityMatcher(
        fuzzy_threshold=fuzzy_threshold,
        use_llm=use_llm
    )
    
    matches = matcher.match(crawl_df, abr_df)
    
    if not matches:
        return pd.DataFrame()
    
    return pd.DataFrame([m.to_dict() for m in matches])


def match_companies_spark(
    crawl_df,
    abr_df,
    spark_session,
    fuzzy_threshold: float = 0.75
):
    """
    Match companies using PySpark for distributed processing.
    
    Args:
        crawl_df: Spark DataFrame from Common Crawl
        abr_df: Spark DataFrame from ABR
        spark_session: SparkSession
        fuzzy_threshold: Minimum match score
        
    Returns:
        Spark DataFrame of matches
    """
    from pyspark.sql.functions import udf, col, broadcast
    from pyspark.sql.types import FloatType
    
    # Register fuzzy matching UDF
    # Import must happen inside UDF to work in worker processes
    @udf(FloatType())
    def fuzzy_score_udf(name1, name2):
        if not name1 or not name2:
            return 0.0
        try:
            # Try to import rapidfuzz in worker process
            try:
                from rapidfuzz import fuzz as rapidfuzz_module
                return float(rapidfuzz_module.token_sort_ratio(name1, name2)) / 100.0
            except ImportError:
                # Fallback to Jaccard similarity if rapidfuzz not available
                set1 = set(name1.lower().split())
                set2 = set(name2.lower().split())
                intersection = len(set1 & set2)
                union = len(set1 | set2)
                return float(intersection / union) if union > 0 else 0.0
        except Exception:
            # Return 0.0 on any error to prevent worker crashes
            return 0.0
    
    # Join on block key
    matched = crawl_df.alias("cc").join(
        broadcast(abr_df.alias("abr")),
        col("cc.block_key") == col("abr.block_key"),
        "inner"
    )
    
    # Compute fuzzy scores
    matched = matched.withColumn(
        "fuzzy_score",
        fuzzy_score_udf(col("cc.normalized_name"), col("abr.normalized_name"))
    )
    
    # Filter by threshold
    matched = matched.filter(col("fuzzy_score") >= fuzzy_threshold)
    
    # Select relevant columns
    result = matched.select(
        col("cc.company_name").alias("crawl_name"),
        col("cc.url").alias("crawl_url"),
        col("abr.entity_name").alias("abr_name"),
        col("abr.abn"),
        col("fuzzy_score"),
        col("abr.state"),
        col("abr.postcode"),
        col("abr.start_date")
    )
    
    return result


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Sample data
    crawl_data = pd.DataFrame({
        'company_name': ['ACME Corp', 'Tech Solutions Australia'],
        'normalized_name': ['ACME', 'TECH SOLUTIONS'],
        'block_key': ['acme', 'tech'],
        'url': ['https://acme.com.au', 'https://techsol.com.au']
    })
    
    abr_data = pd.DataFrame({
        'entity_name': ['ACME Corporation Pty Ltd', 'Tech Solutions Pty Ltd'],
        'normalized_name': ['ACME', 'TECH SOLUTIONS'],
        'block_key': ['acme', 'tech'],
        'abn': ['12345678901', '98765432109'],
        'state': ['NSW', 'VIC'],
        'postcode': ['2000', '3000'],
        'start_date': ['2020-01-01', '2019-06-15']
    })
    
    matches = match_companies(crawl_data, abr_data, use_llm=False)
    print(matches)

