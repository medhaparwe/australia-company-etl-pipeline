"""
Upsert logic for entity matching results and unified companies.
"""

import logging
from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


def upsert_matches(
    matches: List[Dict[str, Any]],
    loader,
    min_score: float = 0.75
) -> int:
    """
    Upsert match results with deduplication and filtering.
    
    Args:
        matches: List of match dictionaries
        loader: PostgresLoader instance
        min_score: Minimum score threshold
        
    Returns:
        Number of records upserted
    """
    if not matches:
        return 0
    
    # Convert to DataFrame
    df = pd.DataFrame(matches)
    
    # Filter by minimum score
    if 'final_score' in df.columns:
        df = df[df['final_score'] >= min_score]
    
    if df.empty:
        return 0
    
    # Deduplicate - keep best match per crawl_url
    if 'crawl_url' in df.columns and 'final_score' in df.columns:
        df = df.sort_values('final_score', ascending=False)
        df = df.drop_duplicates(subset=['crawl_url'], keep='first')
    
    # Load to database
    return loader.load_match_results(df)


def upsert_unified_companies(
    matches: List[Dict[str, Any]],
    abr_df: pd.DataFrame,
    loader,
    min_score: float = 0.75
) -> int:
    """
    Create and upsert unified company records from matches.
    
    Merges information from both sources to create golden records.
    
    Args:
        matches: List of match dictionaries
        abr_df: ABR entities DataFrame
        loader: PostgresLoader instance
        min_score: Minimum score threshold
        
    Returns:
        Number of unified records created
    """
    if not matches:
        return 0
    
    # Convert matches to DataFrame
    match_df = pd.DataFrame(matches)
    
    # Filter by score
    if 'final_score' in match_df.columns:
        match_df = match_df[match_df['final_score'] >= min_score]
    
    if match_df.empty:
        return 0
    
    # Merge with ABR data
    unified_records = []
    
    for _, row in match_df.iterrows():
        abn = row.get('abn')
        
        # Get ABR record
        abr_record = abr_df[abr_df['abn'] == abn]
        
        if abr_record.empty:
            # Use match data only
            unified = create_unified_record_from_match(row)
        else:
            # Merge match and ABR data
            unified = create_unified_record_merged(row, abr_record.iloc[0])
        
        if unified:
            unified_records.append(unified)
    
    if not unified_records:
        return 0
    
    # Load to database
    df = pd.DataFrame(unified_records)
    return loader.load_unified_companies(df)


def create_unified_record_from_match(match: pd.Series) -> Dict[str, Any]:
    """
    Create unified record from match data only.
    
    Args:
        match: Match record Series
        
    Returns:
        Unified record dictionary
    """
    return {
        'abn': match.get('abn'),
        'canonical_name': match.get('abr_name') or match.get('crawl_name'),
        'trading_name': match.get('crawl_name'),
        'url': match.get('crawl_url'),
        'domain': extract_domain_simple(match.get('crawl_url', '')),
        'industry': None,
        'entity_type': None,
        'entity_status': 'Active',
        'state': match.get('state'),
        'postcode': match.get('postcode'),
        'start_date': match.get('start_date'),
        'source': 'MATCHED',
        'confidence_score': match.get('final_score', 0.0)
    }


def create_unified_record_merged(
    match: pd.Series,
    abr: pd.Series
) -> Dict[str, Any]:
    """
    Create unified record by merging match and ABR data.
    
    Priority: ABR for official fields, CC for website/trading name
    
    Args:
        match: Match record Series
        abr: ABR record Series
        
    Returns:
        Unified record dictionary
    """
    return {
        'abn': abr.get('abn'),
        'canonical_name': abr.get('entity_name'),
        'trading_name': match.get('crawl_name'),
        'url': match.get('crawl_url'),
        'domain': extract_domain_simple(match.get('crawl_url', '')),
        'industry': match.get('industry'),
        'entity_type': abr.get('entity_type'),
        'entity_status': abr.get('entity_status', 'Active'),
        'state': abr.get('state'),
        'postcode': abr.get('postcode'),
        'start_date': abr.get('start_date'),
        'source': 'MERGED',
        'confidence_score': match.get('final_score', 0.0)
    }


def extract_domain_simple(url: str) -> str:
    """Extract domain from URL (simple version)."""
    if not url:
        return ''
    
    # Remove protocol
    domain = url.replace('https://', '').replace('http://', '')
    
    # Remove path
    domain = domain.split('/')[0]
    
    # Remove www
    if domain.startswith('www.'):
        domain = domain[4:]
    
    return domain.lower()


def create_unified_from_abr_only(
    abr_df: pd.DataFrame,
    loader,
    exclude_abns: Optional[List[str]] = None
) -> int:
    """
    Create unified records from ABR entities without matches.
    
    Args:
        abr_df: ABR entities DataFrame
        loader: PostgresLoader instance
        exclude_abns: ABNs to exclude (already matched)
        
    Returns:
        Number of records created
    """
    if abr_df.empty:
        return 0
    
    # Exclude already matched ABNs
    if exclude_abns:
        abr_df = abr_df[~abr_df['abn'].isin(exclude_abns)]
    
    if abr_df.empty:
        return 0
    
    unified_records = []
    
    for _, row in abr_df.iterrows():
        unified = {
            'abn': row.get('abn'),
            'canonical_name': row.get('entity_name'),
            'trading_name': None,
            'url': None,
            'domain': None,
            'industry': None,
            'entity_type': row.get('entity_type'),
            'entity_status': row.get('entity_status', 'Active'),
            'state': row.get('state'),
            'postcode': row.get('postcode'),
            'start_date': row.get('start_date'),
            'source': 'ABR',
            'confidence_score': 1.0  # Official data
        }
        unified_records.append(unified)
    
    df = pd.DataFrame(unified_records)
    return loader.load_unified_companies(df)


def verify_match(
    loader,
    match_id: int,
    is_correct: bool,
    verified_by: str
) -> bool:
    """
    Manually verify a match result.
    
    Args:
        loader: PostgresLoader instance
        match_id: ID of match to verify
        is_correct: Whether match is correct
        verified_by: Who verified
        
    Returns:
        Success status
    """
    conn = loader.get_connection()
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE entity_match_results
                SET is_verified = %s,
                    verified_by = %s,
                    verified_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (is_correct, verified_by, match_id))
        
        conn.commit()
        return True
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error verifying match: {e}")
        return False


def get_match_statistics(loader) -> Dict[str, Any]:
    """
    Get statistics about matches.
    
    Args:
        loader: PostgresLoader instance
        
    Returns:
        Statistics dictionary
    """
    stats = {}
    
    # Total matches
    result = loader.execute_query("""
        SELECT 
            COUNT(*) as total,
            AVG(final_score) as avg_score,
            COUNT(CASE WHEN final_score >= 0.90 THEN 1 END) as high_confidence,
            COUNT(CASE WHEN is_verified THEN 1 END) as verified
        FROM entity_match_results
    """)
    
    if result:
        stats['matches'] = result[0]
    
    # By method
    result = loader.execute_query("""
        SELECT 
            match_method,
            COUNT(*) as count,
            AVG(final_score) as avg_score
        FROM entity_match_results
        GROUP BY match_method
    """)
    
    if result:
        stats['by_method'] = {r['match_method']: r for r in result}
    
    # By state
    result = loader.execute_query("""
        SELECT 
            state,
            COUNT(*) as count
        FROM entity_match_results
        WHERE state IS NOT NULL
        GROUP BY state
        ORDER BY count DESC
    """)
    
    if result:
        stats['by_state'] = {r['state']: r['count'] for r in result}
    
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Example usage
    from .load_postgres import PostgresLoader
    
    loader = PostgresLoader()
    
    try:
        stats = get_match_statistics(loader)
        print("Match statistics:", stats)
    finally:
        loader.close()

