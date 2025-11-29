"""
Clean and normalize Common Crawl extracted data.
"""

import re
from typing import Dict, Any, List, Optional
import logging
import pandas as pd

from ..common.utils import normalize_company_name, clean_text, extract_domain

logger = logging.getLogger(__name__)


def clean_commoncrawl_data(
    df: pd.DataFrame,
    drop_nulls: bool = True,
    deduplicate: bool = True
) -> pd.DataFrame:
    """
    Clean Common Crawl extracted data.
    
    Args:
        df: Raw DataFrame with columns: url, company_name, industry, raw_text
        drop_nulls: Drop rows with null company names
        deduplicate: Remove duplicate company names
        
    Returns:
        Cleaned DataFrame
    """
    logger.info(f"Cleaning Common Crawl data: {len(df)} rows")
    
    if df.empty:
        return df
    
    # Make a copy to avoid modifying original
    df = df.copy()
    
    # Clean company names
    if 'company_name' in df.columns:
        df['company_name'] = df['company_name'].apply(clean_company_name)
        df['normalized_name'] = df['company_name'].apply(normalize_company_name)
    
    # Clean URLs
    if 'url' in df.columns:
        df['domain'] = df['url'].apply(extract_domain)
    
    # Clean industry
    if 'industry' in df.columns:
        df['industry'] = df['industry'].apply(clean_industry)
    
    # Clean raw text
    if 'raw_text' in df.columns:
        df['raw_text'] = df['raw_text'].apply(lambda x: clean_text(x)[:5000] if x else '')
    
    # Drop nulls
    if drop_nulls and 'company_name' in df.columns:
        before = len(df)
        df = df.dropna(subset=['company_name'])
        df = df[df['company_name'].str.len() > 2]
        logger.info(f"Dropped {before - len(df)} rows with null/short company names")
    
    # Deduplicate
    if deduplicate and 'normalized_name' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['normalized_name'], keep='first')
        logger.info(f"Removed {before - len(df)} duplicate records")
    
    # Generate blocking key
    if 'normalized_name' in df.columns:
        df['block_key'] = df['normalized_name'].apply(
            lambda x: x[:4].lower() if x and len(x) >= 4 else ''
        )
    
    logger.info(f"Cleaned data: {len(df)} rows remaining")
    
    return df


def clean_company_name(name: Optional[str]) -> Optional[str]:
    """
    Clean a single company name.
    
    Args:
        name: Raw company name
        
    Returns:
        Cleaned company name
    """
    if not name:
        return None
    
    # Basic cleaning
    name = name.strip()
    
    # Remove excessive whitespace
    name = re.sub(r'\s+', ' ', name)
    
    # Remove common prefixes/suffixes that are just noise
    noise_patterns = [
        r'^welcome to\s+',
        r'^about\s+',
        r'^home\s*[-–]\s*',
        r'\s*[-–]\s*home$',
        r'\s*\|\s*official.*$',
    ]
    
    for pattern in noise_patterns:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    
    # Validate length
    if len(name) < 2 or len(name) > 200:
        return None
    
    return name.strip()


def clean_industry(industry: Optional[str]) -> Optional[str]:
    """
    Clean and standardize industry classification.
    
    Args:
        industry: Raw industry text
        
    Returns:
        Cleaned industry
    """
    if not industry:
        return None
    
    # Basic cleaning
    industry = industry.strip()
    
    # Remove common noise
    industry = re.sub(r'^(industry|sector|services?)[\s:]+', '', industry, flags=re.IGNORECASE)
    
    # Standardize common industries
    industry_mapping = {
        r'it|tech|software|digital': 'Information Technology',
        r'finance|bank|accounting': 'Financial Services',
        r'health|medical|pharma': 'Healthcare',
        r'retail|shop|store': 'Retail',
        r'construction|building': 'Construction',
        r'mining|resources': 'Mining & Resources',
        r'manufact': 'Manufacturing',
        r'transport|logistics': 'Transport & Logistics',
        r'education|training|school': 'Education',
        r'legal|law': 'Legal Services',
        r'real estate|property': 'Real Estate',
        r'agricult|farm': 'Agriculture',
        r'energy|power|electricity': 'Energy',
        r'telecom': 'Telecommunications',
        r'hospitality|hotel|restaurant': 'Hospitality',
    }
    
    industry_lower = industry.lower()
    for pattern, standard in industry_mapping.items():
        if re.search(pattern, industry_lower):
            return standard
    
    # Return original if no mapping found (title case)
    return industry.title()[:50]


def clean_commoncrawl_spark(df, spark_session):
    """
    Clean Common Crawl data using PySpark.
    
    Args:
        df: Spark DataFrame
        spark_session: SparkSession
        
    Returns:
        Cleaned Spark DataFrame
    """
    from pyspark.sql.functions import udf, col, lower, trim, regexp_replace
    from pyspark.sql.types import StringType
    
    # Register UDFs
    normalize_udf = udf(normalize_company_name, StringType())
    clean_name_udf = udf(clean_company_name, StringType())
    clean_industry_udf = udf(clean_industry, StringType())
    extract_domain_udf = udf(extract_domain, StringType())
    
    # Apply transformations
    cleaned = df \
        .withColumn("company_name", clean_name_udf(col("company_name"))) \
        .withColumn("normalized_name", normalize_udf(col("company_name"))) \
        .withColumn("domain", extract_domain_udf(col("url"))) \
        .withColumn("industry", clean_industry_udf(col("industry")))
    
    # Filter nulls
    cleaned = cleaned.filter(col("company_name").isNotNull())
    cleaned = cleaned.filter(col("normalized_name") != "")
    
    # Generate blocking key
    cleaned = cleaned.withColumn(
        "block_key",
        lower(col("normalized_name").substr(1, 4))
    )
    
    # Deduplicate
    cleaned = cleaned.dropDuplicates(["normalized_name"])
    
    return cleaned


if __name__ == "__main__":
    # Example usage
    import pandas as pd
    
    sample_data = pd.DataFrame({
        'url': [
            'https://www.acme.com.au/about',
            'https://www.techcorp.com.au/',
            None
        ],
        'company_name': [
            'ACME Corporation Pty Ltd',
            'Welcome to TechCorp Australia',
            None
        ],
        'industry': [
            'Information Technology Services',
            'tech and software',
            None
        ],
        'raw_text': [
            'Some sample text...',
            'More sample text...',
            None
        ]
    })
    
    cleaned = clean_commoncrawl_data(sample_data)
    print(cleaned)

