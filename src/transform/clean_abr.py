"""
Clean and normalize ABR (Australian Business Register) data.
"""

import re
from typing import Dict, Any, Optional
import logging
import pandas as pd

from ..common.utils import normalize_company_name, validate_abn, format_abn

logger = logging.getLogger(__name__)


# Valid Australian states
VALID_STATES = {'NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT'}

# Entity type mapping
ENTITY_TYPE_MAP = {
    'PRV': 'Private Company',
    'PUB': 'Public Company',
    'IND': 'Individual/Sole Trader',
    'TRT': 'Trust',
    'PNR': 'Partnership',
    'SGE': 'State Government Entity',
    'CGE': 'Commonwealth Government Entity',
    'OIE': 'Other Incorporated Entity',
}


def clean_abr_data(
    df: pd.DataFrame,
    validate_abns: bool = True,
    active_only: bool = False
) -> pd.DataFrame:
    """
    Clean ABR data.
    
    Args:
        df: Raw DataFrame with columns: abn, entity_name, entity_type, 
            entity_status, state, postcode, start_date
        validate_abns: Validate ABN checksums
        active_only: Keep only active entities
        
    Returns:
        Cleaned DataFrame
    """
    logger.info(f"Cleaning ABR data: {len(df)} rows")
    
    if df.empty:
        return df
    
    df = df.copy()
    
    # Clean ABN
    if 'abn' in df.columns:
        df['abn'] = df['abn'].apply(clean_abn)
        
        if validate_abns:
            before = len(df)
            df = df[df['abn'].apply(validate_abn)]
            logger.info(f"Removed {before - len(df)} invalid ABNs")
    
    # Clean entity name
    if 'entity_name' in df.columns:
        df['entity_name'] = df['entity_name'].apply(clean_entity_name)
        df['normalized_name'] = df['entity_name'].apply(normalize_company_name)
        
        # Drop null names
        before = len(df)
        df = df.dropna(subset=['entity_name'])
        df = df[df['entity_name'].str.len() > 2]
        logger.info(f"Removed {before - len(df)} rows with invalid names")
    
    # Clean entity type
    if 'entity_type' in df.columns:
        df['entity_type_code'] = df['entity_type']
        df['entity_type'] = df['entity_type'].apply(clean_entity_type)
    
    # Clean entity status
    if 'entity_status' in df.columns:
        df['entity_status'] = df['entity_status'].apply(clean_status)
        
        if active_only:
            before = len(df)
            df = df[df['entity_status'] == 'Active']
            logger.info(f"Filtered to {len(df)} active entities (removed {before - len(df)})")
    
    # Clean state
    if 'state' in df.columns:
        df['state'] = df['state'].apply(clean_state)
    
    # Clean postcode
    if 'postcode' in df.columns:
        df['postcode'] = df['postcode'].apply(clean_postcode)
    
    # Clean start date
    if 'start_date' in df.columns:
        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    
    # Generate blocking key
    if 'normalized_name' in df.columns:
        df['block_key'] = df['normalized_name'].apply(
            lambda x: x[:4].lower() if x and len(x) >= 4 else ''
        )
    
    # Drop duplicates by ABN
    if 'abn' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['abn'], keep='first')
        logger.info(f"Removed {before - len(df)} duplicate ABNs")
    
    logger.info(f"Cleaned ABR data: {len(df)} rows remaining")
    
    return df


def clean_abn(abn: Optional[str]) -> Optional[str]:
    """Clean ABN to 11-digit string."""
    if not abn:
        return None
    
    # Remove non-digits
    digits = re.sub(r'\D', '', str(abn))
    
    # Should be 11 digits
    if len(digits) != 11:
        return None
    
    return digits


def clean_entity_name(name: Optional[str]) -> Optional[str]:
    """Clean entity name."""
    if not name:
        return None
    
    # Basic cleaning
    name = name.strip()
    
    # Remove excessive whitespace
    name = re.sub(r'\s+', ' ', name)
    
    # Remove trailing trustee info
    name = re.sub(r'\s+AS\s+TRUSTEE.*$', '', name, flags=re.IGNORECASE)
    
    # Convert to title case if all caps
    if name.isupper():
        # Keep some words uppercase
        words = name.split()
        result = []
        keep_upper = {'PTY', 'LTD', 'LIMITED', 'NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT', 'ABN', 'ACN'}
        for word in words:
            if word in keep_upper:
                result.append(word)
            else:
                result.append(word.title())
        name = ' '.join(result)
    
    return name[:200] if name else None


def clean_entity_type(entity_type: Optional[str]) -> Optional[str]:
    """Clean and expand entity type."""
    if not entity_type:
        return None
    
    entity_type = entity_type.strip().upper()
    
    return ENTITY_TYPE_MAP.get(entity_type, entity_type)


def clean_status(status: Optional[str]) -> Optional[str]:
    """Clean entity status."""
    if not status:
        return None
    
    status = status.strip().lower()
    
    if 'active' in status or 'registered' in status:
        return 'Active'
    elif 'cancel' in status or 'deregistered' in status:
        return 'Cancelled'
    else:
        return status.title()


def clean_state(state: Optional[str]) -> Optional[str]:
    """Clean state code."""
    if not state:
        return None
    
    state = state.strip().upper()
    
    # Map common variations
    state_map = {
        'NEW SOUTH WALES': 'NSW',
        'VICTORIA': 'VIC',
        'QUEENSLAND': 'QLD',
        'SOUTH AUSTRALIA': 'SA',
        'WESTERN AUSTRALIA': 'WA',
        'TASMANIA': 'TAS',
        'NORTHERN TERRITORY': 'NT',
        'AUSTRALIAN CAPITAL TERRITORY': 'ACT',
    }
    
    state = state_map.get(state, state)
    
    if state in VALID_STATES:
        return state
    
    return None


def clean_postcode(postcode: Optional[str]) -> Optional[str]:
    """Clean postcode."""
    if not postcode:
        return None
    
    # Extract 4-digit postcode
    match = re.search(r'\b(\d{4})\b', str(postcode))
    if match:
        return match.group(1)
    
    return None


def clean_abr_spark(df, spark_session):
    """
    Clean ABR data using PySpark.
    
    Args:
        df: Spark DataFrame
        spark_session: SparkSession
        
    Returns:
        Cleaned Spark DataFrame
    """
    from pyspark.sql.functions import udf, col, lower, when
    from pyspark.sql.types import StringType, BooleanType
    
    # Register UDFs
    normalize_udf = udf(normalize_company_name, StringType())
    clean_name_udf = udf(clean_entity_name, StringType())
    clean_abn_udf = udf(clean_abn, StringType())
    validate_abn_udf = udf(validate_abn, BooleanType())
    clean_state_udf = udf(clean_state, StringType())
    clean_postcode_udf = udf(clean_postcode, StringType())
    
    # Apply transformations
    cleaned = df \
        .withColumn("abn", clean_abn_udf(col("abn"))) \
        .withColumn("entity_name", clean_name_udf(col("entity_name"))) \
        .withColumn("normalized_name", normalize_udf(col("entity_name"))) \
        .withColumn("state", clean_state_udf(col("state"))) \
        .withColumn("postcode", clean_postcode_udf(col("postcode")))
    
    # Standardize status
    cleaned = cleaned.withColumn(
        "entity_status",
        when(lower(col("entity_status")).contains("active"), "Active")
        .when(lower(col("entity_status")).contains("cancel"), "Cancelled")
        .otherwise(col("entity_status"))
    )
    
    # Filter valid ABNs
    cleaned = cleaned.filter(validate_abn_udf(col("abn")))
    
    # Filter null names
    cleaned = cleaned.filter(col("entity_name").isNotNull())
    cleaned = cleaned.filter(col("normalized_name") != "")
    
    # Generate blocking key
    cleaned = cleaned.withColumn(
        "block_key",
        lower(col("normalized_name").substr(1, 4))
    )
    
    # Deduplicate by ABN
    cleaned = cleaned.dropDuplicates(["abn"])
    
    return cleaned


if __name__ == "__main__":
    # Example usage
    sample_data = pd.DataFrame({
        'abn': ['51824753556', '12345678901', 'invalid'],
        'entity_name': ['ACME CORPORATION PTY LTD', 'Test Company Limited', None],
        'entity_type': ['PRV', 'PUB', 'IND'],
        'entity_status': ['Active', 'Cancelled', 'Active'],
        'state': ['NSW', 'victoria', 'QLD'],
        'postcode': ['2000', '3000', '4000'],
        'start_date': ['2020-01-01', '2019-06-15', None]
    })
    
    cleaned = clean_abr_data(sample_data, validate_abns=False)
    print(cleaned)

