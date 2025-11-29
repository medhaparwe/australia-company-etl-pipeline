"""
Utility functions for the Australia Company ETL Pipeline.
Includes text cleaning, normalization, and configuration loading.
"""

import re
import os
import yaml
from typing import Optional, Dict, Any
from urllib.parse import urlparse
from pathlib import Path


# Company name stopwords to remove during normalization
COMPANY_STOPWORDS = [
    "PTY", "LTD", "LIMITED", "PROPRIETARY", "AUSTRALIA", "AUSTRALIAN",
    "HOLDINGS", "GROUP", "SERVICES", "CORPORATION", "CORP", "INC", "CO",
    "THE", "AND", "&", "OF"
]


def load_config(config_path: str = "config/pipeline_config.yaml") -> Dict[str, Any]:
    """
    Load pipeline configuration from YAML file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Configuration dictionary
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    return config


def normalize_company_name(name: Optional[str]) -> str:
    """
    Normalize a company name for matching.
    
    Steps:
    1. Convert to uppercase
    2. Remove punctuation
    3. Remove stopwords (PTY, LTD, etc.)
    4. Collapse whitespace
    5. Strip leading/trailing spaces
    
    Args:
        name: Raw company name
        
    Returns:
        Normalized company name
        
    Example:
        >>> normalize_company_name("ACME Corporation Pty Ltd")
        'ACME'
        >>> normalize_company_name("ABC & Sons Australia Limited")
        'ABC SONS'
    """
    if not name:
        return ""
    
    # Convert to uppercase
    name = name.upper()
    
    # Remove punctuation (except spaces)
    name = re.sub(r'[^\w\s]', ' ', name)
    
    # Remove stopwords
    words = name.split()
    words = [w for w in words if w not in COMPANY_STOPWORDS]
    name = ' '.join(words)
    
    # Collapse multiple spaces
    name = re.sub(r'\s+', ' ', name)
    
    # Strip whitespace
    name = name.strip()
    
    return name


def clean_text(text: Optional[str]) -> str:
    """
    Clean raw text content.
    
    Args:
        text: Raw text (e.g., from webpage)
        
    Returns:
        Cleaned text
    """
    if not text:
        return ""
    
    # Remove HTML tags if any remain
    text = re.sub(r'<[^>]+>', ' ', text)
    
    # Remove URLs
    text = re.sub(r'http[s]?://\S+', ' ', text)
    
    # Remove email addresses
    text = re.sub(r'\S+@\S+', ' ', text)
    
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s.,!?-]', ' ', text)
    
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()


def extract_domain(url: Optional[str]) -> str:
    """
    Extract domain from a URL.
    
    Args:
        url: Full URL
        
    Returns:
        Domain name
        
    Example:
        >>> extract_domain("https://www.acme.com.au/about")
        'acme.com.au'
    """
    if not url:
        return ""
    
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # Remove 'www.' prefix
        if domain.startswith('www.'):
            domain = domain[4:]
        
        return domain.lower()
    except Exception:
        return ""


def is_australian_domain(url: Optional[str]) -> bool:
    """
    Check if a URL is an Australian domain (.au).
    
    Args:
        url: URL to check
        
    Returns:
        True if Australian domain
    """
    domain = extract_domain(url)
    return domain.endswith('.au')


def generate_blocking_key(name: str, key_length: int = 4) -> str:
    """
    Generate a blocking key for entity matching.
    
    Args:
        name: Normalized company name
        key_length: Length of the blocking key
        
    Returns:
        Blocking key (first N characters)
    """
    normalized = normalize_company_name(name)
    
    if len(normalized) < key_length:
        return normalized.lower()
    
    return normalized[:key_length].lower()


def extract_company_from_text(text: str) -> Optional[str]:
    """
    Extract company name from webpage text using patterns.
    
    Args:
        text: Raw webpage text
        
    Returns:
        Extracted company name or None
    """
    if not text:
        return None
    
    # Common patterns for Australian companies
    patterns = [
        r'([A-Z][A-Za-z0-9&,.\s]+(?:Pty|PTY)[\s.]*(?:Ltd|LTD|Limited))',
        r'([A-Z][A-Za-z0-9&,.\s]+(?:Ltd|LTD|Limited))',
        r'([A-Z][A-Za-z0-9&,.\s]+Australia(?:\s+(?:Pty|PTY))?(?:\s+(?:Ltd|LTD))?)',
        r'(?:About|Company|Welcome to)\s+([A-Z][A-Za-z0-9&,.\s]{3,50})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            name = match.group(1).strip()
            # Validate: should be reasonable length
            if 3 <= len(name) <= 100:
                return name
    
    return None


def extract_industry_from_text(text: str) -> Optional[str]:
    """
    Extract industry from webpage text.
    
    Args:
        text: Raw webpage text
        
    Returns:
        Extracted industry or None
    """
    if not text:
        return None
    
    # Look for explicit industry mentions
    patterns = [
        r'[Ii]ndustry[:\s]+([A-Za-z\s&]+)',
        r'[Ss]ector[:\s]+([A-Za-z\s&]+)',
        r'[Ss]pecializ(?:e|ing) in ([A-Za-z\s&]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            industry = match.group(1).strip()
            # Limit to reasonable length
            if 3 <= len(industry) <= 50:
                return industry
    
    return None


def format_abn(abn: Optional[str]) -> str:
    """
    Format ABN to standard format (XX XXX XXX XXX).
    
    Args:
        abn: Raw ABN string
        
    Returns:
        Formatted ABN
    """
    if not abn:
        return ""
    
    # Remove all non-digits
    digits = re.sub(r'\D', '', abn)
    
    # ABN should be 11 digits
    if len(digits) != 11:
        return digits
    
    # Format as XX XXX XXX XXX
    return f"{digits[:2]} {digits[2:5]} {digits[5:8]} {digits[8:]}"


def validate_abn(abn: Optional[str]) -> bool:
    """
    Validate an Australian Business Number using the checksum algorithm.
    
    Args:
        abn: ABN to validate
        
    Returns:
        True if valid ABN
    """
    if not abn:
        return False
    
    # Remove non-digits
    digits = re.sub(r'\D', '', abn)
    
    if len(digits) != 11:
        return False
    
    # ABN validation weights
    weights = [10, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19]
    
    # Subtract 1 from first digit
    numbers = [int(d) for d in digits]
    numbers[0] -= 1
    
    # Calculate weighted sum
    total = sum(n * w for n, w in zip(numbers, weights))
    
    # Valid if divisible by 89
    return total % 89 == 0


def get_db_connection_string(config: Dict[str, Any]) -> str:
    """
    Build PostgreSQL connection string from config.
    
    Args:
        config: Pipeline configuration
        
    Returns:
        Connection string
    """
    pg = config['postgres']
    return f"postgresql://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['database']}"

