"""
Pytest configuration and fixtures for the Australia Company ETL Pipeline tests.
"""

import pytest
import sys
from pathlib import Path
import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def sample_cc_data():
    """Sample Common Crawl data for testing."""
    return pd.DataFrame({
        'url': [
            'https://www.acme.com.au/about',
            'https://techcorp.com.au/',
            'https://greenenergy.com.au/services'
        ],
        'company_name': [
            'ACME Corporation Pty Ltd',
            'TechCorp Australia',
            'Green Energy Partners Pty Ltd'
        ],
        'normalized_name': [
            'ACME',
            'TECHCORP',
            'GREEN ENERGY PARTNERS'
        ],
        'block_key': ['acme', 'tech', 'gree'],
        'industry': ['Manufacturing', 'Technology', 'Energy'],
        'raw_text': [
            'Welcome to ACME Corporation.',
            'TechCorp provides technology solutions.',
            'Green Energy Partners for sustainable energy.'
        ]
    })


@pytest.fixture
def sample_abr_data():
    """Sample ABR data for testing."""
    return pd.DataFrame({
        'abn': ['51824753556', '12345678901', '98765432109'],
        'entity_name': [
            'ACME Corporation Pty Ltd',
            'TechCorp Australia Pty Ltd',
            'Green Energy Partners Pty Ltd'
        ],
        'normalized_name': [
            'ACME',
            'TECHCORP',
            'GREEN ENERGY PARTNERS'
        ],
        'block_key': ['acme', 'tech', 'gree'],
        'entity_type': ['PRV', 'PRV', 'PRV'],
        'entity_status': ['Active', 'Active', 'Active'],
        'state': ['NSW', 'VIC', 'QLD'],
        'postcode': ['2000', '3000', '4000'],
        'start_date': ['2020-01-01', '2019-06-15', '2018-03-20']
    })


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create temporary data directories."""
    raw_dir = tmp_path / "data" / "raw"
    processed_dir = tmp_path / "data" / "processed"
    output_dir = tmp_path / "data" / "output"
    
    raw_dir.mkdir(parents=True)
    processed_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)
    
    return tmp_path

