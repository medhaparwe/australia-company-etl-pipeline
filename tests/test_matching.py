"""
Tests for entity matching modules.
"""

import pytest
import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.transform.entity_match import EntityMatcher, match_companies
from src.transform.feature_engineering import FeatureEngineer, compute_match_probability


class TestEntityMatcher:
    """Tests for EntityMatcher class."""
    
    @pytest.fixture
    def matcher(self):
        """Create a matcher instance."""
        return EntityMatcher(
            fuzzy_threshold=0.75,
            use_llm=False  # Disable LLM for testing
        )
    
    @pytest.fixture
    def sample_cc_data(self):
        """Sample Common Crawl data."""
        return pd.DataFrame({
            'company_name': ['ACME Corporation', 'Tech Solutions'],
            'normalized_name': ['ACME', 'TECH SOLUTIONS'],
            'block_key': ['acme', 'tech'],
            'url': ['https://acme.com.au', 'https://techsol.com.au']
        })
    
    @pytest.fixture
    def sample_abr_data(self):
        """Sample ABR data."""
        return pd.DataFrame({
            'entity_name': ['ACME Corp Pty Ltd', 'Tech Solutions Australia Pty Ltd'],
            'normalized_name': ['ACME', 'TECH SOLUTIONS'],
            'block_key': ['acme', 'tech'],
            'abn': ['12345678901', '98765432109'],
            'state': ['NSW', 'VIC'],
            'postcode': ['2000', '3000'],
            'start_date': ['2020-01-01', '2019-06-15']
        })
    
    def test_matcher_initialization(self, matcher):
        assert matcher.fuzzy_threshold == 0.75
        assert matcher.use_llm == False
    
    def test_exact_name_match(self, matcher, sample_cc_data, sample_abr_data):
        """Test matching when names are identical."""
        matches = matcher.match(sample_cc_data, sample_abr_data)
        
        assert len(matches) == 2
        assert all(m.final_score >= 0.75 for m in matches)
    
    def test_no_match_different_blocks(self, matcher):
        """Test that different block keys don't match."""
        cc_data = pd.DataFrame({
            'company_name': ['Alpha Corp'],
            'normalized_name': ['ALPHA'],
            'block_key': ['alph'],
            'url': ['https://alpha.com.au']
        })
        
        abr_data = pd.DataFrame({
            'entity_name': ['Beta Ltd'],
            'normalized_name': ['BETA'],
            'block_key': ['beta'],
            'abn': ['11111111111'],
            'state': ['NSW'],
            'postcode': ['2000'],
            'start_date': ['2020-01-01']
        })
        
        matches = matcher.match(cc_data, abr_data)
        assert len(matches) == 0


class TestMatchCompanies:
    """Tests for match_companies convenience function."""
    
    def test_returns_dataframe(self):
        cc_data = pd.DataFrame({
            'company_name': ['Test Company'],
            'normalized_name': ['TEST COMPANY'],
            'block_key': ['test'],
            'url': ['https://test.com.au']
        })
        
        abr_data = pd.DataFrame({
            'entity_name': ['Test Company Pty Ltd'],
            'normalized_name': ['TEST COMPANY'],
            'block_key': ['test'],
            'abn': ['12345678901'],
            'state': ['NSW'],
            'postcode': ['2000'],
            'start_date': ['2020-01-01']
        })
        
        result = match_companies(cc_data, abr_data, use_llm=False)
        
        assert isinstance(result, pd.DataFrame)
    
    def test_empty_input_returns_empty(self):
        result = match_companies(pd.DataFrame(), pd.DataFrame(), use_llm=False)
        assert result.empty


class TestFeatureEngineer:
    """Tests for FeatureEngineer class."""
    
    @pytest.fixture
    def engineer(self):
        return FeatureEngineer()
    
    def test_name_features_exact_match(self, engineer):
        features = engineer.generate_features(
            {'company_name': 'ACME Corp'},
            {'entity_name': 'ACME Corp'}
        )
        
        assert 'name_exact_match' in features
        assert features['name_exact_match'] == 1.0
    
    def test_name_features_different(self, engineer):
        features = engineer.generate_features(
            {'company_name': 'Alpha Inc'},
            {'entity_name': 'Beta Ltd'}
        )
        
        assert features['name_exact_match'] == 0.0
    
    def test_domain_features(self, engineer):
        features = engineer.generate_features(
            {'url': 'https://acme.com.au', 'company_name': 'ACME'},
            {'entity_name': 'ACME Pty Ltd'}
        )
        
        assert 'domain_name_match' in features
        assert 'domain_contains_name' in features


class TestComputeMatchProbability:
    """Tests for match probability computation."""
    
    def test_high_score_features(self):
        features = {
            'name_exact_match': 1.0,
            'name_token_sort': 0.95,
            'name_token_set': 0.95,
            'domain_name_match': 0.9,
            'domain_contains_name': 1.0,
            'state_mentioned': 1.0,
            'postcode_mentioned': 1.0
        }
        
        probability = compute_match_probability(features)
        assert probability >= 0.9
    
    def test_low_score_features(self):
        features = {
            'name_exact_match': 0.0,
            'name_token_sort': 0.2,
            'name_token_set': 0.2,
            'domain_name_match': 0.0,
            'domain_contains_name': 0.0,
            'state_mentioned': 0.0,
            'postcode_mentioned': 0.0
        }
        
        probability = compute_match_probability(features)
        assert probability <= 0.2
    
    def test_empty_features(self):
        probability = compute_match_probability({})
        assert probability == 0.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

