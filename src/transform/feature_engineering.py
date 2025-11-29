"""
Feature Engineering for entity matching.

Generates additional features to improve match accuracy.
"""

import re
from typing import Dict, Any, List, Optional
import logging
import pandas as pd

try:
    from rapidfuzz import fuzz
except ImportError:
    fuzz = None

from ..common.utils import normalize_company_name, extract_domain

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """
    Feature engineering for entity matching.
    
    Generates features that help distinguish true matches from false positives.
    """
    
    def __init__(self):
        """Initialize the feature engineer."""
        pass
    
    def generate_features(
        self,
        crawl_row: Dict[str, Any],
        abr_row: Dict[str, Any]
    ) -> Dict[str, float]:
        """
        Generate matching features for a pair of records.
        
        Args:
            crawl_row: Common Crawl record
            abr_row: ABR record
            
        Returns:
            Dictionary of feature names to values
        """
        features = {}
        
        # Name similarity features
        features.update(self._name_features(
            crawl_row.get('company_name', ''),
            abr_row.get('entity_name', '')
        ))
        
        # Domain features
        features.update(self._domain_features(
            crawl_row.get('url', ''),
            abr_row.get('entity_name', '')
        ))
        
        # Location features
        features.update(self._location_features(
            crawl_row.get('raw_text', ''),
            abr_row.get('state', ''),
            abr_row.get('postcode', '')
        ))
        
        # Industry features
        features.update(self._industry_features(
            crawl_row.get('industry', ''),
            abr_row.get('entity_type', '')
        ))
        
        return features
    
    def _name_features(
        self,
        name1: Optional[str],
        name2: Optional[str]
    ) -> Dict[str, float]:
        """Generate name-based features."""
        features = {}
        
        if not name1 or not name2:
            return {
                'name_exact_match': 0.0,
                'name_fuzzy_ratio': 0.0,
                'name_token_sort': 0.0,
                'name_token_set': 0.0,
                'name_partial_ratio': 0.0,
                'normalized_exact': 0.0
            }
        
        # Normalize both names
        norm1 = normalize_company_name(name1)
        norm2 = normalize_company_name(name2)
        
        # Exact match
        features['name_exact_match'] = 1.0 if norm1 == norm2 else 0.0
        features['normalized_exact'] = features['name_exact_match']
        
        if fuzz:
            # RapidFuzz features
            features['name_fuzzy_ratio'] = fuzz.ratio(name1, name2) / 100.0
            features['name_token_sort'] = fuzz.token_sort_ratio(name1, name2) / 100.0
            features['name_token_set'] = fuzz.token_set_ratio(name1, name2) / 100.0
            features['name_partial_ratio'] = fuzz.partial_ratio(name1, name2) / 100.0
        else:
            # Fallback
            features['name_fuzzy_ratio'] = self._jaccard(name1, name2)
            features['name_token_sort'] = features['name_fuzzy_ratio']
            features['name_token_set'] = features['name_fuzzy_ratio']
            features['name_partial_ratio'] = features['name_fuzzy_ratio']
        
        return features
    
    def _domain_features(
        self,
        url: Optional[str],
        entity_name: Optional[str]
    ) -> Dict[str, float]:
        """Generate domain-based features."""
        features = {
            'domain_name_match': 0.0,
            'domain_contains_name': 0.0
        }
        
        if not url or not entity_name:
            return features
        
        domain = extract_domain(url).lower()
        name_parts = normalize_company_name(entity_name).lower().split()
        
        # Check if domain contains company name parts
        for part in name_parts:
            if len(part) >= 4 and part in domain:
                features['domain_contains_name'] = 1.0
                break
        
        # Check for close domain match
        domain_clean = re.sub(r'\.com\.au$|\.au$', '', domain)
        domain_clean = re.sub(r'[^a-z0-9]', '', domain_clean)
        
        name_clean = ''.join(name_parts)
        name_clean = re.sub(r'[^a-z0-9]', '', name_clean)
        
        if domain_clean and name_clean:
            if fuzz:
                features['domain_name_match'] = fuzz.ratio(domain_clean, name_clean) / 100.0
            else:
                features['domain_name_match'] = 1.0 if domain_clean == name_clean else 0.0
        
        return features
    
    def _location_features(
        self,
        raw_text: Optional[str],
        state: Optional[str],
        postcode: Optional[str]
    ) -> Dict[str, float]:
        """Generate location-based features."""
        features = {
            'state_mentioned': 0.0,
            'postcode_mentioned': 0.0
        }
        
        if not raw_text:
            return features
        
        text_lower = raw_text.lower()
        
        # Check state mention
        if state:
            state_names = {
                'NSW': ['nsw', 'new south wales', 'sydney'],
                'VIC': ['vic', 'victoria', 'melbourne'],
                'QLD': ['qld', 'queensland', 'brisbane'],
                'SA': ['south australia', 'adelaide'],
                'WA': ['western australia', 'perth'],
                'TAS': ['tasmania', 'hobart'],
                'NT': ['northern territory', 'darwin'],
                'ACT': ['act', 'canberra', 'australian capital']
            }
            
            for variant in state_names.get(state.upper(), [state.lower()]):
                if variant in text_lower:
                    features['state_mentioned'] = 1.0
                    break
        
        # Check postcode mention
        if postcode:
            if postcode in raw_text:
                features['postcode_mentioned'] = 1.0
        
        return features
    
    def _industry_features(
        self,
        extracted_industry: Optional[str],
        entity_type: Optional[str]
    ) -> Dict[str, float]:
        """Generate industry-based features."""
        features = {
            'industry_present': 0.0,
            'is_company': 0.0
        }
        
        # Check if industry was extracted
        if extracted_industry:
            features['industry_present'] = 1.0
        
        # Check if entity type is a company
        if entity_type:
            company_types = ['PRV', 'PUB', 'Private Company', 'Public Company']
            if entity_type in company_types:
                features['is_company'] = 1.0
        
        return features
    
    def _jaccard(self, s1: str, s2: str) -> float:
        """Simple Jaccard similarity."""
        if not s1 or not s2:
            return 0.0
        
        set1 = set(s1.lower().split())
        set2 = set(s2.lower().split())
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        return intersection / union if union > 0 else 0.0
    
    def generate_features_batch(
        self,
        pairs: List[tuple]
    ) -> pd.DataFrame:
        """
        Generate features for multiple record pairs.
        
        Args:
            pairs: List of (crawl_row, abr_row) tuples
            
        Returns:
            DataFrame with features
        """
        all_features = []
        
        for crawl_row, abr_row in pairs:
            features = self.generate_features(crawl_row, abr_row)
            all_features.append(features)
        
        return pd.DataFrame(all_features)


def compute_match_probability(features: Dict[str, float]) -> float:
    """
    Compute match probability from features.
    
    Simple weighted sum approach. Could be replaced with ML model.
    
    Args:
        features: Feature dictionary
        
    Returns:
        Match probability (0-1)
    """
    weights = {
        'name_exact_match': 0.30,
        'name_token_sort': 0.20,
        'name_token_set': 0.15,
        'domain_name_match': 0.15,
        'domain_contains_name': 0.10,
        'state_mentioned': 0.05,
        'postcode_mentioned': 0.05
    }
    
    score = 0.0
    total_weight = 0.0
    
    for feature, weight in weights.items():
        if feature in features:
            score += features[feature] * weight
            total_weight += weight
    
    if total_weight == 0:
        return 0.0
    
    return score / total_weight


if __name__ == "__main__":
    # Example usage
    engineer = FeatureEngineer()
    
    crawl_record = {
        'company_name': 'ACME Digital Services',
        'url': 'https://acmedigital.com.au',
        'raw_text': 'ACME Digital is based in Sydney NSW...',
        'industry': 'Information Technology'
    }
    
    abr_record = {
        'entity_name': 'ACME Digital Services Pty Ltd',
        'entity_type': 'PRV',
        'state': 'NSW',
        'postcode': '2000'
    }
    
    features = engineer.generate_features(crawl_record, abr_record)
    print("Features:", features)
    
    probability = compute_match_probability(features)
    print(f"Match probability: {probability:.2%}")

