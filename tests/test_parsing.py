"""
Tests for data parsing modules.
"""

import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.utils import (
    normalize_company_name,
    validate_abn,
    format_abn,
    extract_domain,
    is_australian_domain,
    clean_text
)


class TestNormalizeCompanyName:
    """Tests for company name normalization."""
    
    def test_removes_pty_ltd(self):
        assert normalize_company_name("ACME PTY LTD") == "ACME"
        assert normalize_company_name("ACME Pty Ltd") == "ACME"
    
    def test_removes_australia(self):
        assert normalize_company_name("ACME Australia") == "ACME"
    
    def test_handles_none(self):
        assert normalize_company_name(None) == ""
    
    def test_handles_empty(self):
        assert normalize_company_name("") == ""
    
    def test_removes_punctuation(self):
        result = normalize_company_name("ACME & SONS PTY LTD")
        assert "ACME" in result
        assert "SONS" in result
    
    def test_uppercase_conversion(self):
        result = normalize_company_name("acme corporation")
        assert result == "ACME CORPORATION"


class TestValidateABN:
    """Tests for ABN validation."""
    
    def test_valid_abn(self):
        # Known valid ABN
        assert validate_abn("51824753556") == True
    
    def test_invalid_abn_wrong_length(self):
        assert validate_abn("1234567890") == False  # 10 digits
        assert validate_abn("123456789012") == False  # 12 digits
    
    def test_invalid_abn_none(self):
        assert validate_abn(None) == False
    
    def test_invalid_abn_empty(self):
        assert validate_abn("") == False
    
    def test_abn_with_spaces(self):
        # Should handle formatted ABN
        assert validate_abn("51 824 753 556") == True


class TestFormatABN:
    """Tests for ABN formatting."""
    
    def test_format_plain_abn(self):
        result = format_abn("51824753556")
        assert result == "51 824 753 556"
    
    def test_format_none(self):
        assert format_abn(None) == ""
    
    def test_format_already_formatted(self):
        result = format_abn("51 824 753 556")
        assert result == "51 824 753 556"


class TestExtractDomain:
    """Tests for domain extraction."""
    
    def test_extract_from_https(self):
        assert extract_domain("https://www.acme.com.au/about") == "acme.com.au"
    
    def test_extract_from_http(self):
        assert extract_domain("http://acme.com.au") == "acme.com.au"
    
    def test_removes_www(self):
        assert extract_domain("https://www.example.com") == "example.com"
    
    def test_handles_none(self):
        assert extract_domain(None) == ""
    
    def test_handles_invalid_url(self):
        result = extract_domain("not a url")
        assert result == ""


class TestIsAustralianDomain:
    """Tests for Australian domain detection."""
    
    def test_com_au(self):
        assert is_australian_domain("https://www.acme.com.au") == True
    
    def test_org_au(self):
        assert is_australian_domain("https://charity.org.au") == True
    
    def test_non_australian(self):
        assert is_australian_domain("https://www.example.com") == False
    
    def test_handles_none(self):
        assert is_australian_domain(None) == False


class TestCleanText:
    """Tests for text cleaning."""
    
    def test_removes_html_tags(self):
        result = clean_text("<p>Hello</p> <b>World</b>")
        assert "<p>" not in result
        assert "<b>" not in result
    
    def test_removes_urls(self):
        result = clean_text("Visit https://example.com for more")
        assert "https://" not in result
    
    def test_removes_emails(self):
        result = clean_text("Contact info@example.com")
        assert "@" not in result
    
    def test_handles_none(self):
        assert clean_text(None) == ""
    
    def test_collapses_whitespace(self):
        result = clean_text("Too    many   spaces")
        assert "  " not in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

