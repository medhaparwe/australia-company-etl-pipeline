"""
LLM-based Entity Matching for the Australia Company ETL Pipeline.
Uses OpenAI GPT models for semantic entity resolution.
"""

import os
import json
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
import logging

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Result of an LLM entity match."""
    is_match: bool
    score: float
    reason: str
    confidence: str  # "high", "medium", "low"


class LLMMatcher:
    """
    LLM-powered entity matcher for company resolution.
    
    Uses GPT models to determine if two company records
    refer to the same real-world entity.
    """
    
    def __init__(
        self,
        model: str = "gpt-4o-mini",
        api_key: Optional[str] = None,
        temperature: float = 0.0
    ):
        """
        Initialize the LLM matcher.
        
        Args:
            model: OpenAI model to use
            api_key: OpenAI API key (defaults to OPENAI_API_KEY env var)
            temperature: Sampling temperature (0 for deterministic)
        """
        self.model = model
        self.temperature = temperature
        
        if OpenAI is None:
            logger.warning("OpenAI package not installed. LLM matching disabled.")
            self.client = None
        else:
            api_key = api_key or os.getenv("OPENAI_API_KEY")
            if api_key:
                self.client = OpenAI(api_key=api_key)
            else:
                logger.warning("No OpenAI API key found. LLM matching disabled.")
                self.client = None
    
    def is_available(self) -> bool:
        """Check if LLM matching is available."""
        return self.client is not None
    
    def match_companies(
        self,
        web_company: Dict[str, Any],
        abr_company: Dict[str, Any]
    ) -> MatchResult:
        """
        Determine if two company records match using LLM.
        
        Args:
            web_company: Company data from Common Crawl
                - name: Company name
                - url: Website URL
                - industry: Industry (optional)
                
            abr_company: Company data from ABR
                - entity_name: Legal entity name
                - abn: Australian Business Number
                - state: State code
                - postcode: Postcode
                
        Returns:
            MatchResult with match decision and confidence
        """
        if not self.is_available():
            # Return neutral score if LLM not available
            return MatchResult(
                is_match=False,
                score=0.5,
                reason="LLM matching not available",
                confidence="low"
            )
        
        prompt = self._build_prompt(web_company, abr_company)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": self._get_system_prompt()
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=self.temperature,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            return MatchResult(
                is_match=result.get("match", False),
                score=float(result.get("score", 0.0)),
                reason=result.get("reason", ""),
                confidence=result.get("confidence", "low")
            )
            
        except Exception as e:
            logger.error(f"LLM matching error: {e}")
            return MatchResult(
                is_match=False,
                score=0.5,
                reason=f"Error: {str(e)}",
                confidence="low"
            )
    
    def batch_match(
        self,
        pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]]
    ) -> List[MatchResult]:
        """
        Match multiple company pairs in batch.
        
        Args:
            pairs: List of (web_company, abr_company) tuples
            
        Returns:
            List of MatchResults
        """
        results = []
        for web_company, abr_company in pairs:
            result = self.match_companies(web_company, abr_company)
            results.append(result)
        return results
    
    def _get_system_prompt(self) -> str:
        """Get the system prompt for entity matching."""
        return """You are an expert entity resolution system specialized in matching Australian company records.

Your task is to determine if two company records from different data sources refer to the same real-world business entity.

Consider:
1. Company name similarity (accounting for abbreviations, trading names vs legal names)
2. Location consistency (state, postcode)
3. Industry alignment (if available)
4. Website domain matching company name

You must respond with a JSON object containing:
{
    "match": true/false,
    "score": 0.0 to 1.0 (confidence score),
    "reason": "Brief explanation of your decision",
    "confidence": "high" | "medium" | "low"
}

Examples of matches:
- "ACME Digital" matches "ACME DIGITAL SERVICES PTY LTD" (abbreviated legal name)
- "Koala Tech" matches "KOALA TECHNOLOGIES PTY LTD" (trading name)
- "Smith & Associates" matches "SMITH AND ASSOCIATES PTY LTD" (& vs AND)

Examples of non-matches:
- "ACME Group" vs "ACME IT Solutions" (different business lines)
- "Sydney Plumbing" vs "Sydney Electrical" (different industries, same location)"""
    
    def _build_prompt(
        self,
        web_company: Dict[str, Any],
        abr_company: Dict[str, Any]
    ) -> str:
        """Build the matching prompt."""
        return f"""Compare these two company records and determine if they represent the same entity:

**Source 1: Website (Common Crawl)**
- Company Name: {web_company.get('name', 'N/A')}
- Website URL: {web_company.get('url', 'N/A')}
- Industry: {web_company.get('industry', 'N/A')}

**Source 2: Australian Business Register (ABR)**
- Legal Entity Name: {abr_company.get('entity_name', 'N/A')}
- ABN: {abr_company.get('abn', 'N/A')}
- State: {abr_company.get('state', 'N/A')}
- Postcode: {abr_company.get('postcode', 'N/A')}

Do these records refer to the same real-world company? Provide your analysis as JSON."""


def get_llm_score(
    name1: str,
    name2: str,
    model: str = "gpt-4o-mini"
) -> float:
    """
    Simple function to get LLM similarity score between two names.
    
    Args:
        name1: First company name
        name2: Second company name
        model: OpenAI model to use
        
    Returns:
        Similarity score (0-1)
    """
    matcher = LLMMatcher(model=model)
    
    result = matcher.match_companies(
        {"name": name1},
        {"entity_name": name2}
    )
    
    return result.score

