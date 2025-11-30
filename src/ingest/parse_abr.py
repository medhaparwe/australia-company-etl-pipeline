"""
Parse ABR (Australian Business Register) XML bulk extract files.
"""

import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Any, List, Optional, Generator
from dataclasses import dataclass, asdict
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class ABREntity:
    """Entity record from ABR."""
    abn: str
    entity_name: str
    entity_type: Optional[str]
    entity_status: Optional[str]
    state: Optional[str]
    postcode: Optional[str]
    start_date: Optional[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ABRParser:
    """
    Parser for ABR XML bulk extract files.
    
    Handles both full bulk extracts and incremental updates.
    """
    
    def __init__(self):
        """Initialize the parser."""
        self.record_tag = "ABRRecord"  # May vary by file format
        self.namespace = None
    
    def parse_file(
        self,
        file_path: str,
        max_records: Optional[int] = None
    ) -> Generator[ABREntity, None, None]:
        """
        Parse an ABR XML file and yield entity records.
        
        Args:
            file_path: Path to XML file
            max_records: Maximum records to parse (None for all)
            
        Yields:
            ABREntity records
        """
        logger.info(f"Parsing ABR file: {file_path}")
        
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return
        
        count = 0
        
        try:
            # Use iterparse for memory efficiency with large files
            context = ET.iterparse(file_path, events=['end'])
            
            for event, elem in context:
                # Check for record element
                tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                
                if tag in ['ABRRecord', 'ABR', 'Record']:
                    if max_records and count >= max_records:
                        break
                    
                    entity = self._parse_record(elem)
                    if entity:
                        count += 1
                        yield entity
                    
                    # Clear element to save memory
                    elem.clear()
            
            logger.info(f"Parsed {count} records from {file_path}")
            
        except ET.ParseError as e:
            logger.error(f"XML parse error: {e}")
        except Exception as e:
            logger.error(f"Error parsing file: {e}")
    
    def _parse_record(self, elem: ET.Element) -> Optional[ABREntity]:
        """Parse a single ABR record element."""
        try:
            # Extract ABN (text content of ABN element)
            abn_elem = elem.find('ABN')
            if abn_elem is None or not abn_elem.text:
                return None
            abn = abn_elem.text.strip()
            
            # Extract ABN status from attribute
            entity_status = abn_elem.get('status')
            
            # Extract status date from ABN attribute
            start_date = abn_elem.get('ABNStatusFromDate')
            
            # Extract entity name - handle different entity types
            entity_name = self._extract_entity_name(elem)
            if not entity_name:
                return None
            
            # Extract entity type
            entity_type = (
                self._get_text(elem, 'EntityType/EntityTypeText') or
                self._get_text(elem, 'EntityType/EntityTypeInd')
            )
            
            # Extract address components from MainEntity or LegalEntity
            state, postcode = self._extract_address(elem)
            
            return ABREntity(
                abn=self._format_abn(abn),
                entity_name=entity_name.strip(),
                entity_type=entity_type,
                entity_status=entity_status,
                state=state,
                postcode=postcode,
                start_date=self._format_date(start_date)
            )
            
        except Exception as e:
            logger.debug(f"Error parsing record: {e}")
            return None
    
    def _extract_entity_name(self, elem: ET.Element) -> Optional[str]:
        """Extract entity name from various possible locations."""
        # Try MainEntity (for companies)
        main_entity = elem.find('MainEntity')
        if main_entity is not None:
            # Non-individual name (companies)
            name = self._get_text(main_entity, 'NonIndividualName/NonIndividualNameText')
            if name:
                return name
        
        # Try LegalEntity (for individuals/sole traders)
        legal_entity = elem.find('LegalEntity')
        if legal_entity is not None:
            # Non-individual name in LegalEntity
            name = self._get_text(legal_entity, 'NonIndividualName/NonIndividualNameText')
            if name:
                return name
            
            # Individual name - construct from parts
            ind_name = legal_entity.find('IndividualName')
            if ind_name is not None:
                name_parts = []
                # Get all given names
                for given in ind_name.findall('GivenName'):
                    if given.text:
                        name_parts.append(given.text.strip())
                # Get family name
                family = ind_name.find('FamilyName')
                if family is not None and family.text:
                    name_parts.append(family.text.strip())
                if name_parts:
                    return ' '.join(name_parts)
        
        # Fallback to other possible locations
        return (
            self._get_text(elem, 'EntityName') or
            self._get_text(elem, 'MainName/NonIndividualNameText') or
            self._get_text(elem, 'Name')
        )
    
    def _extract_address(self, elem: ET.Element) -> tuple:
        """Extract state and postcode from address."""
        state = None
        postcode = None
        
        # Try MainEntity address
        paths_to_try = [
            'MainEntity/BusinessAddress/AddressDetails',
            'LegalEntity/BusinessAddress/AddressDetails',
            'BusinessAddress/AddressDetails',
            'AddressDetails',
        ]
        
        for path in paths_to_try:
            addr = elem.find(path)
            if addr is not None:
                state_elem = addr.find('State')
                postcode_elem = addr.find('Postcode')
                if state_elem is not None and state_elem.text:
                    state = state_elem.text.strip()
                if postcode_elem is not None and postcode_elem.text:
                    postcode = postcode_elem.text.strip()
                if state or postcode:
                    break
        
        return state, postcode
    
    def _get_text(self, elem: ET.Element, path: str) -> Optional[str]:
        """Get text content from element path."""
        try:
            child = elem.find(path)
            if child is not None and child.text:
                return child.text.strip()
        except Exception:
            pass
        return None
    
    def _format_abn(self, abn: str) -> str:
        """Format ABN as standard 11-digit string."""
        # Remove non-digits
        digits = ''.join(c for c in abn if c.isdigit())
        return digits[:11] if len(digits) >= 11 else digits
    
    def _format_date(self, date_str: Optional[str]) -> Optional[str]:
        """Format date string to ISO format."""
        if not date_str:
            return None
        
        # Try common date formats
        formats = [
            '%Y%m%d',      # ABR format: 20211015
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%Y/%m/%d',
            '%d-%m-%Y',
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str[:10] if fmt != '%Y%m%d' else date_str[:8], fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        return date_str
    
    def parse_multiple_files(
        self,
        file_paths: List[str],
        max_records_per_file: Optional[int] = None
    ) -> Generator[ABREntity, None, None]:
        """
        Parse multiple ABR XML files.
        
        Args:
            file_paths: List of XML file paths
            max_records_per_file: Max records per file
            
        Yields:
            ABREntity records
        """
        for file_path in file_paths:
            try:
                yield from self.parse_file(file_path, max_records_per_file)
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue


def parse_abr_to_dataframe(
    file_path: str,
    max_records: Optional[int] = None
):
    """
    Parse ABR XML file and return as pandas DataFrame.
    
    Args:
        file_path: Path to XML file
        max_records: Maximum records to parse
        
    Returns:
        pandas DataFrame
    """
    import pandas as pd
    
    parser = ABRParser()
    records = list(parser.parse_file(file_path, max_records))
    
    if not records:
        return pd.DataFrame()
    
    return pd.DataFrame([r.to_dict() for r in records])


def parse_abr_to_spark(
    file_paths: List[str],
    spark_session,
    output_path: Optional[str] = None
):
    """
    Parse ABR XML files using Spark for large-scale processing.
    
    Args:
        file_paths: List of XML file paths
        spark_session: SparkSession instance
        output_path: Optional path to save as Parquet
        
    Returns:
        Spark DataFrame
    """
    from pyspark.sql.types import StructType, StructField, StringType
    
    schema = StructType([
        StructField("abn", StringType(), False),
        StructField("entity_name", StringType(), False),
        StructField("entity_type", StringType(), True),
        StructField("entity_status", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("start_date", StringType(), True),
    ])
    
    parser = ABRParser()
    
    # Collect all records
    all_records = []
    for file_path in file_paths:
        for entity in parser.parse_file(file_path):
            all_records.append(entity.to_dict())
    
    # Create DataFrame
    df = spark_session.createDataFrame(all_records, schema)
    
    if output_path:
        df.write.mode('overwrite').parquet(output_path)
    
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Example usage - parse ABR XML files from data/raw/abr
    from pathlib import Path
    
    parser = ABRParser()
    abr_dir = Path("data/raw/abr")
    xml_files = list(abr_dir.glob("*.xml"))
    
    if xml_files:
        for i, entity in enumerate(parser.parse_file(str(xml_files[0]), max_records=10)):
            print(f"\n--- Record {i+1} ---")
            print(f"ABN: {entity.abn}")
            print(f"Name: {entity.entity_name}")
            print(f"Type: {entity.entity_type}")
            print(f"Status: {entity.entity_status}")
            print(f"State: {entity.state}")
    else:
        print("No ABR XML files found in data/raw/abr/")

