"""
PostgreSQL data loading for the Australia Company ETL Pipeline.
"""

import os
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
import pandas as pd

try:
    import psycopg2
    from psycopg2.extras import execute_batch, execute_values
except ImportError:
    psycopg2 = None

try:
    from sqlalchemy import create_engine, text
except ImportError:
    create_engine = None

logger = logging.getLogger(__name__)


class PostgresLoader:
    """
    PostgreSQL data loader for ETL pipeline.
    
    Handles connection management, table creation, and data loading.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "companydb",
        user: str = "postgres",
        password: str = "postgres"
    ):
        """
        Initialize the PostgreSQL loader.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        
        self._connection = None
        self._engine = None
    
    @property
    def connection_string(self) -> str:
        """Get the connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def get_connection(self):
        """Get a database connection."""
        if psycopg2 is None:
            raise ImportError("psycopg2 is required for PostgreSQL operations")
        
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
        
        return self._connection
    
    def get_engine(self):
        """Get a SQLAlchemy engine."""
        if create_engine is None:
            raise ImportError("SQLAlchemy is required for this operation")
        
        if self._engine is None:
            self._engine = create_engine(self.connection_string)
        
        return self._engine
    
    def close(self):
        """Close database connections."""
        if self._connection and not self._connection.closed:
            self._connection.close()
        if self._engine:
            self._engine.dispose()
    
    def create_tables(self, sql_file: str = "src/load/create_tables.sql"):
        """
        Create database tables from SQL file.
        
        Args:
            sql_file: Path to SQL schema file
        """
        logger.info(f"Creating tables from: {sql_file}")
        
        sql_path = Path(sql_file)
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        
        with open(sql_path, 'r') as f:
            sql = f.read()
        
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            logger.info("Tables created successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating tables: {e}")
            raise
    
    def load_web_companies(
        self,
        df: pd.DataFrame,
        batch_size: int = 1000
    ) -> int:
        """
        Load Common Crawl companies into web_companies table.
        
        Args:
            df: DataFrame with columns: url, domain, company_name, 
                normalized_name, industry, raw_text, block_key
            batch_size: Batch size for inserts
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading {len(df)} web companies")
        
        if df.empty:
            return 0
        
        # Prepare data
        columns = ['url', 'domain', 'company_name', 'normalized_name', 
                   'industry', 'raw_text', 'block_key']
        
        # Filter to available columns
        available_cols = [c for c in columns if c in df.columns]
        data = df[available_cols].to_dict('records')
        
        conn = self.get_connection()
        inserted = 0
        
        try:
            with conn.cursor() as cur:
                # Build insert query
                cols_str = ', '.join(available_cols)
                placeholders = ', '.join(['%s'] * len(available_cols))
                
                query = f"""
                    INSERT INTO web_companies ({cols_str})
                    VALUES ({placeholders})
                    ON CONFLICT DO NOTHING
                """
                
                # Batch insert
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    values = [tuple(row.get(c) for c in available_cols) for row in batch]
                    execute_batch(cur, query, values)
                    inserted += len(batch)
                    
                    if inserted % 10000 == 0:
                        logger.info(f"Loaded {inserted} records...")
                
            conn.commit()
            logger.info(f"Loaded {inserted} web companies")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading web companies: {e}")
            raise
        
        return inserted
    
    def load_abr_entities(
        self,
        df: pd.DataFrame,
        batch_size: int = 1000
    ) -> int:
        """
        Load ABR entities into abr_entities table.
        
        Args:
            df: DataFrame with columns: abn, entity_name, normalized_name,
                entity_type, entity_status, state, postcode, start_date, block_key
            batch_size: Batch size for inserts
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading {len(df)} ABR entities")
        
        if df.empty:
            return 0
        
        columns = ['abn', 'entity_name', 'normalized_name', 'entity_type',
                   'entity_status', 'state', 'postcode', 'start_date', 'block_key']
        
        available_cols = [c for c in columns if c in df.columns]
        data = df[available_cols].to_dict('records')
        
        conn = self.get_connection()
        inserted = 0
        
        try:
            with conn.cursor() as cur:
                cols_str = ', '.join(available_cols)
                placeholders = ', '.join(['%s'] * len(available_cols))
                
                query = f"""
                    INSERT INTO abr_entities ({cols_str})
                    VALUES ({placeholders})
                    ON CONFLICT (abn) DO UPDATE SET
                        entity_name = EXCLUDED.entity_name,
                        normalized_name = EXCLUDED.normalized_name,
                        entity_status = EXCLUDED.entity_status,
                        updated_at = CURRENT_TIMESTAMP
                """
                
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    values = [tuple(row.get(c) for c in available_cols) for row in batch]
                    execute_batch(cur, query, values)
                    inserted += len(batch)
                    
                    if inserted % 10000 == 0:
                        logger.info(f"Loaded {inserted} records...")
                
            conn.commit()
            logger.info(f"Loaded {inserted} ABR entities")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading ABR entities: {e}")
            raise
        
        return inserted
    
    def load_match_results(
        self,
        df: pd.DataFrame,
        batch_size: int = 1000
    ) -> int:
        """
        Load entity match results.
        
        Args:
            df: DataFrame with match results
            batch_size: Batch size for inserts
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading {len(df)} match results")
        
        if df.empty:
            return 0
        
        columns = ['crawl_name', 'crawl_url', 'abr_name', 'abn', 
                   'fuzzy_score', 'llm_score', 'final_score', 'match_method',
                   'state', 'postcode', 'start_date']
        
        available_cols = [c for c in columns if c in df.columns]
        data = df[available_cols].to_dict('records')
        
        conn = self.get_connection()
        inserted = 0
        
        try:
            with conn.cursor() as cur:
                cols_str = ', '.join(available_cols)
                placeholders = ', '.join(['%s'] * len(available_cols))
                
                query = f"""
                    INSERT INTO entity_match_results ({cols_str})
                    VALUES ({placeholders})
                    ON CONFLICT (crawl_url, abn) DO UPDATE SET
                        final_score = EXCLUDED.final_score,
                        match_method = EXCLUDED.match_method
                """
                
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    values = [tuple(row.get(c) for c in available_cols) for row in batch]
                    execute_batch(cur, query, values)
                    inserted += len(batch)
                
            conn.commit()
            logger.info(f"Loaded {inserted} match results")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading match results: {e}")
            raise
        
        return inserted
    
    def load_unified_companies(
        self,
        df: pd.DataFrame,
        batch_size: int = 1000
    ) -> int:
        """
        Load unified company records.
        
        Args:
            df: DataFrame with unified company data
            batch_size: Batch size for inserts
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading {len(df)} unified companies")
        
        if df.empty:
            return 0
        
        columns = ['abn', 'canonical_name', 'trading_name', 'url', 'domain',
                   'industry', 'entity_type', 'entity_status', 'state',
                   'postcode', 'start_date', 'source', 'confidence_score']
        
        available_cols = [c for c in columns if c in df.columns]
        data = df[available_cols].to_dict('records')
        
        conn = self.get_connection()
        inserted = 0
        
        try:
            with conn.cursor() as cur:
                cols_str = ', '.join(available_cols)
                placeholders = ', '.join(['%s'] * len(available_cols))
                
                # Build update clause for UPSERT
                update_cols = [c for c in available_cols if c != 'abn']
                update_str = ', '.join([f"{c} = EXCLUDED.{c}" for c in update_cols])
                
                query = f"""
                    INSERT INTO unified_companies ({cols_str})
                    VALUES ({placeholders})
                    ON CONFLICT (abn) DO UPDATE SET
                        {update_str},
                        updated_at = CURRENT_TIMESTAMP
                """
                
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    values = [tuple(row.get(c) for c in available_cols) for row in batch]
                    execute_batch(cur, query, values)
                    inserted += len(batch)
                
            conn.commit()
            logger.info(f"Loaded {inserted} unified companies")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading unified companies: {e}")
            raise
        
        return inserted
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            List of result dictionaries
        """
        conn = self.get_connection()
        
        with conn.cursor() as cur:
            cur.execute(query, params)
            
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
            
            return []
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get table statistics.
        
        Returns:
            Dictionary of table counts
        """
        stats = {}
        
        tables = ['web_companies', 'abr_entities', 'entity_match_results', 'unified_companies']
        
        for table in tables:
            try:
                result = self.execute_query(f"SELECT COUNT(*) as cnt FROM {table}")
                stats[table] = result[0]['cnt'] if result else 0
            except Exception:
                stats[table] = 0
        
        return stats


def load_dataframe_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    connection_string: str,
    if_exists: str = 'append'
) -> int:
    """
    Convenience function to load DataFrame to PostgreSQL.
    
    Args:
        df: DataFrame to load
        table_name: Target table name
        connection_string: PostgreSQL connection string
        if_exists: What to do if table exists ('append', 'replace', 'fail')
        
    Returns:
        Number of records loaded
    """
    if create_engine is None:
        raise ImportError("SQLAlchemy is required for this operation")
    
    engine = create_engine(connection_string)
    
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    
    return len(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test connection
    loader = PostgresLoader()
    
    try:
        # Create tables
        loader.create_tables()
        
        # Get stats
        stats = loader.get_stats()
        print("Table statistics:", stats)
        
    finally:
        loader.close()

