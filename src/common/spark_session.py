"""
Spark Session management for the Australia Company ETL Pipeline.
"""

from pyspark.sql import SparkSession
from typing import Optional, Dict, Any
import os


def get_spark_session(
    app_name: str = "AustraliaCompanyETL",
    master: str = "local[*]",
    config: Optional[Dict[str, Any]] = None
) -> SparkSession:
    """
    Create or get an existing Spark session with optimized settings.
    
    Args:
        app_name: Name of the Spark application
        master: Spark master URL
        config: Optional additional Spark configuration
        
    Returns:
        SparkSession instance
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master)
    
    # Default configurations for the pipeline
    default_config = {
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        # For XML parsing
        "spark.jars.packages": "com.databricks:spark-xml_2.12:0.17.0",
    }
    
    # Override with provided config
    if config:
        default_config.update(config)
    
    # Apply all configurations
    for key, value in default_config.items():
        builder = builder.config(key, value)
    
    # Get or create session
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """
    Stop the Spark session gracefully.
    
    Args:
        spark: SparkSession to stop
    """
    if spark:
        spark.stop()


class SparkSessionManager:
    """
    Context manager for Spark session lifecycle.
    
    Usage:
        with SparkSessionManager() as spark:
            df = spark.read.parquet("data.parquet")
            ...
    """
    
    def __init__(
        self,
        app_name: str = "AustraliaCompanyETL",
        master: str = "local[*]",
        config: Optional[Dict[str, Any]] = None
    ):
        self.app_name = app_name
        self.master = master
        self.config = config
        self.spark: Optional[SparkSession] = None
    
    def __enter__(self) -> SparkSession:
        self.spark = get_spark_session(
            app_name=self.app_name,
            master=self.master,
            config=self.config
        )
        return self.spark
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.spark:
            self.spark.stop()
            self.spark = None

