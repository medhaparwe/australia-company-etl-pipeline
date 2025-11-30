"""
Spark Session management for the Australia Company ETL Pipeline.
"""

from pyspark.sql import SparkSession
from typing import Optional, Dict, Any
import os
import sys
import platform
import urllib.request
import zipfile
import tempfile
import logging

logger = logging.getLogger(__name__)


def setup_hadoop_home_windows():
    """
    Set up HADOOP_HOME on Windows with winutils.exe.
    This is required for PySpark to work on Windows.
    """
    if platform.system() != "Windows":
        return
    
    # Check if HADOOP_HOME is already set and valid
    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home:
        winutils_path = os.path.join(hadoop_home, "bin", "winutils.exe")
        if os.path.exists(winutils_path):
            logger.info(f"Using existing HADOOP_HOME: {hadoop_home}")
            return
    
    # Create a local hadoop directory in the project
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    hadoop_dir = os.path.join(project_root, "hadoop")
    hadoop_bin_dir = os.path.join(hadoop_dir, "bin")
    winutils_path = os.path.join(hadoop_bin_dir, "winutils.exe")
    
    # Create directories if they don't exist
    os.makedirs(hadoop_bin_dir, exist_ok=True)
    
    # Download winutils.exe if it doesn't exist
    if not os.path.exists(winutils_path):
        logger.info("Downloading winutils.exe for Windows Spark support...")
        # Using a known working version of winutils for Hadoop 3.x
        winutils_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe"
        hadoop_dll_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll"
        
        try:
            urllib.request.urlretrieve(winutils_url, winutils_path)
            logger.info(f"Downloaded winutils.exe to {winutils_path}")
            
            # Also download hadoop.dll which may be needed
            hadoop_dll_path = os.path.join(hadoop_bin_dir, "hadoop.dll")
            urllib.request.urlretrieve(hadoop_dll_url, hadoop_dll_path)
            logger.info(f"Downloaded hadoop.dll to {hadoop_dll_path}")
        except Exception as e:
            logger.warning(f"Failed to download winutils.exe: {e}")
            logger.warning("Please manually download winutils.exe from:")
            logger.warning("https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin")
            logger.warning(f"And place it in: {hadoop_bin_dir}")
            # Create a dummy file to prevent repeated download attempts
            # but still raise an error for the user
            raise RuntimeError(
                f"HADOOP_HOME setup failed. Please download winutils.exe manually from "
                f"https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin "
                f"and place it in {hadoop_bin_dir}"
            )
    
    # Set environment variables
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["hadoop.home.dir"] = hadoop_dir
    
    # Add hadoop bin to PATH
    current_path = os.environ.get("PATH", "")
    if hadoop_bin_dir not in current_path:
        os.environ["PATH"] = hadoop_bin_dir + os.pathsep + current_path
    
    logger.info(f"Set HADOOP_HOME to {hadoop_dir}")


def setup_python_executable():
    """
    Set up Python executable path for Spark on Windows.
    Spark defaults to 'python3' which doesn't exist on Windows.
    """
    if platform.system() == "Windows":
        # Get the current Python executable
        python_exe = sys.executable
        
        # Set environment variables for Spark to use
        os.environ["PYSPARK_PYTHON"] = python_exe
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
        
        logger.info(f"Set PYSPARK_PYTHON to {python_exe} for Windows compatibility")


def get_spark_session(
    app_name: str = "AustraliaCompanyETL",
    master: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None
) -> SparkSession:
    """
    Create or get an existing Spark session with optimized settings.
    
    Supports both local and remote Spark clusters (e.g., Docker).
    The master URL can be set via:
    1. Environment variable SPARK_MASTER_URL
    2. Config parameter
    3. Defaults to local[*] if neither is set
    
    Args:
        app_name: Name of the Spark application
        master: Spark master URL (e.g., "local[*]", "spark://spark-master:7077")
        config: Optional additional Spark configuration
        
    Returns:
        SparkSession instance
    """
    # Setup Hadoop for Windows (required for winutils.exe)
    setup_hadoop_home_windows()
    
    # Setup Python executable for Windows (required to use 'python' instead of 'python3')
    setup_python_executable()
    
    # Determine Spark master URL
    # Priority: 1) parameter, 2) environment variable, 3) default
    if master is None:
        master = os.getenv("SPARK_MASTER_URL", "local[*]")
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master)
    
    # Get Python executable for Spark config
    python_exe = sys.executable
    
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
        # Set Python executable paths (important for Windows)
        "spark.pyspark.python": python_exe,
        "spark.pyspark.driver.python": python_exe,
    }
    
    # Windows-specific configurations to handle Python worker issues
    if platform.system() == "Windows":
        # Enable fault handler for better error messages
        default_config["spark.sql.execution.pyspark.udf.faulthandler.enabled"] = "true"
        default_config["spark.python.worker.faulthandler.enabled"] = "true"
        
        # Reduce task size to avoid large task warnings
        default_config["spark.sql.files.maxPartitionBytes"] = "128m"
        default_config["spark.sql.files.openCostInBytes"] = "4194304"  # 4MB
        
        # Python worker settings for Windows
        default_config["spark.python.worker.reuse"] = "true"
        default_config["spark.python.worker.timeout"] = "600s"
        
        # Network settings for Windows socket issues
        default_config["spark.network.timeout"] = "600s"
        default_config["spark.executor.heartbeatInterval"] = "60s"
        
        # Reduce memory overhead to avoid OOM
        default_config["spark.executor.memoryOverhead"] = "512m"
        default_config["spark.driver.memoryOverhead"] = "512m"
        
        # Serialization settings
        default_config["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"
        default_config["spark.kryoserializer.buffer.max"] = "512m"
    
    # Additional config for remote Spark cluster
    if master and master.startswith("spark://"):
        # When connecting to remote Spark, ensure driver can connect back
        default_config["spark.driver.host"] = os.getenv("SPARK_DRIVER_HOST", "localhost")
        default_config["spark.driver.bindAddress"] = "0.0.0.0"
        # Network timeout for remote connections
        default_config["spark.network.timeout"] = "600s"
    
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
        master: Optional[str] = None,
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

