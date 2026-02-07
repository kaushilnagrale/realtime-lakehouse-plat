"""
Spark session factory with Delta Lake integration and optimized configurations.
Provides both streaming and batch session builders.
"""

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.config.settings import settings
import logging

logger = logging.getLogger(__name__)


class SparkSessionFactory:
    """Factory for creating configured Spark sessions with Delta Lake support."""
    
    _instance: SparkSession = None
    
    @classmethod
    def get_or_create(cls, app_name: str = None, streaming: bool = False) -> SparkSession:
        """
        Get or create a Spark session with Delta Lake and streaming support.
        
        Args:
            app_name: Override application name
            streaming: Enable streaming-specific optimizations
            
        Returns:
            Configured SparkSession instance
        """
        if cls._instance is not None and cls._instance._jsc.sc().isStopped() is False:
            return cls._instance
        
        app_name = app_name or settings.spark.app_name
        
        builder = (
            SparkSession.builder
            .master(settings.spark.master)
            .appName(app_name)
            # Delta Lake configurations
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # Memory settings
            .config("spark.driver.memory", settings.spark.driver_memory)
            .config("spark.executor.memory", settings.spark.executor_memory)
            .config("spark.executor.cores", settings.spark.executor_cores)
            # Shuffle optimization
            .config("spark.sql.shuffle.partitions", settings.spark.shuffle_partitions)
            # Delta Lake optimizations
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
            .config("spark.databricks.delta.autoCompact.enabled", "true")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            # Adaptive Query Execution
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
        )
        
        if streaming:
            builder = (
                builder
                .config("spark.sql.streaming.schemaInference", "true")
                .config("spark.sql.streaming.checkpointLocation", settings.spark.checkpoint_location)
                # Kafka integration
                .config("spark.jars.packages", 
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                        "io.delta:delta-spark_2.12:3.0.0")
            )
        
        cls._instance = configure_spark_with_delta_pip(builder).getOrCreate()
        cls._instance.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Spark session created: {app_name} | Master: {settings.spark.master}")
        return cls._instance
    
    @classmethod
    def stop(cls):
        """Stop the active Spark session."""
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None
            logger.info("Spark session stopped")


def get_spark(streaming: bool = False) -> SparkSession:
    """Convenience function to get a Spark session."""
    return SparkSessionFactory.get_or_create(streaming=streaming)
