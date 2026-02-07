"""
Bronze Layer: Raw data landing zone with immutable append-only storage.

Responsibilities:
- Ingest raw events with minimal transformation
- Add ingestion metadata (timestamps, source lineage, batch IDs)
- Partition by ingestion date for efficient downstream reads
- Maintain complete audit trail of all received data
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, date_format,
    year, month, dayofmonth, hour, input_file_name,
    monotonically_increasing_id, sha2, concat_ws
)
from delta.tables import DeltaTable
from src.config.settings import settings
from src.config.spark_config import get_spark
from src.utils.logger import get_logger
from src.utils.metrics import MetricsCollector
from typing import Optional
import os

logger = get_logger(__name__)
metrics = MetricsCollector()


class BronzeLayer:
    """
    Bronze layer processor implementing the first stage of the Medallion Architecture.
    
    Design Principles:
    - Raw data fidelity: No business transformations applied
    - Append-only: Immutable record of all ingested data
    - Full lineage: Every record tracked with source metadata
    - Partition strategy: Date-based partitioning for time-range queries
    """
    
    TABLE_NAME = "bronze_events"
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or get_spark(streaming=True)
        self.bronze_path = settings.delta.bronze_path
        self.checkpoint_path = f"{settings.spark.checkpoint_location}/bronze"
        
        logger.info(f"BronzeLayer initialized | Path: {self.bronze_path}")
    
    def initialize_table(self) -> None:
        """Create the Bronze Delta table if it doesn't exist."""
        if not DeltaTable.isDeltaTable(self.spark, self.bronze_path):
            logger.info("Initializing Bronze Delta table...")
            
            # Create empty table with schema
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS delta.`{self.bronze_path}` (
                    -- Raw event data
                    event_id STRING,
                    event_type STRING,
                    user_id STRING,
                    session_id STRING,
                    timestamp STRING,
                    product_id STRING,
                    product_name STRING,
                    category STRING,
                    price DOUBLE,
                    quantity INT,
                    currency STRING,
                    device_type STRING,
                    browser STRING,
                    os STRING,
                    ip_address STRING,
                    country STRING,
                    city STRING,
                    referrer STRING,
                    utm_source STRING,
                    utm_medium STRING,
                    utm_campaign STRING,
                    
                    -- Ingestion metadata
                    kafka_key STRING,
                    kafka_topic STRING,
                    kafka_partition INT,
                    kafka_offset LONG,
                    kafka_timestamp TIMESTAMP,
                    ingestion_timestamp TIMESTAMP,
                    event_timestamp TIMESTAMP,
                    row_hash STRING,
                    batch_id STRING,
                    source_system STRING,
                    
                    -- Partition columns
                    event_date STRING,
                    event_hour INT
                )
                USING DELTA
                PARTITIONED BY (event_date, event_hour)
                TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true',
                    'delta.logRetentionDuration' = 'interval 30 days',
                    'delta.deletedFileRetentionDuration' = 'interval 7 days'
                )
            """)
            
            logger.info("Bronze Delta table created successfully")
    
    def process_stream(self, input_stream: DataFrame) -> None:
        """
        Process a streaming DataFrame and write to Bronze layer.
        
        Uses append mode with exactly-once semantics via checkpointing.
        
        Args:
            input_stream: Streaming DataFrame from Kafka consumer
        """
        logger.info("Starting Bronze stream processing...")
        
        enriched_stream = self._enrich_with_metadata(input_stream)
        
        query = (
            enriched_stream
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_path)
            .option("mergeSchema", "true")
            .trigger(processingTime=settings.spark.trigger_interval)
            .partitionBy("event_date", "event_hour")
            .start(self.bronze_path)
        )
        
        logger.info(f"Bronze streaming query started | ID: {query.id}")
        metrics.increment("bronze_stream_started")
        
        return query
    
    def process_batch(self, input_df: DataFrame) -> int:
        """
        Process a batch DataFrame and append to Bronze layer.
        
        Args:
            input_df: Batch DataFrame to ingest
            
        Returns:
            Number of records written
        """
        enriched_df = self._enrich_with_metadata(input_df)
        record_count = enriched_df.count()
        
        (
            enriched_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("event_date", "event_hour")
            .save(self.bronze_path)
        )
        
        logger.info(f"Bronze batch write complete | Records: {record_count}")
        metrics.increment("bronze_batch_records", record_count)
        
        return record_count
    
    def _enrich_with_metadata(self, df: DataFrame) -> DataFrame:
        """Add Bronze-layer metadata columns to the DataFrame."""
        return (
            df
            .withColumn("batch_id", lit(self._generate_batch_id()))
            .withColumn("source_system", lit("kafka_streaming"))
            .withColumn(
                "event_date",
                date_format(col("event_timestamp"), "yyyy-MM-dd")
            )
            .withColumn("event_hour", hour(col("event_timestamp")))
        )
    
    def get_table(self) -> DeltaTable:
        """Get the Bronze DeltaTable instance."""
        return DeltaTable.forPath(self.spark, self.bronze_path)
    
    def read_table(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> DataFrame:
        """
        Read from the Bronze table with optional date range filtering.
        
        Args:
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
        """
        df = self.spark.read.format("delta").load(self.bronze_path)
        
        if start_date:
            df = df.filter(col("event_date") >= start_date)
        if end_date:
            df = df.filter(col("event_date") <= end_date)
        
        return df
    
    def get_table_stats(self) -> dict:
        """Get statistics about the Bronze table."""
        df = self.spark.read.format("delta").load(self.bronze_path)
        
        return {
            "total_records": df.count(),
            "partitions": df.select("event_date").distinct().count(),
            "event_types": [
                row.event_type 
                for row in df.select("event_type").distinct().collect()
            ],
            "date_range": {
                "min": df.agg({"event_date": "min"}).collect()[0][0],
                "max": df.agg({"event_date": "max"}).collect()[0][0],
            },
            "path": self.bronze_path,
        }
    
    @staticmethod
    def _generate_batch_id() -> str:
        """Generate a unique batch identifier."""
        from datetime import datetime
        import uuid
        return f"bronze_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
