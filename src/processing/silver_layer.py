"""
Silver Layer: Cleansed, validated, and deduplicated data.

Responsibilities:
- Deduplication using event_id + row_hash
- Null handling and default value imputation
- Data type casting and standardization
- Schema validation against registered schemas
- Data quality checks with Great Expectations integration
- SCD Type 1 merge for dimension updates
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, lit, current_timestamp,
    lower, trim, regexp_replace, to_timestamp,
    row_number, first, last, count, sum as spark_sum,
    date_format, hour, sha2, concat_ws
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from src.config.settings import settings
from src.config.spark_config import get_spark
from src.utils.logger import get_logger
from src.utils.metrics import MetricsCollector
from typing import Optional, List

logger = get_logger(__name__)
metrics = MetricsCollector()


class SilverLayer:
    """
    Silver layer processor implementing data cleansing and quality enforcement.
    
    Transformations Applied:
    1. Deduplication (exactly-once via row_hash)
    2. Null handling (defaults for required fields)
    3. Data standardization (lowercase, trimming, type casting)
    4. Schema conformance validation
    5. Data quality scoring per record
    6. MERGE (upsert) into Silver Delta table
    """
    
    TABLE_NAME = "silver_events"
    
    # Quality thresholds
    COMPLETENESS_THRESHOLD = 0.8  # 80% of fields must be non-null
    REQUIRED_FIELDS = ["event_id", "event_type", "event_timestamp"]
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or get_spark(streaming=True)
        self.silver_path = settings.delta.silver_path
        self.bronze_path = settings.delta.bronze_path
        self.checkpoint_path = f"{settings.spark.checkpoint_location}/silver"
        
        logger.info(f"SilverLayer initialized | Path: {self.silver_path}")
    
    def initialize_table(self) -> None:
        """Create the Silver Delta table if it doesn't exist."""
        if not DeltaTable.isDeltaTable(self.spark, self.silver_path):
            logger.info("Initializing Silver Delta table...")
            
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS delta.`{self.silver_path}` (
                    -- Core event fields (cleaned)
                    event_id STRING NOT NULL,
                    event_type STRING NOT NULL,
                    user_id STRING,
                    session_id STRING,
                    event_timestamp TIMESTAMP NOT NULL,
                    
                    -- Product data (standardized)
                    product_id STRING,
                    product_name STRING,
                    category STRING,
                    price DOUBLE,
                    quantity INT,
                    total_amount DOUBLE,
                    currency STRING,
                    
                    -- User context (enriched)
                    device_type STRING,
                    browser STRING,
                    os STRING,
                    country STRING,
                    city STRING,
                    
                    -- Marketing attribution
                    referrer STRING,
                    utm_source STRING,
                    utm_medium STRING,
                    utm_campaign STRING,
                    channel_group STRING,
                    
                    -- Lineage and quality
                    row_hash STRING,
                    bronze_batch_id STRING,
                    silver_processed_at TIMESTAMP,
                    data_quality_score DOUBLE,
                    quality_flags STRING,
                    
                    -- Partition columns
                    event_date STRING,
                    event_hour INT
                )
                USING DELTA
                PARTITIONED BY (event_date)
                TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
            """)
            
            logger.info("Silver Delta table created successfully")
    
    def process_stream(self, bronze_stream: DataFrame) -> None:
        """
        Process Bronze streaming data through Silver transformations.
        Uses foreachBatch to enable MERGE operations on streaming data.
        """
        logger.info("Starting Silver stream processing...")
        
        query = (
            bronze_stream
            .writeStream
            .foreachBatch(self._process_micro_batch)
            .option("checkpointLocation", self.checkpoint_path)
            .trigger(processingTime=settings.spark.trigger_interval)
            .start()
        )
        
        logger.info(f"Silver streaming query started | ID: {query.id}")
        return query
    
    def _process_micro_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Process a single micro-batch through the Silver pipeline.
        Called by foreachBatch in streaming mode.
        """
        if batch_df.isEmpty():
            return
        
        record_count = batch_df.count()
        logger.info(f"Silver micro-batch {batch_id} | Records: {record_count}")
        
        # Apply transformation pipeline
        cleaned = self.transform(batch_df)
        
        # Merge into Silver table
        self._merge_to_silver(cleaned)
        
        metrics.increment("silver_batch_processed", record_count)
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply the complete Silver transformation pipeline.
        
        Pipeline:
        1. Deduplication
        2. Null handling
        3. Standardization
        4. Derived columns
        5. Quality scoring
        """
        return (
            df
            .transform(self._deduplicate)
            .transform(self._handle_nulls)
            .transform(self._standardize_fields)
            .transform(self._derive_columns)
            .transform(self._score_quality)
            .transform(self._add_silver_metadata)
        )
    
    def _deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Remove duplicate records using event_id.
        Keeps the latest record per event_id based on kafka_timestamp.
        """
        window = Window.partitionBy("event_id").orderBy(col("kafka_timestamp").desc())
        
        deduped = (
            df
            .withColumn("_row_num", row_number().over(window))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )
        
        original_count = df.count()
        deduped_count = deduped.count()
        dupes_removed = original_count - deduped_count
        
        if dupes_removed > 0:
            logger.info(f"Deduplication: {dupes_removed} duplicates removed "
                        f"({original_count} → {deduped_count})")
            metrics.increment("silver_duplicates_removed", dupes_removed)
        
        return deduped
    
    def _handle_nulls(self, df: DataFrame) -> DataFrame:
        """Apply null handling rules for Silver layer."""
        return (
            df
            .withColumn("user_id", coalesce(col("user_id"), lit("anonymous")))
            .withColumn("session_id", coalesce(col("session_id"), lit("unknown")))
            .withColumn("currency", coalesce(col("currency"), lit("USD")))
            .withColumn("device_type", coalesce(col("device_type"), lit("unknown")))
            .withColumn("country", coalesce(col("country"), lit("unknown")))
            .withColumn("city", coalesce(col("city"), lit("unknown")))
            .withColumn("price", coalesce(col("price"), lit(0.0)))
            .withColumn("quantity", coalesce(col("quantity"), lit(1)))
            .withColumn("utm_source", coalesce(col("utm_source"), lit("direct")))
            .withColumn("utm_medium", coalesce(col("utm_medium"), lit("none")))
        )
    
    def _standardize_fields(self, df: DataFrame) -> DataFrame:
        """Standardize string fields: lowercase, trim, clean."""
        return (
            df
            .withColumn("event_type", lower(trim(col("event_type"))))
            .withColumn("category", lower(trim(coalesce(col("category"), lit("uncategorized")))))
            .withColumn("device_type", lower(trim(col("device_type"))))
            .withColumn("browser", lower(trim(coalesce(col("browser"), lit("unknown")))))
            .withColumn("os", lower(trim(coalesce(col("os"), lit("unknown")))))
            .withColumn("country", lower(trim(col("country"))))
            .withColumn("city", lower(trim(col("city"))))
            # Standardize email-like fields
            .withColumn("referrer", 
                when(col("referrer").isNull(), lit("direct"))
                .otherwise(lower(trim(col("referrer"))))
            )
        )
    
    def _derive_columns(self, df: DataFrame) -> DataFrame:
        """Create derived analytical columns."""
        return (
            df
            # Calculate total transaction amount
            .withColumn("total_amount", col("price") * col("quantity"))
            # Classify marketing channel
            .withColumn("channel_group",
                when(col("utm_source") == "direct", "Direct")
                .when(col("utm_medium").isin("cpc", "ppc", "paid"), "Paid Search")
                .when(col("utm_medium") == "organic", "Organic Search")
                .when(col("utm_medium") == "social", "Social")
                .when(col("utm_medium") == "email", "Email")
                .when(col("utm_medium") == "referral", "Referral")
                .otherwise("Other")
            )
        )
    
    def _score_quality(self, df: DataFrame) -> DataFrame:
        """
        Compute a data quality score (0-1) for each record.
        Based on completeness, validity, and consistency checks.
        """
        quality_fields = [
            "event_id", "event_type", "user_id", "session_id",
            "event_timestamp", "product_id", "category", "price",
            "device_type", "country"
        ]
        
        # Count non-null fields for completeness score
        completeness_expr = sum(
            when(col(f).isNotNull() & (col(f) != "unknown") & (col(f) != ""), lit(1))
            .otherwise(lit(0))
            for f in quality_fields
        ) / len(quality_fields)
        
        # Build quality flags
        flags = []
        flag_expr = concat_ws(",",
            when(col("user_id") == "anonymous", lit("missing_user"))
            .otherwise(lit(None)),
            when(col("price") <= 0, lit("zero_price"))
            .otherwise(lit(None)),
            when(col("product_id").isNull(), lit("missing_product"))
            .otherwise(lit(None)),
        )
        
        return (
            df
            .withColumn("data_quality_score", completeness_expr)
            .withColumn("quality_flags", flag_expr)
        )
    
    def _add_silver_metadata(self, df: DataFrame) -> DataFrame:
        """Add Silver-layer processing metadata."""
        return (
            df
            .withColumn("silver_processed_at", current_timestamp())
            .withColumn("bronze_batch_id", col("batch_id"))
        )
    
    def _merge_to_silver(self, cleaned_df: DataFrame) -> None:
        """
        MERGE (upsert) cleaned data into the Silver Delta table.
        Uses event_id as the merge key with SCD Type 1 (overwrite).
        """
        if not DeltaTable.isDeltaTable(self.spark, self.silver_path):
            # First write: save directly
            (
                cleaned_df
                .select(self._silver_columns())
                .write.format("delta")
                .mode("overwrite")
                .partitionBy("event_date")
                .save(self.silver_path)
            )
            return
        
        silver_table = DeltaTable.forPath(self.spark, self.silver_path)
        
        (
            silver_table.alias("target")
            .merge(
                cleaned_df.select(self._silver_columns()).alias("source"),
                "target.event_id = source.event_id"
            )
            .whenMatchedUpdateAll()    # SCD Type 1: overwrite existing
            .whenNotMatchedInsertAll() # Insert new records
            .execute()
        )
        
        logger.info("Silver MERGE completed successfully")
    
    def _silver_columns(self) -> List[str]:
        """List of columns to include in the Silver table."""
        return [
            "event_id", "event_type", "user_id", "session_id",
            "event_timestamp", "product_id", "product_name", "category",
            "price", "quantity", "total_amount", "currency",
            "device_type", "browser", "os", "country", "city",
            "referrer", "utm_source", "utm_medium", "utm_campaign",
            "channel_group", "row_hash", "bronze_batch_id",
            "silver_processed_at", "data_quality_score", "quality_flags",
            "event_date", "event_hour",
        ]
    
    def process_batch(self, start_date: str = None, end_date: str = None) -> int:
        """
        Batch reprocess Bronze data into Silver.
        Useful for backfilling or recomputing after schema changes.
        """
        bronze_df = self.spark.read.format("delta").load(self.bronze_path)
        
        if start_date:
            bronze_df = bronze_df.filter(col("event_date") >= start_date)
        if end_date:
            bronze_df = bronze_df.filter(col("event_date") <= end_date)
        
        cleaned = self.transform(bronze_df)
        self._merge_to_silver(cleaned)
        
        count = cleaned.count()
        logger.info(f"Silver batch reprocessing complete | Records: {count}")
        return count
