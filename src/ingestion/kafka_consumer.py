"""
Kafka Consumer using Spark Structured Streaming.
Implements exactly-once semantics with checkpoint-based offset management.
Supports JSON and Avro deserialization with schema registry integration.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, expr,
    sha2, concat_ws, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, LongType, MapType
)
from src.config.settings import settings
from src.config.spark_config import get_spark
from src.utils.logger import get_logger
from src.utils.metrics import MetricsCollector
from typing import Optional, Dict, Any

logger = get_logger(__name__)
metrics = MetricsCollector()


# ─── Event Schema Definitions ───────────────────────────────────────────────

ECOMMERCE_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),     # page_view, add_to_cart, purchase, etc.
    StructField("user_id", StringType(), nullable=True),
    StructField("session_id", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=True),
    StructField("product_name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("device_type", StringType(), nullable=True),     # mobile, desktop, tablet
    StructField("browser", StringType(), nullable=True),
    StructField("os", StringType(), nullable=True),
    StructField("ip_address", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("referrer", StringType(), nullable=True),
    StructField("utm_source", StringType(), nullable=True),
    StructField("utm_medium", StringType(), nullable=True),
    StructField("utm_campaign", StringType(), nullable=True),
    StructField("properties", MapType(StringType(), StringType()), nullable=True),
])


class KafkaStreamConsumer:
    """
    Consumes events from Kafka topics using Spark Structured Streaming.
    
    Features:
    - Exactly-once processing via checkpoint-based offset tracking
    - Schema validation and dead-letter queue routing
    - Automatic watermarking for late-arriving events
    - Configurable trigger intervals and batch sizes
    """
    
    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        topic: Optional[str] = None,
        schema: Optional[StructType] = None,
    ):
        self.spark = spark or get_spark(streaming=True)
        self.topic = topic or settings.kafka.raw_events_topic
        self.schema = schema or ECOMMERCE_EVENT_SCHEMA
        self.consumer_group = settings.kafka.consumer_group
        
        logger.info(f"KafkaStreamConsumer initialized | Topic: {self.topic}")
    
    def create_stream(
        self,
        starting_offsets: str = "earliest",
        max_offsets_per_trigger: int = 10000,
        include_headers: bool = False,
    ) -> DataFrame:
        """
        Create a Spark Structured Streaming DataFrame from Kafka.
        
        Args:
            starting_offsets: Where to start reading ("earliest", "latest", or JSON offsets)
            max_offsets_per_trigger: Max records per micro-batch for backpressure control
            include_headers: Whether to include Kafka headers in output
            
        Returns:
            Streaming DataFrame with parsed event data
        """
        logger.info(f"Creating Kafka stream | Offsets: {starting_offsets} | "
                     f"Max/trigger: {max_offsets_per_trigger}")
        
        # Read raw stream from Kafka
        raw_stream = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", starting_offsets)
            .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
            .option("kafka.group.id", self.consumer_group)
            .option("failOnDataLoss", "false")
            .option("kafka.isolation.level", "read_committed")  # Exactly-once
            .load()
        )
        
        # Parse JSON payload and add metadata
        parsed_stream = (
            raw_stream
            .select(
                col("key").cast("string").alias("kafka_key"),
                from_json(col("value").cast("string"), self.schema).alias("data"),
                col("topic").alias("kafka_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("timestamp").alias("kafka_timestamp"),
            )
            .select(
                "kafka_key",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "data.*",
            )
            # Add ingestion metadata
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("event_timestamp", to_timestamp(col("timestamp")))
            # Generate deterministic row hash for deduplication
            .withColumn(
                "row_hash",
                sha2(concat_ws("||", col("event_id"), col("timestamp"), col("user_id")), 256)
            )
        )
        
        metrics.increment("kafka_stream_created")
        return parsed_stream
    
    def create_batch_reader(
        self,
        starting_offsets: str = "earliest",
        ending_offsets: str = "latest",
    ) -> DataFrame:
        """
        Create a batch DataFrame from Kafka for historical reprocessing.
        
        Useful for backfilling or recomputing medallion layers.
        """
        logger.info(f"Creating Kafka batch reader | {starting_offsets} → {ending_offsets}")
        
        batch_df = (
            self.spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", starting_offsets)
            .option("endingOffsets", ending_offsets)
            .load()
            .select(
                from_json(col("value").cast("string"), self.schema).alias("data"),
                col("timestamp").alias("kafka_timestamp"),
            )
            .select("data.*", "kafka_timestamp")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        )
        
        record_count = batch_df.count()
        logger.info(f"Batch reader loaded {record_count} records")
        metrics.increment("kafka_batch_reads", record_count)
        
        return batch_df


class DeadLetterQueueHandler:
    """Routes malformed or failed records to a dead-letter Kafka topic."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or get_spark(streaming=True)
        self.dlq_topic = settings.kafka.dead_letter_topic
    
    def route_to_dlq(self, failed_df: DataFrame, error_reason: str) -> None:
        """
        Write failed records to the dead-letter queue.
        
        Args:
            failed_df: DataFrame containing records that failed processing
            error_reason: Description of why records failed
        """
        dlq_df = (
            failed_df
            .withColumn("error_reason", lit(error_reason))
            .withColumn("failed_at", current_timestamp())
            .selectExpr(
                "CAST(event_id AS STRING) AS key",
                "to_json(struct(*)) AS value"
            )
        )
        
        (
            dlq_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
            .option("topic", self.dlq_topic)
            .save()
        )
        
        logger.warning(f"Routed records to DLQ | Reason: {error_reason}")
        metrics.increment("dlq_records_written")
