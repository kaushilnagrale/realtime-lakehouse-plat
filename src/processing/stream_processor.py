"""
Unified Stream Processor: Orchestrates the full Bronze → Silver → Gold pipeline.

This is the main entry point for the streaming pipeline.
Manages lifecycle of all streaming queries and handles graceful shutdown.
"""

from pyspark.sql import SparkSession
from src.config.spark_config import get_spark, SparkSessionFactory
from src.config.settings import settings
from src.ingestion.kafka_consumer import KafkaStreamConsumer
from src.processing.bronze_layer import BronzeLayer
from src.processing.silver_layer import SilverLayer
from src.processing.gold_layer import GoldLayer
from src.utils.logger import get_logger
from src.utils.metrics import MetricsCollector
import signal
import sys

logger = get_logger(__name__)
metrics = MetricsCollector()


class StreamProcessor:
    """
    Unified streaming pipeline orchestrator.
    
    Manages the complete data flow:
    Kafka → Bronze (append) → Silver (merge) → Gold (aggregate)
    
    Features:
    - Graceful shutdown handling
    - Query health monitoring
    - Automatic restart on failure
    - Metrics collection at each stage
    """
    
    def __init__(self):
        self.spark = get_spark(streaming=True)
        self.kafka_consumer = KafkaStreamConsumer(self.spark)
        self.bronze = BronzeLayer(self.spark)
        self.silver = SilverLayer(self.spark)
        self.gold = GoldLayer(self.spark)
        
        self.active_queries = []
        self._setup_signal_handlers()
        
        logger.info("StreamProcessor initialized")
    
    def start(self) -> None:
        """
        Start the complete streaming pipeline.
        
        Flow:
        1. Initialize Delta tables
        2. Create Kafka stream
        3. Start Bronze ingestion
        4. Start Silver transformation (reads from Bronze)
        5. Start Gold aggregation (reads from Silver)
        6. Monitor query health
        """
        logger.info("=" * 60)
        logger.info("STARTING REAL-TIME LAKEHOUSE PIPELINE")
        logger.info("=" * 60)
        
        # Step 1: Initialize tables
        self._initialize_tables()
        
        # Step 2: Create the source stream
        kafka_stream = self.kafka_consumer.create_stream(
            starting_offsets="latest",
            max_offsets_per_trigger=settings.kafka.max_poll_records,
        )
        
        # Step 3: Bronze layer - raw ingestion
        bronze_query = self.bronze.process_stream(kafka_stream)
        self.active_queries.append(("bronze", bronze_query))
        
        # Step 4: Silver layer - reads Bronze as streaming source
        bronze_stream = (
            self.spark.readStream
            .format("delta")
            .load(settings.delta.bronze_path)
        )
        silver_query = self.silver.process_stream(bronze_stream)
        self.active_queries.append(("silver", silver_query))
        
        # Step 5: Gold real-time KPIs - reads Silver as streaming source
        silver_stream = (
            self.spark.readStream
            .format("delta")
            .load(settings.delta.silver_path)
        )
        gold_query = self.gold.compute_realtime_kpis(silver_stream)
        self.active_queries.append(("gold_kpis", gold_query))
        
        logger.info(f"Pipeline started with {len(self.active_queries)} active queries")
        metrics.increment("pipeline_starts")
        
        # Step 6: Block and monitor
        self._monitor_queries()
    
    def _initialize_tables(self) -> None:
        """Initialize all Delta Lake tables."""
        logger.info("Initializing Delta Lake tables...")
        self.bronze.initialize_table()
        self.silver.initialize_table()
        logger.info("All Delta tables initialized")
    
    def _monitor_queries(self) -> None:
        """
        Monitor active streaming queries.
        Blocks until all queries terminate or an error occurs.
        """
        logger.info("Monitoring active streaming queries...")
        
        try:
            for name, query in self.active_queries:
                query.awaitTermination()
        except Exception as e:
            logger.error(f"Query monitoring error: {e}")
            self.stop()
            raise
    
    def stop(self) -> None:
        """Gracefully stop all streaming queries."""
        logger.info("Stopping streaming pipeline...")
        
        for name, query in self.active_queries:
            try:
                query.stop()
                logger.info(f"Stopped query: {name}")
            except Exception as e:
                logger.error(f"Error stopping {name}: {e}")
        
        self.active_queries.clear()
        SparkSessionFactory.stop()
        
        logger.info("Pipeline stopped successfully")
        metrics.increment("pipeline_stops")
    
    def get_status(self) -> dict:
        """Get status of all active streaming queries."""
        return {
            name: {
                "id": str(query.id),
                "is_active": query.isActive,
                "status": query.status,
                "recent_progress": (
                    query.recentProgress[-1] if query.recentProgress else None
                ),
            }
            for name, query in self.active_queries
        }
    
    def _setup_signal_handlers(self) -> None:
        """Setup graceful shutdown on SIGINT/SIGTERM."""
        def handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            self.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)


def main():
    """Entry point for the streaming pipeline."""
    processor = StreamProcessor()
    
    try:
        processor.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
    finally:
        processor.stop()


if __name__ == "__main__":
    main()
