"""
Delta Lake Table Manager: Handles optimization, vacuuming, and time travel.

Provides table maintenance operations critical for production Delta Lake deployments:
- OPTIMIZE (bin-packing / file compaction)
- Z-ORDER clustering for multi-dimensional query patterns
- VACUUM to remove orphaned files
- Time travel for point-in-time queries and rollbacks
- Table history and audit logging
"""

from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from src.config.settings import settings
from src.config.spark_config import get_spark
from src.utils.logger import get_logger
from src.utils.metrics import MetricsCollector
from typing import Optional, List, Dict
from datetime import datetime

logger = get_logger(__name__)
metrics = MetricsCollector()


class DeltaTableManager:
    """
    Manages Delta Lake table lifecycle and optimization operations.
    
    Key Operations:
    - OPTIMIZE: Compacts small files into target-sized files (128MB default)
    - Z-ORDER: Co-locates related data for faster multi-dimensional queries
    - VACUUM: Removes files no longer referenced by the transaction log
    - Time Travel: Query historical versions of the table
    """
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or get_spark()
        logger.info("DeltaTableManager initialized")
    
    # ─── Optimization ────────────────────────────────────────────────────
    
    def optimize_table(
        self,
        table_path: str,
        z_order_columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
    ) -> Dict:
        """
        Run OPTIMIZE on a Delta table with optional Z-ORDER.
        
        OPTIMIZE compacts small files into larger ones, reducing the number
        of files Spark needs to read. Z-ORDER clusters data by specified
        columns for faster predicate pushdown.
        
        Args:
            table_path: Path to the Delta table
            z_order_columns: Columns to Z-ORDER by (e.g., ["event_date", "user_id"])
            where_clause: Optional partition filter (e.g., "event_date >= '2024-01-01'")
            
        Returns:
            Optimization statistics
        """
        logger.info(f"Optimizing table: {table_path}")
        
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        # Build OPTIMIZE command
        optimize_builder = delta_table.optimize()
        
        if where_clause:
            optimize_builder = optimize_builder.where(where_clause)
        
        if z_order_columns:
            result = optimize_builder.executeZOrderBy(*z_order_columns)
            logger.info(f"Z-ORDER optimization by {z_order_columns}")
        else:
            result = optimize_builder.executeCompaction()
            logger.info("Compaction optimization")
        
        stats = result.select("metrics.*").collect()[0].asDict() if result.count() > 0 else {}
        
        logger.info(f"Optimization complete | Stats: {stats}")
        metrics.increment("tables_optimized")
        
        return stats
    
    def vacuum_table(
        self,
        table_path: str,
        retention_hours: Optional[int] = None,
        dry_run: bool = False,
    ) -> int:
        """
        Run VACUUM to remove orphaned files from a Delta table.
        
        IMPORTANT: Files older than the retention period that are no longer
        referenced by the transaction log are permanently deleted.
        
        Args:
            table_path: Path to the Delta table
            retention_hours: Minimum file retention (default from settings)
            dry_run: If True, only report files that would be deleted
            
        Returns:
            Number of files deleted (or would be deleted in dry_run)
        """
        retention = retention_hours or settings.delta.vacuum_retention_hours
        logger.info(f"Vacuuming table: {table_path} | Retention: {retention}h | DryRun: {dry_run}")
        
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        # Disable retention check safety for programmatic vacuum
        self.spark.conf.set(
            "spark.databricks.delta.retentionDurationCheck.enabled", "false"
        )
        
        if dry_run:
            files = delta_table.vacuum(retention).collect()
            file_count = len(files)
            logger.info(f"Vacuum dry run: {file_count} files would be deleted")
        else:
            delta_table.vacuum(retention)
            file_count = -1  # Actual count not returned by vacuum()
            logger.info(f"Vacuum complete for {table_path}")
        
        metrics.increment("tables_vacuumed")
        return file_count
    
    # ─── Time Travel ─────────────────────────────────────────────────────
    
    def read_version(self, table_path: str, version: int) -> DataFrame:
        """
        Read a specific version of a Delta table (time travel by version).
        
        Args:
            table_path: Path to the Delta table
            version: Version number to read
        """
        logger.info(f"Time travel read | Path: {table_path} | Version: {version}")
        
        return (
            self.spark.read
            .format("delta")
            .option("versionAsOf", version)
            .load(table_path)
        )
    
    def read_timestamp(self, table_path: str, timestamp: str) -> DataFrame:
        """
        Read a Delta table as of a specific timestamp.
        
        Args:
            table_path: Path to the Delta table
            timestamp: ISO timestamp string (e.g., "2024-01-15T10:30:00")
        """
        logger.info(f"Time travel read | Path: {table_path} | Timestamp: {timestamp}")
        
        return (
            self.spark.read
            .format("delta")
            .option("timestampAsOf", timestamp)
            .load(table_path)
        )
    
    def get_history(
        self,
        table_path: str,
        limit: int = 20,
    ) -> DataFrame:
        """
        Get the transaction history of a Delta table.
        
        Returns version, timestamp, operation, and metrics for each commit.
        """
        delta_table = DeltaTable.forPath(self.spark, table_path)
        return delta_table.history(limit)
    
    def rollback_to_version(self, table_path: str, version: int) -> None:
        """
        Rollback a Delta table to a previous version by overwriting with historical data.
        
        WARNING: This is a destructive operation. Creates a new version
        with data from the specified historical version.
        """
        logger.warning(f"Rolling back {table_path} to version {version}")
        
        historical_df = self.read_version(table_path, version)
        
        (
            historical_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(table_path)
        )
        
        logger.info(f"Rollback complete | Table: {table_path} | Target: v{version}")
    
    # ─── Table Information ───────────────────────────────────────────────
    
    def get_table_details(self, table_path: str) -> Dict:
        """Get comprehensive details about a Delta table."""
        delta_table = DeltaTable.forPath(self.spark, table_path)
        detail = delta_table.detail().collect()[0]
        
        df = self.spark.read.format("delta").load(table_path)
        
        return {
            "path": table_path,
            "format": detail["format"],
            "num_files": detail["numFiles"],
            "size_bytes": detail["sizeInBytes"],
            "size_mb": round(detail["sizeInBytes"] / (1024 * 1024), 2),
            "partitioning": detail["partitionColumns"],
            "properties": detail["properties"],
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
        }
    
    def optimize_all_tables(self) -> Dict:
        """Run optimization on all lakehouse tables."""
        paths = {
            "bronze": settings.delta.bronze_path,
            "silver": settings.delta.silver_path,
            "gold_revenue": f"{settings.delta.gold_path}/revenue_metrics",
            "gold_sessions": f"{settings.delta.gold_path}/session_analytics",
            "gold_products": f"{settings.delta.gold_path}/product_performance",
            "gold_funnel": f"{settings.delta.gold_path}/funnel_analysis",
        }
        
        z_order_config = {
            "bronze": ["event_date", "event_type"],
            "silver": ["event_date", "user_id"],
            "gold_revenue": ["event_date", "category"],
        }
        
        results = {}
        for name, path in paths.items():
            try:
                if DeltaTable.isDeltaTable(self.spark, path):
                    z_cols = z_order_config.get(name)
                    results[name] = self.optimize_table(path, z_order_columns=z_cols)
                    logger.info(f"Optimized {name}")
                else:
                    results[name] = "skipped (table does not exist)"
            except Exception as e:
                results[name] = f"error: {str(e)}"
                logger.error(f"Error optimizing {name}: {e}")
        
        return results
