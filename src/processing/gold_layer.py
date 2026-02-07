"""
Gold Layer: Business-level aggregations and analytical data marts.

Responsibilities:
- Pre-computed aggregations for low-latency analytics
- Business KPI calculations (revenue, conversion rates, session metrics)
- Dimensional modeling (star schema denormalization)
- Incremental aggregation with merge-on-read
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, min as spark_min,
    max as spark_max, when, lit, current_timestamp, round as spark_round,
    window, collect_list, first, last, datediff, expr, coalesce,
    date_format, hour, percentile_approx
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from src.config.settings import settings
from src.config.spark_config import get_spark
from src.utils.logger import get_logger
from src.utils.metrics import MetricsCollector
from typing import Optional

logger = get_logger(__name__)
metrics = MetricsCollector()


class GoldLayer:
    """
    Gold layer processor creating business-ready analytical data marts.
    
    Aggregation Tables:
    1. Revenue Metrics (hourly/daily aggregations by dimensions)
    2. User Session Analytics (session duration, page depth, conversion)
    3. Product Performance (top products, category trends)
    4. Funnel Analysis (conversion funnel drop-off rates)
    5. Real-Time KPI Dashboard (rolling window aggregations)
    """
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or get_spark(streaming=True)
        self.gold_path = settings.delta.gold_path
        self.silver_path = settings.delta.silver_path
        self.checkpoint_path = f"{settings.spark.checkpoint_location}/gold"
        
        logger.info(f"GoldLayer initialized | Path: {self.gold_path}")
    
    # ─── Revenue Metrics ─────────────────────────────────────────────────
    
    def compute_revenue_metrics(self, silver_df: Optional[DataFrame] = None) -> DataFrame:
        """
        Compute hourly revenue aggregations by multiple dimensions.
        
        Metrics:
        - Total revenue, order count, AOV
        - Revenue by category, channel, device, country
        - Hourly/daily trends
        """
        df = silver_df or self.spark.read.format("delta").load(self.silver_path)
        
        # Filter to purchase events only
        purchases = df.filter(col("event_type") == "purchase")
        
        revenue_metrics = (
            purchases
            .groupBy(
                "event_date",
                "event_hour",
                "category",
                "channel_group",
                "device_type",
                "country",
                "currency",
            )
            .agg(
                spark_sum("total_amount").alias("total_revenue"),
                count("event_id").alias("order_count"),
                countDistinct("user_id").alias("unique_buyers"),
                avg("total_amount").alias("avg_order_value"),
                spark_sum("quantity").alias("total_items_sold"),
                spark_min("total_amount").alias("min_order_value"),
                spark_max("total_amount").alias("max_order_value"),
                percentile_approx("total_amount", 0.5).alias("median_order_value"),
            )
            .withColumn("revenue_per_buyer", 
                spark_round(col("total_revenue") / col("unique_buyers"), 2))
            .withColumn("items_per_order",
                spark_round(col("total_items_sold") / col("order_count"), 2))
            .withColumn("computed_at", current_timestamp())
        )
        
        # Write to Gold
        output_path = f"{self.gold_path}/revenue_metrics"
        self._write_gold_table(revenue_metrics, output_path, "event_date")
        
        logger.info(f"Revenue metrics computed | Records: {revenue_metrics.count()}")
        metrics.increment("gold_revenue_computed")
        
        return revenue_metrics
    
    # ─── User Session Analytics ──────────────────────────────────────────
    
    def compute_session_analytics(self, silver_df: Optional[DataFrame] = None) -> DataFrame:
        """
        Compute user session-level analytics.
        
        Metrics:
        - Session duration
        - Pages per session
        - Bounce rate
        - Conversion rate
        - Session-to-purchase funnel
        """
        df = silver_df or self.spark.read.format("delta").load(self.silver_path)
        
        # Session-level aggregation
        session_window = Window.partitionBy("session_id").orderBy("event_timestamp")
        
        session_events = (
            df
            .withColumn("session_start", first("event_timestamp").over(
                Window.partitionBy("session_id").orderBy("event_timestamp")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ))
            .withColumn("session_end", last("event_timestamp").over(
                Window.partitionBy("session_id").orderBy("event_timestamp")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ))
        )
        
        session_analytics = (
            session_events
            .groupBy("session_id", "user_id", "device_type", "channel_group",
                      "country", "event_date")
            .agg(
                first("session_start").alias("session_start"),
                first("session_end").alias("session_end"),
                count("event_id").alias("total_events"),
                countDistinct("event_type").alias("distinct_event_types"),
                count(when(col("event_type") == "page_view", 1)).alias("page_views"),
                count(when(col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
                count(when(col("event_type") == "purchase", 1)).alias("purchase_count"),
                spark_sum(when(col("event_type") == "purchase", col("total_amount"))
                    .otherwise(0)).alias("session_revenue"),
                collect_list("event_type").alias("event_sequence"),
            )
            .withColumn("session_duration_seconds",
                (col("session_end").cast("long") - col("session_start").cast("long")))
            .withColumn("is_bounce",
                when(col("total_events") == 1, lit(True)).otherwise(lit(False)))
            .withColumn("has_conversion",
                when(col("purchase_count") > 0, lit(True)).otherwise(lit(False)))
            .withColumn("computed_at", current_timestamp())
        )
        
        # Write to Gold
        output_path = f"{self.gold_path}/session_analytics"
        self._write_gold_table(session_analytics, output_path, "event_date")
        
        logger.info(f"Session analytics computed | Sessions: {session_analytics.count()}")
        metrics.increment("gold_sessions_computed")
        
        return session_analytics
    
    # ─── Product Performance ─────────────────────────────────────────────
    
    def compute_product_performance(self, silver_df: Optional[DataFrame] = None) -> DataFrame:
        """
        Compute product-level performance metrics.
        
        Metrics:
        - Revenue per product
        - View-to-purchase conversion rate
        - Average selling price
        - Category rankings
        """
        df = silver_df or self.spark.read.format("delta").load(self.silver_path)
        
        product_metrics = (
            df
            .filter(col("product_id").isNotNull())
            .groupBy("product_id", "product_name", "category", "event_date")
            .agg(
                count(when(col("event_type") == "page_view", 1)).alias("views"),
                count(when(col("event_type") == "add_to_cart", 1)).alias("adds_to_cart"),
                count(when(col("event_type") == "purchase", 1)).alias("purchases"),
                spark_sum(when(col("event_type") == "purchase", col("total_amount"))
                    .otherwise(0)).alias("total_revenue"),
                avg(when(col("event_type") == "purchase", col("price")))
                    .alias("avg_selling_price"),
                countDistinct("user_id").alias("unique_users"),
            )
            .withColumn("view_to_cart_rate",
                spark_round(
                    when(col("views") > 0, col("adds_to_cart") / col("views"))
                    .otherwise(0), 4
                ))
            .withColumn("cart_to_purchase_rate",
                spark_round(
                    when(col("adds_to_cart") > 0, col("purchases") / col("adds_to_cart"))
                    .otherwise(0), 4
                ))
            .withColumn("view_to_purchase_rate",
                spark_round(
                    when(col("views") > 0, col("purchases") / col("views"))
                    .otherwise(0), 4
                ))
            .withColumn("computed_at", current_timestamp())
        )
        
        output_path = f"{self.gold_path}/product_performance"
        self._write_gold_table(product_metrics, output_path, "event_date")
        
        logger.info(f"Product performance computed | Products: {product_metrics.count()}")
        return product_metrics
    
    # ─── Funnel Analysis ─────────────────────────────────────────────────
    
    def compute_funnel_analysis(self, silver_df: Optional[DataFrame] = None) -> DataFrame:
        """
        Compute conversion funnel metrics.
        
        Funnel stages: page_view → add_to_cart → checkout → purchase
        """
        df = silver_df or self.spark.read.format("delta").load(self.silver_path)
        
        funnel = (
            df
            .groupBy("event_date", "channel_group", "device_type")
            .agg(
                countDistinct(when(col("event_type") == "page_view", col("user_id")))
                    .alias("stage_1_viewers"),
                countDistinct(when(col("event_type") == "add_to_cart", col("user_id")))
                    .alias("stage_2_cart_adders"),
                countDistinct(when(col("event_type") == "checkout", col("user_id")))
                    .alias("stage_3_checkout"),
                countDistinct(when(col("event_type") == "purchase", col("user_id")))
                    .alias("stage_4_purchasers"),
            )
            .withColumn("view_to_cart_rate",
                spark_round(col("stage_2_cart_adders") / col("stage_1_viewers"), 4))
            .withColumn("cart_to_checkout_rate",
                spark_round(
                    when(col("stage_2_cart_adders") > 0,
                         col("stage_3_checkout") / col("stage_2_cart_adders"))
                    .otherwise(0), 4))
            .withColumn("checkout_to_purchase_rate",
                spark_round(
                    when(col("stage_3_checkout") > 0,
                         col("stage_4_purchasers") / col("stage_3_checkout"))
                    .otherwise(0), 4))
            .withColumn("overall_conversion_rate",
                spark_round(
                    when(col("stage_1_viewers") > 0,
                         col("stage_4_purchasers") / col("stage_1_viewers"))
                    .otherwise(0), 4))
            .withColumn("computed_at", current_timestamp())
        )
        
        output_path = f"{self.gold_path}/funnel_analysis"
        self._write_gold_table(funnel, output_path, "event_date")
        
        logger.info(f"Funnel analysis computed | Records: {funnel.count()}")
        return funnel
    
    # ─── Real-Time KPI Dashboard ─────────────────────────────────────────
    
    def compute_realtime_kpis(self, silver_stream: DataFrame) -> None:
        """
        Compute rolling-window KPIs for the real-time dashboard.
        Uses Spark Structured Streaming with tumbling windows.
        """
        logger.info("Starting real-time KPI computation...")
        
        kpis = (
            silver_stream
            .withWatermark("event_timestamp", settings.spark.watermark_delay)
            .groupBy(
                window(col("event_timestamp"), "5 minutes", "1 minute"),
                "event_type",
            )
            .agg(
                count("event_id").alias("event_count"),
                countDistinct("user_id").alias("active_users"),
                spark_sum(when(col("event_type") == "purchase", col("total_amount"))
                    .otherwise(0)).alias("window_revenue"),
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "event_type", "event_count", "active_users", "window_revenue",
            )
        )
        
        query = (
            kpis.writeStream
            .format("delta")
            .outputMode("update")
            .option("checkpointLocation", f"{self.checkpoint_path}/realtime_kpis")
            .trigger(processingTime="30 seconds")
            .start(f"{self.gold_path}/realtime_kpis")
        )
        
        logger.info(f"Real-time KPI stream started | ID: {query.id}")
        return query
    
    # ─── Utilities ───────────────────────────────────────────────────────
    
    def _write_gold_table(
        self, df: DataFrame, path: str, partition_col: str
    ) -> None:
        """Write or merge DataFrame into a Gold Delta table."""
        if DeltaTable.isDeltaTable(self.spark, path):
            # Overwrite partition-level for incremental refresh
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", 
                    f"{partition_col} >= '{self._get_min_partition(df, partition_col)}'")
                .save(path)
            )
        else:
            (
                df.write.format("delta")
                .mode("overwrite")
                .partitionBy(partition_col)
                .save(path)
            )
    
    @staticmethod
    def _get_min_partition(df: DataFrame, col_name: str) -> str:
        """Get the minimum partition value from a DataFrame."""
        return df.agg(spark_min(col(col_name))).collect()[0][0]
    
    def compute_all(self) -> dict:
        """Run all Gold layer computations. Typically called by the orchestrator."""
        logger.info("Computing all Gold aggregations...")
        
        silver_df = self.spark.read.format("delta").load(self.silver_path)
        
        results = {
            "revenue_metrics": self.compute_revenue_metrics(silver_df).count(),
            "session_analytics": self.compute_session_analytics(silver_df).count(),
            "product_performance": self.compute_product_performance(silver_df).count(),
            "funnel_analysis": self.compute_funnel_analysis(silver_df).count(),
        }
        
        logger.info(f"All Gold aggregations complete: {results}")
        return results
