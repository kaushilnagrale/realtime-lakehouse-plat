"""
Unit tests for the Lakehouse Platform.
Tests data quality, transformations, and schema validation.
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


# ─── Fixtures ────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Create a test Spark session."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("LakehouseTests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_events(spark):
    """Create a sample events DataFrame."""
    data = [
        ("evt-001", "page_view", "user_001", "sess_001", "2024-01-15T10:00:00Z",
         "PROD-E01", "Wireless Headphones", "electronics", 79.99, 1, "USD",
         "mobile", "chrome", "iOS", "US", "New York", "google.com",
         "google", "cpc", "summer_sale"),
        ("evt-002", "add_to_cart", "user_001", "sess_001", "2024-01-15T10:05:00Z",
         "PROD-E01", "Wireless Headphones", "electronics", 79.99, 1, "USD",
         "mobile", "chrome", "iOS", "US", "New York", "google.com",
         "google", "cpc", "summer_sale"),
        ("evt-003", "purchase", "user_001", "sess_001", "2024-01-15T10:10:00Z",
         "PROD-E01", "Wireless Headphones", "electronics", 79.99, 2, "USD",
         "mobile", "chrome", "iOS", "US", "New York", "google.com",
         "google", "cpc", "summer_sale"),
        ("evt-004", "page_view", "user_002", "sess_002", "2024-01-15T11:00:00Z",
         None, None, None, None, None, None,
         "desktop", "firefox", "Windows", "UK", "London", None,
         "direct", "none", None),
        # Duplicate event
        ("evt-001", "page_view", "user_001", "sess_001", "2024-01-15T10:00:00Z",
         "PROD-E01", "Wireless Headphones", "electronics", 79.99, 1, "USD",
         "mobile", "chrome", "iOS", "US", "New York", "google.com",
         "google", "cpc", "summer_sale"),
    ]
    
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("user_id", StringType()),
        StructField("session_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("price", DoubleType()),
        StructField("quantity", IntegerType()),
        StructField("currency", StringType()),
        StructField("device_type", StringType()),
        StructField("browser", StringType()),
        StructField("os", StringType()),
        StructField("country", StringType()),
        StructField("city", StringType()),
        StructField("referrer", StringType()),
        StructField("utm_source", StringType()),
        StructField("utm_medium", StringType()),
        StructField("utm_campaign", StringType()),
    ])
    
    return spark.createDataFrame(data, schema)


# ─── Schema Registry Tests ──────────────────────────────────────────────

class TestSchemaRegistry:
    
    def test_register_new_schema(self):
        from src.ingestion.schema_registry import SchemaRegistry
        
        registry = SchemaRegistry()
        schema = StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=True),
        ])
        
        version = registry.register_schema("test_subject", schema)
        assert version.version == 1
        assert version.compatibility == "BACKWARD"
    
    def test_schema_unchanged_returns_same_version(self):
        from src.ingestion.schema_registry import SchemaRegistry
        
        registry = SchemaRegistry()
        schema = StructType([StructField("id", StringType(), nullable=False)])
        
        v1 = registry.register_schema("test", schema)
        v2 = registry.register_schema("test", schema)
        
        assert v1.version == v2.version  # Same schema, no new version
    
    def test_backward_incompatible_raises_error(self):
        from src.ingestion.schema_registry import SchemaRegistry, SchemaCompatibilityError
        
        registry = SchemaRegistry()
        v1_schema = StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
        ])
        v2_schema = StructType([
            StructField("id", StringType(), nullable=False),
            # Removed 'name' which is required
        ])
        
        registry.register_schema("test", v1_schema)
        
        with pytest.raises(SchemaCompatibilityError):
            registry.register_schema("test", v2_schema, compatibility="BACKWARD")


# ─── Data Quality Tests ─────────────────────────────────────────────────

class TestDataQuality:
    
    def test_not_null_check_passes(self, spark, sample_events):
        from src.utils.data_quality import DataQualityValidator, QualityLevel
        
        validator = DataQualityValidator("test")
        validator.add_not_null_check("event_id", QualityLevel.CRITICAL, threshold=0.99)
        
        report = validator.validate(sample_events)
        assert report.overall_pass is True
        assert report.checks_passed == 1
    
    def test_not_null_check_fails_for_nullable_column(self, spark, sample_events):
        from src.utils.data_quality import DataQualityValidator, QualityLevel
        
        validator = DataQualityValidator("test")
        # product_id is null for some events
        validator.add_not_null_check("product_id", QualityLevel.CRITICAL, threshold=1.0)
        
        report = validator.validate(sample_events)
        assert report.overall_pass is False
    
    def test_uniqueness_check(self, spark, sample_events):
        from src.utils.data_quality import DataQualityValidator, QualityLevel
        
        validator = DataQualityValidator("test")
        # event_id has a duplicate (evt-001 appears twice)
        validator.add_uniqueness_check("event_id", QualityLevel.WARNING, threshold=1.0)
        
        report = validator.validate(sample_events)
        assert report.checks_warned == 1


# ─── Metrics Tests ───────────────────────────────────────────────────────

class TestMetrics:
    
    def test_counter_increment(self):
        from src.utils.metrics import MetricsCollector
        
        # Reset singleton for test isolation
        MetricsCollector._instance = None
        metrics = MetricsCollector()
        metrics.reset()
        
        metrics.increment("test_counter")
        metrics.increment("test_counter", 5)
        
        assert metrics.get_counter("test_counter") == 6
    
    def test_gauge_set(self):
        from src.utils.metrics import MetricsCollector
        
        MetricsCollector._instance = None
        metrics = MetricsCollector()
        metrics.reset()
        
        metrics.set_gauge("test_gauge", 42.5)
        assert metrics.get_gauge("test_gauge") == 42.5
    
    def test_prometheus_export(self):
        from src.utils.metrics import MetricsCollector
        
        MetricsCollector._instance = None
        metrics = MetricsCollector()
        metrics.reset()
        
        metrics.increment("requests", 100)
        metrics.set_gauge("active_connections", 5)
        
        output = metrics.export_prometheus()
        assert "lakehouse_requests_total 100" in output
        assert "lakehouse_active_connections 5" in output


# ─── Sample Data Generation Tests ────────────────────────────────────────

class TestSampleDataGeneration:
    
    def test_generate_event_has_required_fields(self):
        from scripts.produce_sample_data import generate_event
        
        event = generate_event()
        
        assert "event_id" in event
        assert "event_type" in event
        assert "user_id" in event
        assert "timestamp" in event
        assert event["event_type"] in [
            "page_view", "add_to_cart", "remove_from_cart",
            "checkout", "purchase", "search"
        ]
    
    def test_generate_multiple_events(self):
        from scripts.produce_sample_data import generate_events
        
        events = list(generate_events(100))
        assert len(events) == 100
        
        # All event IDs should be unique
        ids = [e["event_id"] for e in events]
        assert len(set(ids)) == 100


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
