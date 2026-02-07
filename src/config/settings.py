"""
Centralized configuration management using Pydantic Settings.
All environment variables are validated at startup with sensible defaults.
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
from enum import Enum


class Environment(str, Enum):
    LOCAL = "local"
    DEV = "dev"
    STAGING = "staging"
    PRODUCTION = "production"


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = Field(default="localhost:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    schema_registry_url: str = Field(default="http://localhost:8081", env="SCHEMA_REGISTRY_URL")
    consumer_group: str = Field(default="lakehouse-consumer-group", env="KAFKA_CONSUMER_GROUP")
    auto_offset_reset: str = Field(default="earliest", env="KAFKA_AUTO_OFFSET_RESET")
    max_poll_records: int = Field(default=500, env="KAFKA_MAX_POLL_RECORDS")
    
    # Topics
    raw_events_topic: str = Field(default="ecommerce_events", env="KAFKA_RAW_EVENTS_TOPIC")
    dead_letter_topic: str = Field(default="dead_letter_queue", env="KAFKA_DLQ_TOPIC")


class SparkSettings(BaseSettings):
    master: str = Field(default="local[*]", env="SPARK_MASTER")
    app_name: str = Field(default="LakehousePipeline", env="SPARK_APP_NAME")
    
    # Memory configuration
    driver_memory: str = Field(default="4g", env="SPARK_DRIVER_MEMORY")
    executor_memory: str = Field(default="4g", env="SPARK_EXECUTOR_MEMORY")
    executor_cores: int = Field(default=2, env="SPARK_EXECUTOR_CORES")
    
    # Streaming configuration
    trigger_interval: str = Field(default="10 seconds", env="SPARK_TRIGGER_INTERVAL")
    checkpoint_location: str = Field(default="/data/checkpoints", env="SPARK_CHECKPOINT_LOCATION")
    watermark_delay: str = Field(default="10 minutes", env="SPARK_WATERMARK_DELAY")
    
    # Shuffle partitions
    shuffle_partitions: int = Field(default=200, env="SPARK_SHUFFLE_PARTITIONS")


class DeltaLakeSettings(BaseSettings):
    base_path: str = Field(default="/data/lakehouse", env="DELTA_LAKE_PATH")
    
    # Layer paths (derived from base_path)
    @property
    def bronze_path(self) -> str:
        return f"{self.base_path}/bronze"
    
    @property
    def silver_path(self) -> str:
        return f"{self.base_path}/silver"
    
    @property
    def gold_path(self) -> str:
        return f"{self.base_path}/gold"
    
    # Optimization settings
    optimize_frequency_hours: int = Field(default=6, env="DELTA_OPTIMIZE_FREQUENCY_HOURS")
    vacuum_retention_hours: int = Field(default=168, env="DELTA_VACUUM_RETENTION_HOURS")  # 7 days
    z_order_columns: str = Field(default="event_date,user_id", env="DELTA_ZORDER_COLUMNS")
    
    # Compaction
    target_file_size_mb: int = Field(default=128, env="DELTA_TARGET_FILE_SIZE_MB")


class APISettings(BaseSettings):
    host: str = Field(default="0.0.0.0", env="API_HOST")
    port: int = Field(default=8000, env="API_PORT")
    workers: int = Field(default=4, env="API_WORKERS")
    cors_origins: str = Field(default="*", env="API_CORS_ORIGINS")
    rate_limit_per_minute: int = Field(default=100, env="API_RATE_LIMIT")


class MonitoringSettings(BaseSettings):
    prometheus_port: int = Field(default=9090, env="PROMETHEUS_PORT")
    grafana_port: int = Field(default=3000, env="GRAFANA_PORT")
    metrics_prefix: str = Field(default="lakehouse", env="METRICS_PREFIX")
    health_check_interval: int = Field(default=30, env="HEALTH_CHECK_INTERVAL")


class Settings(BaseSettings):
    """Root settings aggregating all configuration sections."""
    
    environment: Environment = Field(default=Environment.LOCAL, env="ENVIRONMENT")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    debug: bool = Field(default=False, env="DEBUG")
    
    kafka: KafkaSettings = KafkaSettings()
    spark: SparkSettings = SparkSettings()
    delta: DeltaLakeSettings = DeltaLakeSettings()
    api: APISettings = APISettings()
    monitoring: MonitoringSettings = MonitoringSettings()
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Singleton instance
settings = Settings()
