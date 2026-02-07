# 🏠 Real-Time Analytics Lakehouse Platform

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-00ADD8.svg)](https://delta.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade **real-time analytics lakehouse platform** built on the **Medallion Architecture** (Bronze → Silver → Gold) using Apache Spark Structured Streaming, Delta Lake, Apache Kafka, and a FastAPI serving layer. Designed for sub-second data ingestion, ACID-compliant transformations, and low-latency analytical queries.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        REAL-TIME LAKEHOUSE PLATFORM                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────────┐   │
│  │  DATA SOURCES │    │  INGESTION   │    │      MEDALLION LAYERS        │   │
│  │              │    │   LAYER      │    │                              │   │
│  │ Kafka Topics ├───►│ Spark Struct ├───►│  Bronze ─► Silver ─► Gold   │   │
│  │ REST API     │    │  Streaming   │    │  (Raw)    (Clean)  (Agg)    │   │
│  │ CSV/JSON     │    │ Schema Reg.  │    │       Delta Lake Tables      │   │
│  └──────────────┘    └──────────────┘    └──────────┬───────────────────┘   │
│                                                      │                      │
│  ┌──────────────────────────────────────────────────┴───────────────────┐   │
│  │                        SERVING LAYER                                 │   │
│  │  FastAPI REST ─── GraphQL ─── WebSocket (Live Dashboard)             │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  ORCHESTRATION: Airflow DAGs │ MONITORING: Prometheus + Grafana      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## ✨ Key Features

- **Real-Time Ingestion**: Apache Kafka + Spark Structured Streaming with exactly-once semantics
- **Medallion Architecture**: Bronze (raw), Silver (cleaned/validated), Gold (aggregated) Delta Lake layers
- **ACID Transactions**: Delta Lake provides ACID compliance, time travel, and schema enforcement
- **Stream-Batch Unification**: Single codebase handles both streaming and batch workloads
- **Low-Latency Serving**: FastAPI with WebSocket support for live analytics dashboards
- **Schema Evolution**: Automatic schema registry with backward/forward compatibility
- **Data Quality**: Great Expectations-based validation at each medallion layer
- **Orchestration**: Apache Airflow DAGs for batch ETL and maintenance jobs
- **Monitoring**: Prometheus metrics + Grafana dashboards for pipeline health
- **Infrastructure as Code**: Docker Compose for local dev, Terraform-ready for cloud

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Stream Processing | Apache Spark 3.5 (Structured Streaming) |
| Storage Layer | Delta Lake 3.0 (Parquet + Transaction Log) |
| Message Broker | Apache Kafka 3.6 |
| Schema Registry | Confluent Schema Registry |
| Serving API | FastAPI + Uvicorn |
| Orchestration | Apache Airflow 2.8 |
| Data Quality | Great Expectations |
| Monitoring | Prometheus + Grafana |
| Containerization | Docker + Docker Compose |
| Language | Python 3.10+ |

## 📂 Project Structure

```
realtime-lakehouse-platform/
├── src/
│   ├── ingestion/              # Data ingestion from Kafka, REST, files
│   │   ├── kafka_consumer.py   # Spark Structured Streaming from Kafka
│   │   ├── rest_ingestion.py   # REST API-based data ingestion
│   │   ├── file_ingestion.py   # Batch file ingestion (CSV/JSON/Parquet)
│   │   └── schema_registry.py  # Schema validation and evolution
│   ├── processing/             # Medallion layer transformations
│   │   ├── bronze_layer.py     # Raw data landing with metadata
│   │   ├── silver_layer.py     # Cleansed, deduplicated, validated data
│   │   ├── gold_layer.py       # Business-level aggregations
│   │   └── stream_processor.py # Unified stream processing engine
│   ├── storage/                # Delta Lake table management
│   │   ├── delta_manager.py    # Table creation, optimization, vacuuming
│   │   ├── partition_manager.py# Dynamic partitioning strategies
│   │   └── time_travel.py      # Point-in-time queries and rollbacks
│   ├── serving/                # Analytics API layer
│   │   ├── api.py              # FastAPI REST endpoints
│   │   ├── websocket.py        # Real-time WebSocket streaming
│   │   └── query_engine.py     # Optimized query execution
│   ├── orchestration/          # Workflow management
│   │   ├── dag_definitions.py  # Airflow DAG definitions
│   │   └── task_definitions.py # Reusable Airflow tasks
│   ├── utils/                  # Shared utilities
│   │   ├── logger.py           # Structured logging
│   │   ├── metrics.py          # Prometheus metrics collectors
│   │   └── data_quality.py     # Great Expectations validators
│   └── config/                 # Configuration management
│       ├── settings.py         # Pydantic settings with env vars
│       └── spark_config.py     # Spark session configuration
├── tests/                      # Unit and integration tests
├── scripts/                    # Deployment and utility scripts
├── docker/                     # Docker configurations
├── docs/                       # Documentation and architecture diagrams
├── docker-compose.yml          # Full local development stack
├── requirements.txt            # Python dependencies
├── pyproject.toml              # Project metadata
└── Makefile                    # Common development commands
```

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Docker & Docker Compose
- Java 11+ (for Spark)

### 1. Clone and Setup
```bash
git clone https://github.com/yourusername/realtime-lakehouse-platform.git
cd realtime-lakehouse-platform
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 2. Start Infrastructure
```bash
docker-compose up -d  # Kafka, Schema Registry, Prometheus, Grafana
```

### 3. Initialize Delta Lake Tables
```bash
python -m scripts.init_tables
```

### 4. Start the Streaming Pipeline
```bash
python -m src.processing.stream_processor
```

### 5. Launch the Analytics API
```bash
uvicorn src.serving.api:app --reload --port 8000
```

### 6. Open the Dashboard
Navigate to `http://localhost:8000/docs` for the API docs, or `http://localhost:3000` for Grafana.

## 📊 Data Flow Example

```python
# Produce sample e-commerce events to Kafka
python -m scripts.produce_sample_data --events 10000 --topic ecommerce_events

# Watch the medallion pipeline process in real-time
# Bronze: Raw events land with ingestion metadata
# Silver: Deduplication, null handling, schema validation
# Gold: Revenue aggregations, user session analytics, product metrics
```

## 🔧 Configuration

All configuration is managed via environment variables with Pydantic validation:

```env
# .env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
DELTA_LAKE_PATH=/data/lakehouse
SPARK_MASTER=local[*]
API_HOST=0.0.0.0
API_PORT=8000
LOG_LEVEL=INFO
```

## 📈 Monitoring

- **Grafana Dashboard**: `http://localhost:3000` (admin/admin)
- **Prometheus Metrics**: `http://localhost:9090`
- **API Health Check**: `http://localhost:8000/health`

## 🧪 Testing

```bash
make test          # Run all tests
make test-unit     # Unit tests only
make test-int      # Integration tests
make lint          # Code quality checks
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file.
