"""
Analytics Serving Layer: FastAPI REST API + WebSocket for real-time dashboards.

Provides low-latency access to Gold-layer aggregations and supports:
- REST endpoints for historical analytics queries
- WebSocket streaming for live dashboard updates
- Health checks and pipeline status monitoring
- Prometheus metrics endpoint
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from enum import Enum
import asyncio
import json
import logging

logger = logging.getLogger(__name__)

# ─── App Initialization ─────────────────────────────────────────────────

app = FastAPI(
    title="Lakehouse Analytics API",
    description="Real-time analytics serving layer for the Medallion Lakehouse Platform",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Models ──────────────────────────────────────────────────────────────

class TimeGranularity(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class RevenueMetricsResponse(BaseModel):
    event_date: str
    event_hour: Optional[int] = None
    category: Optional[str] = None
    channel_group: Optional[str] = None
    device_type: Optional[str] = None
    country: Optional[str] = None
    total_revenue: float
    order_count: int
    unique_buyers: int
    avg_order_value: float
    revenue_per_buyer: float


class SessionAnalyticsResponse(BaseModel):
    event_date: str
    total_sessions: int
    avg_session_duration: float
    avg_page_views: float
    bounce_rate: float
    conversion_rate: float
    total_revenue: float


class ProductPerformanceResponse(BaseModel):
    product_id: str
    product_name: Optional[str]
    category: str
    views: int
    purchases: int
    total_revenue: float
    view_to_purchase_rate: float


class FunnelResponse(BaseModel):
    event_date: str
    channel_group: str
    stage_1_viewers: int
    stage_2_cart_adders: int
    stage_3_checkout: int
    stage_4_purchasers: int
    overall_conversion_rate: float


class PipelineStatusResponse(BaseModel):
    status: str
    active_queries: int
    bronze_records: Optional[int] = None
    silver_records: Optional[int] = None
    gold_tables: Optional[int] = None
    last_updated: str


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    version: str
    uptime_seconds: float


# ─── Query Engine (Simulated for standalone serving) ─────────────────────

class QueryEngine:
    """
    Executes queries against Gold-layer Delta tables.
    In production, this connects to a SparkSession reading Delta tables.
    For API serving, can also use DuckDB or Trino for low-latency queries.
    """
    
    def __init__(self):
        self._cache: Dict[str, Any] = {}
        self._last_refresh: Dict[str, datetime] = {}
        self.cache_ttl_seconds = 30
    
    async def query_revenue_metrics(
        self,
        start_date: str,
        end_date: str,
        granularity: TimeGranularity = TimeGranularity.DAILY,
        category: Optional[str] = None,
        channel: Optional[str] = None,
        country: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict]:
        """Query Gold revenue metrics with filters."""
        # In production: self.spark.read.format("delta").load(gold_path)
        # For demo: return structured placeholder
        cache_key = f"revenue_{start_date}_{end_date}_{granularity}_{category}"
        
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]
        
        # Simulated query result structure
        results = [
            {
                "event_date": start_date,
                "total_revenue": 125430.50,
                "order_count": 1247,
                "unique_buyers": 892,
                "avg_order_value": 100.58,
                "revenue_per_buyer": 140.62,
                "category": category or "all",
                "channel_group": channel or "all",
                "country": country or "all",
            }
        ]
        
        self._update_cache(cache_key, results)
        return results
    
    async def query_session_analytics(
        self,
        start_date: str,
        end_date: str,
        device_type: Optional[str] = None,
    ) -> List[Dict]:
        """Query Gold session analytics."""
        return [
            {
                "event_date": start_date,
                "total_sessions": 15420,
                "avg_session_duration": 342.5,
                "avg_page_views": 4.7,
                "bounce_rate": 0.32,
                "conversion_rate": 0.058,
                "total_revenue": 125430.50,
            }
        ]
    
    async def query_product_performance(
        self,
        start_date: str,
        end_date: str,
        category: Optional[str] = None,
        top_n: int = 20,
    ) -> List[Dict]:
        """Query Gold product performance metrics."""
        return [
            {
                "product_id": "PROD-001",
                "product_name": "Premium Widget",
                "category": category or "electronics",
                "views": 5420,
                "purchases": 312,
                "total_revenue": 46800.00,
                "view_to_purchase_rate": 0.0576,
            }
        ]
    
    async def query_funnel(
        self,
        start_date: str,
        end_date: str,
        channel: Optional[str] = None,
    ) -> List[Dict]:
        """Query Gold funnel analysis."""
        return [
            {
                "event_date": start_date,
                "channel_group": channel or "all",
                "stage_1_viewers": 50000,
                "stage_2_cart_adders": 8500,
                "stage_3_checkout": 3200,
                "stage_4_purchasers": 2100,
                "overall_conversion_rate": 0.042,
            }
        ]
    
    async def get_realtime_kpis(self) -> Dict:
        """Get latest real-time KPI window."""
        return {
            "window_start": datetime.utcnow().isoformat(),
            "active_users": 342,
            "events_per_second": 127.5,
            "revenue_last_5min": 4523.80,
            "orders_last_5min": 23,
            "top_event_type": "page_view",
        }
    
    def _is_cache_valid(self, key: str) -> bool:
        if key not in self._cache or key not in self._last_refresh:
            return False
        elapsed = (datetime.utcnow() - self._last_refresh[key]).total_seconds()
        return elapsed < self.cache_ttl_seconds
    
    def _update_cache(self, key: str, data: Any) -> None:
        self._cache[key] = data
        self._last_refresh[key] = datetime.utcnow()


# Initialize query engine
query_engine = QueryEngine()
_start_time = datetime.utcnow()


# ─── REST Endpoints ──────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """Health check endpoint for load balancers and monitoring."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat(),
        version="1.0.0",
        uptime_seconds=(datetime.utcnow() - _start_time).total_seconds(),
    )


@app.get("/api/v1/revenue", tags=["Analytics"])
async def get_revenue_metrics(
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    granularity: TimeGranularity = Query(TimeGranularity.DAILY),
    category: Optional[str] = Query(None, description="Filter by product category"),
    channel: Optional[str] = Query(None, description="Filter by marketing channel"),
    country: Optional[str] = Query(None, description="Filter by country"),
    limit: int = Query(100, ge=1, le=1000),
):
    """
    Query revenue metrics from the Gold layer.
    
    Returns aggregated revenue data with support for:
    - Time granularity (hourly, daily, weekly, monthly)
    - Dimensional filtering (category, channel, country)
    - Pagination
    """
    results = await query_engine.query_revenue_metrics(
        start_date, end_date, granularity, category, channel, country, limit
    )
    return {"data": results, "count": len(results), "granularity": granularity}


@app.get("/api/v1/sessions", tags=["Analytics"])
async def get_session_analytics(
    start_date: str = Query(...),
    end_date: str = Query(...),
    device_type: Optional[str] = Query(None),
):
    """Query session-level analytics from the Gold layer."""
    results = await query_engine.query_session_analytics(start_date, end_date, device_type)
    return {"data": results, "count": len(results)}


@app.get("/api/v1/products", tags=["Analytics"])
async def get_product_performance(
    start_date: str = Query(...),
    end_date: str = Query(...),
    category: Optional[str] = Query(None),
    top_n: int = Query(20, ge=1, le=100),
):
    """Query product performance metrics from the Gold layer."""
    results = await query_engine.query_product_performance(start_date, end_date, category, top_n)
    return {"data": results, "count": len(results)}


@app.get("/api/v1/funnel", tags=["Analytics"])
async def get_funnel_analysis(
    start_date: str = Query(...),
    end_date: str = Query(...),
    channel: Optional[str] = Query(None),
):
    """Query conversion funnel analysis from the Gold layer."""
    results = await query_engine.query_funnel(start_date, end_date, channel)
    return {"data": results, "count": len(results)}


@app.get("/api/v1/kpis/realtime", tags=["Real-Time"])
async def get_realtime_kpis():
    """Get the latest real-time KPI window (5-minute rolling)."""
    return await query_engine.get_realtime_kpis()


# ─── WebSocket for Live Dashboard ────────────────────────────────────────

class ConnectionManager:
    """Manages WebSocket connections for real-time dashboard streaming."""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected | Total: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected | Total: {len(self.active_connections)}")
    
    async def broadcast(self, message: dict):
        """Broadcast a message to all connected clients."""
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)
        
        for conn in disconnected:
            self.active_connections.remove(conn)


ws_manager = ConnectionManager()


@app.websocket("/ws/kpis")
async def websocket_kpi_stream(websocket: WebSocket):
    """
    WebSocket endpoint for streaming real-time KPIs to dashboards.
    
    Pushes updated KPI data every 5 seconds.
    """
    await ws_manager.connect(websocket)
    
    try:
        while True:
            kpis = await query_engine.get_realtime_kpis()
            await websocket.send_json(kpis)
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


@app.websocket("/ws/events")
async def websocket_event_stream(websocket: WebSocket):
    """
    WebSocket endpoint for streaming raw event feed.
    Useful for live activity monitoring dashboards.
    """
    await ws_manager.connect(websocket)
    
    try:
        while True:
            # In production: read from a Redis pub/sub or Kafka consumer
            event = {
                "type": "event_stream",
                "timestamp": datetime.utcnow().isoformat(),
                "data": {
                    "event_type": "page_view",
                    "user_id": "user_12345",
                    "product": "Premium Widget",
                    "country": "US",
                }
            }
            await websocket.send_json(event)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


# ─── Prometheus Metrics ──────────────────────────────────────────────────

@app.get("/metrics", tags=["System"])
async def prometheus_metrics():
    """Prometheus-compatible metrics endpoint."""
    return JSONResponse(content={
        "lakehouse_api_requests_total": 0,
        "lakehouse_active_websockets": len(ws_manager.active_connections),
        "lakehouse_query_latency_seconds": 0.0,
        "lakehouse_cache_hit_rate": 0.0,
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
