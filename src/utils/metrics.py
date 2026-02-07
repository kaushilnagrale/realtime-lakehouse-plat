"""
Prometheus metrics collection for pipeline observability.
Tracks throughput, latency, errors, and data quality across all layers.
"""

from typing import Dict, Optional
from datetime import datetime
import threading
import logging

logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Thread-safe metrics collector with Prometheus-compatible export.
    
    Metrics tracked:
    - Counters: Records processed, errors, DLQ writes
    - Gauges: Active queries, cache size, connection count
    - Histograms: Processing latency, batch sizes
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._counters: Dict[str, int] = {}
                    cls._instance._gauges: Dict[str, float] = {}
                    cls._instance._histograms: Dict[str, list] = {}
                    cls._instance._last_reset = datetime.utcnow()
        return cls._instance
    
    def increment(self, name: str, value: int = 1) -> None:
        """Increment a counter metric."""
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + value
    
    def set_gauge(self, name: str, value: float) -> None:
        """Set a gauge metric to a specific value."""
        with self._lock:
            self._gauges[name] = value
    
    def observe(self, name: str, value: float) -> None:
        """Record an observation for a histogram metric."""
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = []
            self._histograms[name].append(value)
            # Keep last 1000 observations
            if len(self._histograms[name]) > 1000:
                self._histograms[name] = self._histograms[name][-1000:]
    
    def get_counter(self, name: str) -> int:
        return self._counters.get(name, 0)
    
    def get_gauge(self, name: str) -> float:
        return self._gauges.get(name, 0.0)
    
    def export_prometheus(self) -> str:
        """Export all metrics in Prometheus text format."""
        lines = []
        
        for name, value in self._counters.items():
            lines.append(f"lakehouse_{name}_total {value}")
        
        for name, value in self._gauges.items():
            lines.append(f"lakehouse_{name} {value}")
        
        for name, values in self._histograms.items():
            if values:
                lines.append(f"lakehouse_{name}_count {len(values)}")
                lines.append(f"lakehouse_{name}_sum {sum(values):.4f}")
                lines.append(f"lakehouse_{name}_avg {sum(values)/len(values):.4f}")
        
        return "\n".join(lines)
    
    def export_dict(self) -> Dict:
        """Export all metrics as a dictionary."""
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {
                k: {"count": len(v), "sum": sum(v), "avg": sum(v)/len(v) if v else 0}
                for k, v in self._histograms.items()
            },
            "last_reset": self._last_reset.isoformat(),
        }
    
    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()
            self._last_reset = datetime.utcnow()
