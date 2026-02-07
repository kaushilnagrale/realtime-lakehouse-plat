"""
Structured logging with JSON formatting for production observability.
Integrates with ELK Stack, Datadog, or any JSON-compatible log aggregator.
"""

import logging
import json
import sys
from datetime import datetime, timezone
from typing import Optional
from src.config.settings import settings


class JSONFormatter(logging.Formatter):
    """Formats log records as JSON for structured log aggregation."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "service": "lakehouse-platform",
            "environment": settings.environment.value,
        }
        
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info),
            }
        
        if hasattr(record, "extra_data"):
            log_entry["extra"] = record.extra_data
        
        return json.dumps(log_entry, default=str)


def get_logger(
    name: str,
    level: Optional[str] = None,
    json_format: bool = True,
) -> logging.Logger:
    """
    Create a configured logger instance.
    
    Args:
        name: Logger name (typically __name__)
        level: Override log level
        json_format: Use JSON formatting (True for production)
    """
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger
    
    log_level = getattr(logging, (level or settings.log_level).upper())
    logger.setLevel(log_level)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    
    if json_format and settings.environment.value != "local":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "[%(asctime)s] %(levelname)-8s %(name)s:%(funcName)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
    
    logger.addHandler(handler)
    logger.propagate = False
    
    return logger
