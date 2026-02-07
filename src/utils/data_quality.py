"""
Data Quality Framework using Great Expectations patterns.
Validates data at each Medallion layer transition.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, lit, sum as spark_sum
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Callable
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class QualityLevel(str, Enum):
    CRITICAL = "critical"    # Fails the pipeline
    WARNING = "warning"      # Logs warning, continues
    INFO = "info"            # Informational only


@dataclass
class QualityCheck:
    """A single data quality validation rule."""
    name: str
    description: str
    level: QualityLevel
    check_fn: Callable[[DataFrame], bool]
    threshold: float = 1.0  # Required pass rate (0-1)


@dataclass
class QualityReport:
    """Results of a quality validation suite."""
    layer: str
    timestamp: str
    total_records: int
    checks_passed: int
    checks_failed: int
    checks_warned: int
    results: List[Dict] = field(default_factory=list)
    overall_pass: bool = True


class DataQualityValidator:
    """
    Validates DataFrames against configurable quality rules.
    
    Usage:
        validator = DataQualityValidator("bronze")
        validator.add_not_null_check("event_id", QualityLevel.CRITICAL)
        validator.add_uniqueness_check("event_id", QualityLevel.CRITICAL)
        report = validator.validate(df)
    """
    
    def __init__(self, layer: str):
        self.layer = layer
        self.checks: List[QualityCheck] = []
    
    def add_not_null_check(
        self, column: str, level: QualityLevel = QualityLevel.CRITICAL,
        threshold: float = 0.99
    ) -> None:
        """Ensure a column has no more than (1-threshold)% null values."""
        def check(df: DataFrame) -> float:
            total = df.count()
            if total == 0:
                return 1.0
            non_null = df.filter(col(column).isNotNull() & ~isnan(column)).count()
            return non_null / total
        
        self.checks.append(QualityCheck(
            name=f"not_null_{column}",
            description=f"Column '{column}' must be >= {threshold*100}% non-null",
            level=level,
            check_fn=check,
            threshold=threshold,
        ))
    
    def add_uniqueness_check(
        self, column: str, level: QualityLevel = QualityLevel.CRITICAL,
        threshold: float = 0.99
    ) -> None:
        """Ensure a column has unique values (deduplication check)."""
        def check(df: DataFrame) -> float:
            total = df.count()
            if total == 0:
                return 1.0
            distinct = df.select(column).distinct().count()
            return distinct / total
        
        self.checks.append(QualityCheck(
            name=f"unique_{column}",
            description=f"Column '{column}' must be >= {threshold*100}% unique",
            level=level,
            check_fn=check,
            threshold=threshold,
        ))
    
    def add_range_check(
        self, column: str, min_val: float, max_val: float,
        level: QualityLevel = QualityLevel.WARNING,
        threshold: float = 0.95
    ) -> None:
        """Ensure numeric values fall within an expected range."""
        def check(df: DataFrame) -> float:
            total = df.count()
            if total == 0:
                return 1.0
            in_range = df.filter(
                (col(column) >= min_val) & (col(column) <= max_val)
            ).count()
            return in_range / total
        
        self.checks.append(QualityCheck(
            name=f"range_{column}",
            description=f"Column '{column}' must be in [{min_val}, {max_val}]",
            level=level,
            check_fn=check,
            threshold=threshold,
        ))
    
    def add_allowed_values_check(
        self, column: str, allowed: List[str],
        level: QualityLevel = QualityLevel.WARNING,
        threshold: float = 0.95
    ) -> None:
        """Ensure column values are from an allowed set."""
        def check(df: DataFrame) -> float:
            total = df.count()
            if total == 0:
                return 1.0
            valid = df.filter(col(column).isin(allowed)).count()
            return valid / total
        
        self.checks.append(QualityCheck(
            name=f"allowed_values_{column}",
            description=f"Column '{column}' must be in {allowed}",
            level=level,
            check_fn=check,
            threshold=threshold,
        ))
    
    def validate(self, df: DataFrame) -> QualityReport:
        """Run all registered checks against the DataFrame."""
        from datetime import datetime
        
        total_records = df.count()
        report = QualityReport(
            layer=self.layer,
            timestamp=datetime.utcnow().isoformat(),
            total_records=total_records,
            checks_passed=0,
            checks_failed=0,
            checks_warned=0,
        )
        
        for check in self.checks:
            try:
                actual_rate = check.check_fn(df)
                passed = actual_rate >= check.threshold
                
                result = {
                    "check": check.name,
                    "description": check.description,
                    "level": check.level.value,
                    "threshold": check.threshold,
                    "actual": round(actual_rate, 4),
                    "passed": passed,
                }
                report.results.append(result)
                
                if passed:
                    report.checks_passed += 1
                elif check.level == QualityLevel.CRITICAL:
                    report.checks_failed += 1
                    report.overall_pass = False
                    logger.error(f"CRITICAL quality check FAILED: {check.name} "
                                 f"({actual_rate:.2%} < {check.threshold:.2%})")
                else:
                    report.checks_warned += 1
                    logger.warning(f"Quality check WARNING: {check.name} "
                                   f"({actual_rate:.2%} < {check.threshold:.2%})")
                    
            except Exception as e:
                logger.error(f"Quality check error '{check.name}': {e}")
                report.results.append({
                    "check": check.name,
                    "error": str(e),
                    "passed": False,
                })
                if check.level == QualityLevel.CRITICAL:
                    report.checks_failed += 1
                    report.overall_pass = False
        
        logger.info(
            f"Quality report [{self.layer}]: "
            f"{report.checks_passed} passed, "
            f"{report.checks_warned} warnings, "
            f"{report.checks_failed} failures | "
            f"Overall: {'PASS' if report.overall_pass else 'FAIL'}"
        )
        
        return report


# ─── Pre-configured Validators ──────────────────────────────────────────

def create_bronze_validator() -> DataQualityValidator:
    """Pre-configured quality checks for the Bronze layer."""
    v = DataQualityValidator("bronze")
    v.add_not_null_check("event_id", QualityLevel.CRITICAL, threshold=1.0)
    v.add_not_null_check("event_type", QualityLevel.CRITICAL, threshold=1.0)
    v.add_not_null_check("timestamp", QualityLevel.CRITICAL, threshold=0.99)
    v.add_uniqueness_check("event_id", QualityLevel.WARNING, threshold=0.95)
    return v


def create_silver_validator() -> DataQualityValidator:
    """Pre-configured quality checks for the Silver layer."""
    v = DataQualityValidator("silver")
    v.add_not_null_check("event_id", QualityLevel.CRITICAL, threshold=1.0)
    v.add_not_null_check("event_type", QualityLevel.CRITICAL, threshold=1.0)
    v.add_not_null_check("event_timestamp", QualityLevel.CRITICAL, threshold=1.0)
    v.add_uniqueness_check("event_id", QualityLevel.CRITICAL, threshold=0.999)
    v.add_range_check("price", 0, 100000, QualityLevel.WARNING, threshold=0.99)
    v.add_range_check("quantity", 1, 1000, QualityLevel.WARNING, threshold=0.99)
    v.add_allowed_values_check(
        "event_type",
        ["page_view", "add_to_cart", "remove_from_cart", "checkout", "purchase", "search"],
        QualityLevel.WARNING,
        threshold=0.95
    )
    return v
