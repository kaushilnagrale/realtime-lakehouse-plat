"""
Schema Registry integration for managing event schemas with versioning.
Supports schema evolution with backward/forward compatibility checks.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, List
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import json
import hashlib
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class SchemaVersion:
    """Represents a versioned schema entry."""
    version: int
    schema: StructType
    fingerprint: str
    created_at: datetime
    compatibility: str = "BACKWARD"  # BACKWARD, FORWARD, FULL, NONE


@dataclass
class SchemaRegistryEntry:
    """A subject in the schema registry with version history."""
    subject: str
    versions: List[SchemaVersion] = field(default_factory=list)
    
    @property
    def latest(self) -> Optional[SchemaVersion]:
        return self.versions[-1] if self.versions else None
    
    @property
    def latest_version(self) -> int:
        return self.latest.version if self.latest else 0


class SchemaRegistry:
    """
    Local schema registry for managing Spark StructType schemas.
    
    Provides:
    - Schema versioning with fingerprint-based change detection
    - Backward/forward compatibility validation
    - Schema evolution tracking
    - Integration with Confluent Schema Registry (optional)
    """
    
    def __init__(self):
        self._registry: Dict[str, SchemaRegistryEntry] = {}
        logger.info("Schema registry initialized")
    
    def register_schema(
        self,
        subject: str,
        schema: StructType,
        compatibility: str = "BACKWARD",
    ) -> SchemaVersion:
        """
        Register a new schema version for a subject.
        
        Args:
            subject: Schema subject name (e.g., "ecommerce_events")
            schema: PySpark StructType schema
            compatibility: Compatibility mode (BACKWARD, FORWARD, FULL, NONE)
            
        Returns:
            The newly registered SchemaVersion
            
        Raises:
            SchemaCompatibilityError: If schema is not compatible with previous version
        """
        fingerprint = self._compute_fingerprint(schema)
        
        if subject not in self._registry:
            self._registry[subject] = SchemaRegistryEntry(subject=subject)
        
        entry = self._registry[subject]
        
        # Check if schema is unchanged
        if entry.latest and entry.latest.fingerprint == fingerprint:
            logger.info(f"Schema unchanged for '{subject}' (v{entry.latest_version})")
            return entry.latest
        
        # Validate compatibility with previous version
        if entry.latest and compatibility != "NONE":
            self._validate_compatibility(entry.latest.schema, schema, compatibility)
        
        # Register new version
        new_version = SchemaVersion(
            version=entry.latest_version + 1,
            schema=schema,
            fingerprint=fingerprint,
            created_at=datetime.utcnow(),
            compatibility=compatibility,
        )
        entry.versions.append(new_version)
        
        logger.info(f"Registered schema '{subject}' v{new_version.version} | "
                     f"Fingerprint: {fingerprint[:12]}...")
        
        return new_version
    
    def get_schema(self, subject: str, version: Optional[int] = None) -> StructType:
        """
        Retrieve a schema by subject and optional version.
        
        Args:
            subject: Schema subject name
            version: Specific version number (None for latest)
            
        Returns:
            The requested StructType schema
        """
        if subject not in self._registry:
            raise KeyError(f"Subject '{subject}' not found in registry")
        
        entry = self._registry[subject]
        
        if version is None:
            return entry.latest.schema
        
        for v in entry.versions:
            if v.version == version:
                return v.schema
        
        raise KeyError(f"Version {version} not found for subject '{subject}'")
    
    def get_all_versions(self, subject: str) -> List[SchemaVersion]:
        """Get all schema versions for a subject."""
        if subject not in self._registry:
            return []
        return self._registry[subject].versions
    
    def _validate_compatibility(
        self,
        old_schema: StructType,
        new_schema: StructType,
        mode: str,
    ) -> None:
        """
        Validate schema compatibility between versions.
        
        BACKWARD: New schema can read data written with old schema
        FORWARD: Old schema can read data written with new schema
        FULL: Both backward and forward compatible
        """
        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}
        
        if mode in ("BACKWARD", "FULL"):
            # All old fields must exist in new schema (or be nullable)
            for name, old_field in old_fields.items():
                if name not in new_fields:
                    if not old_field.nullable:
                        raise SchemaCompatibilityError(
                            f"BACKWARD incompatible: Required field '{name}' removed"
                        )
            
            # New required fields must have defaults
            for name, new_field in new_fields.items():
                if name not in old_fields and not new_field.nullable:
                    raise SchemaCompatibilityError(
                        f"BACKWARD incompatible: New required field '{name}' without default"
                    )
        
        if mode in ("FORWARD", "FULL"):
            # New schema shouldn't add required fields
            for name, new_field in new_fields.items():
                if name not in old_fields and not new_field.nullable:
                    raise SchemaCompatibilityError(
                        f"FORWARD incompatible: New required field '{name}' added"
                    )
        
        logger.info(f"Schema compatibility check passed ({mode})")
    
    @staticmethod
    def _compute_fingerprint(schema: StructType) -> str:
        """Compute a deterministic fingerprint for a schema."""
        schema_json = json.dumps(schema.jsonValue(), sort_keys=True)
        return hashlib.sha256(schema_json.encode()).hexdigest()
    
    def export_registry(self) -> Dict:
        """Export the entire registry as a serializable dict."""
        output = {}
        for subject, entry in self._registry.items():
            output[subject] = {
                "versions": [
                    {
                        "version": v.version,
                        "fingerprint": v.fingerprint,
                        "created_at": v.created_at.isoformat(),
                        "compatibility": v.compatibility,
                        "fields": [f.name for f in v.schema.fields],
                    }
                    for v in entry.versions
                ]
            }
        return output


class SchemaCompatibilityError(Exception):
    """Raised when a schema evolution violates compatibility rules."""
    pass


# Pre-registered schemas
registry = SchemaRegistry()
