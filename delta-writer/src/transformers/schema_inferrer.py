"""Schema inference for MongoDB documents to Delta Lake tables.

This module handles inferring Delta Lake schemas from MongoDB documents,
supporting schema evolution and merging.
"""

from typing import List, Dict, Any, Optional
from enum import Enum
import pyarrow as pa
import structlog

from .bson_to_delta import BSONToDeltaConverter

logger = structlog.get_logger(__name__)


class SchemaMergeMode(Enum):
    """Schema merge mode for controlling schema evolution behavior."""

    AUTO = "auto"  # Automatically merge schemas with type widening
    STRICT = "strict"  # Only allow adding new fields, no type changes
    PERMISSIVE = "permissive"  # Allow all schema changes including type conversions


class SchemaEvolutionMetrics:
    """Metrics for tracking schema evolution events."""

    def __init__(self):
        """Initialize metrics counters."""
        self.fields_added = 0
        self.types_widened = 0
        self.types_changed = 0
        self.schemas_merged = 0
        self.incompatible_types_encountered = 0

    def reset(self):
        """Reset all metrics to zero."""
        self.fields_added = 0
        self.types_widened = 0
        self.types_changed = 0
        self.schemas_merged = 0
        self.incompatible_types_encountered = 0

    def to_dict(self) -> Dict[str, int]:
        """Convert metrics to dictionary."""
        return {
            "fields_added": self.fields_added,
            "types_widened": self.types_widened,
            "types_changed": self.types_changed,
            "schemas_merged": self.schemas_merged,
            "incompatible_types_encountered": self.incompatible_types_encountered
        }


# Global metrics instance
_schema_metrics = SchemaEvolutionMetrics()


class SchemaInferrer:
    """Infers PyArrow schemas from MongoDB documents."""

    @staticmethod
    def infer_schema_from_document(doc: dict) -> pa.Schema:
        """
        Infer a PyArrow schema from a single MongoDB document.

        Args:
            doc: MongoDB document

        Returns:
            PyArrow Schema
        """
        fields = []

        for key, value in doc.items():
            try:
                field_type = BSONToDeltaConverter.infer_pyarrow_type(value)
                # Make all fields nullable (MongoDB is schemaless)
                fields.append(pa.field(key, field_type, nullable=True))
            except Exception as e:
                logger.error(
                    "schema_inference_failed",
                    field=key,
                    error=str(e),
                    value_type=type(value).__name__
                )
                # Fallback to string type
                fields.append(pa.field(key, pa.string(), nullable=True))

        return pa.schema(fields)

    @staticmethod
    def infer_schema_from_documents(docs: List[dict]) -> pa.Schema:
        """
        Infer a unified PyArrow schema from multiple MongoDB documents.

        This merges schemas from all documents to create a unified schema
        that can accommodate all variations.

        Args:
            docs: List of MongoDB documents

        Returns:
            Unified PyArrow Schema
        """
        if not docs:
            logger.warning("empty_document_list_for_schema_inference")
            return pa.schema([])

        # Start with schema from first document
        unified_schema = SchemaInferrer.infer_schema_from_document(docs[0])

        # Merge with schemas from remaining documents
        for doc in docs[1:]:
            doc_schema = SchemaInferrer.infer_schema_from_document(doc)
            unified_schema = SchemaInferrer.merge_schemas(unified_schema, doc_schema)

        logger.info(
            "schema_inferred_from_documents",
            num_documents=len(docs),
            num_fields=len(unified_schema),
            fields=[field.name for field in unified_schema]
        )

        return unified_schema

    @staticmethod
    def merge_schemas(schema1: pa.Schema, schema2: pa.Schema) -> pa.Schema:
        """
        Merge two PyArrow schemas into a unified schema.

        This handles schema evolution by:
        1. Keeping all fields from both schemas
        2. Merging overlapping fields using type widening
        3. Making all fields nullable

        Args:
            schema1: First schema
            schema2: Second schema

        Returns:
            Merged schema
        """
        # Build a map of field names to types
        field_map: Dict[str, pa.DataType] = {}

        # Add fields from schema1
        for field in schema1:
            field_map[field.name] = field.type

        # Merge fields from schema2
        for field in schema2:
            if field.name in field_map:
                # Field exists in both schemas - merge types
                existing_type = field_map[field.name]
                new_type = field.type

                if existing_type != new_type:
                    # Type conflict - use type widening
                    merged_type = BSONToDeltaConverter.merge_pyarrow_types(
                        existing_type,
                        new_type
                    )
                    field_map[field.name] = merged_type
                    logger.debug(
                        "schema_type_merged",
                        field=field.name,
                        type1=str(existing_type),
                        type2=str(new_type),
                        merged_type=str(merged_type)
                    )
            else:
                # New field - add it
                field_map[field.name] = field.type
                logger.debug(
                    "schema_field_added",
                    field=field.name,
                    type=str(field.type)
                )

        # Build the merged schema with all fields nullable
        merged_fields = [
            pa.field(name, dtype, nullable=True)
            for name, dtype in field_map.items()
        ]

        return pa.schema(merged_fields)

    @staticmethod
    def merge_schema_with_mode(
        schema1: pa.Schema,
        schema2: pa.Schema,
        mode: SchemaMergeMode = SchemaMergeMode.AUTO
    ) -> pa.Schema:
        """
        Merge two PyArrow schemas with specified merge mode.

        Args:
            schema1: First schema
            schema2: Second schema
            mode: Schema merge mode controlling evolution behavior

        Returns:
            Merged schema according to the specified mode

        Raises:
            ValueError: If schemas are incompatible in STRICT mode
        """
        global _schema_metrics

        logger.info(
            "merging_schemas_with_mode",
            mode=mode.value,
            schema1_fields=len(schema1),
            schema2_fields=len(schema2)
        )

        if mode == SchemaMergeMode.STRICT:
            # In STRICT mode, only allow adding new fields
            # Do not allow type changes
            field_map: Dict[str, pa.DataType] = {}

            # Add fields from schema1
            for field in schema1:
                field_map[field.name] = field.type

            # Validate schema2 fields
            for field in schema2:
                if field.name in field_map:
                    # Field exists - must have same type in strict mode
                    existing_type = field_map[field.name]
                    new_type = field.type

                    if existing_type != new_type:
                        _schema_metrics.incompatible_types_encountered += 1
                        logger.error(
                            "strict_mode_type_mismatch",
                            field=field.name,
                            existing_type=str(existing_type),
                            new_type=str(new_type)
                        )
                        raise ValueError(
                            f"STRICT mode: Type mismatch for field '{field.name}': "
                            f"{existing_type} vs {new_type}"
                        )
                else:
                    # New field - allowed in strict mode
                    field_map[field.name] = field.type
                    _schema_metrics.fields_added += 1
                    logger.info(
                        "strict_mode_field_added",
                        field=field.name,
                        type=str(field.type)
                    )

            merged_fields = [
                pa.field(name, dtype, nullable=True)
                for name, dtype in field_map.items()
            ]

            _schema_metrics.schemas_merged += 1
            return pa.schema(merged_fields)

        elif mode == SchemaMergeMode.PERMISSIVE:
            # In PERMISSIVE mode, allow all changes
            # Use merge_schemas which does type widening and falls back to string
            result = SchemaInferrer.merge_schemas(schema1, schema2)
            _schema_metrics.schemas_merged += 1
            logger.info(
                "permissive_mode_merge_completed",
                result_fields=len(result)
            )
            return result

        else:  # SchemaMergeMode.AUTO (default)
            # AUTO mode: merge with type widening but track metrics
            field_map: Dict[str, pa.DataType] = {}

            # Add fields from schema1
            for field in schema1:
                field_map[field.name] = field.type

            # Merge fields from schema2
            for field in schema2:
                if field.name in field_map:
                    # Field exists in both schemas
                    existing_type = field_map[field.name]
                    new_type = field.type

                    if existing_type != new_type:
                        # Type conflict - use type widening
                        merged_type = BSONToDeltaConverter.merge_pyarrow_types(
                            existing_type,
                            new_type
                        )

                        # Track metrics
                        if merged_type == pa.string() and \
                           not (pa.types.is_string(existing_type) or pa.types.is_string(new_type)):
                            _schema_metrics.incompatible_types_encountered += 1
                        elif SchemaInferrer._is_type_widening(existing_type, merged_type):
                            _schema_metrics.types_widened += 1
                        else:
                            _schema_metrics.types_changed += 1

                        field_map[field.name] = merged_type
                        logger.info(
                            "auto_mode_type_merged",
                            field=field.name,
                            type1=str(existing_type),
                            type2=str(new_type),
                            merged_type=str(merged_type)
                        )
                else:
                    # New field - add it
                    field_map[field.name] = field.type
                    _schema_metrics.fields_added += 1
                    logger.info(
                        "auto_mode_field_added",
                        field=field.name,
                        type=str(field.type)
                    )

            merged_fields = [
                pa.field(name, dtype, nullable=True)
                for name, dtype in field_map.items()
            ]

            _schema_metrics.schemas_merged += 1
            return pa.schema(merged_fields)

    @staticmethod
    def _is_type_widening(from_type: pa.DataType, to_type: pa.DataType) -> bool:
        """
        Check if a type change is a widening operation.

        Args:
            from_type: Original type
            to_type: New type

        Returns:
            True if this is a widening operation
        """
        # Numeric widening hierarchy
        numeric_hierarchy = [
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.float32(),
            pa.float64(),
        ]

        try:
            idx1 = numeric_hierarchy.index(from_type)
            idx2 = numeric_hierarchy.index(to_type)
            return idx2 > idx1
        except ValueError:
            return False

    @staticmethod
    def validate_schema_compatibility(
        existing_schema: pa.Schema,
        new_schema: pa.Schema,
        allow_field_removal: bool = False
    ) -> Dict[str, Any]:
        """
        Validate if a new schema is compatible with an existing schema.

        Args:
            existing_schema: Existing table schema
            new_schema: New schema to validate
            allow_field_removal: Whether to allow field removal (default: False)

        Returns:
            Dictionary with validation results:
            - compatible: bool
            - issues: List of compatibility issues
            - warnings: List of warnings
        """
        issues = []
        warnings = []

        existing_fields = {field.name: field.type for field in existing_schema}
        new_fields = {field.name: field.type for field in new_schema}

        # Check for removed fields
        removed_fields = set(existing_fields.keys()) - set(new_fields.keys())
        if removed_fields:
            if allow_field_removal:
                warnings.append(f"Fields removed: {removed_fields}")
                logger.warning(
                    "schema_compatibility_fields_removed",
                    fields=list(removed_fields)
                )
            else:
                issues.append(f"Fields removed (not allowed): {removed_fields}")
                logger.error(
                    "schema_compatibility_field_removal_not_allowed",
                    fields=list(removed_fields)
                )

        # Check for type compatibility on overlapping fields
        for field_name in set(existing_fields.keys()) & set(new_fields.keys()):
            existing_type = existing_fields[field_name]
            new_type = new_fields[field_name]

            if not SchemaInferrer._types_compatible(existing_type, new_type):
                issues.append(
                    f"Incompatible type change for '{field_name}': "
                    f"{existing_type} -> {new_type}"
                )
                logger.error(
                    "schema_compatibility_incompatible_type",
                    field=field_name,
                    existing_type=str(existing_type),
                    new_type=str(new_type)
                )

        # Check for added fields (just informational)
        added_fields = set(new_fields.keys()) - set(existing_fields.keys())
        if added_fields:
            warnings.append(f"New fields added: {added_fields}")
            logger.info(
                "schema_compatibility_fields_added",
                fields=list(added_fields)
            )

        compatible = len(issues) == 0

        result = {
            "compatible": compatible,
            "issues": issues,
            "warnings": warnings
        }

        logger.info(
            "schema_compatibility_validation_complete",
            compatible=compatible,
            issues_count=len(issues),
            warnings_count=len(warnings)
        )

        return result

    @staticmethod
    def get_metrics() -> Dict[str, int]:
        """
        Get current schema evolution metrics.

        Returns:
            Dictionary with metric counters
        """
        return _schema_metrics.to_dict()

    @staticmethod
    def reset_metrics():
        """Reset schema evolution metrics to zero."""
        global _schema_metrics
        _schema_metrics.reset()
        logger.info("schema_evolution_metrics_reset")

    @staticmethod
    def add_metadata_fields(schema: pa.Schema, metadata_fields: Optional[Dict[str, pa.DataType]] = None) -> pa.Schema:
        """
        Add metadata fields to a schema.

        Default metadata fields:
        - _cdc_timestamp: When the change was captured
        - _cdc_operation: insert/update/delete
        - _ingestion_timestamp: When the record was written to Delta Lake
        - _kafka_offset: Kafka offset for idempotency
        - _kafka_partition: Kafka partition
        - _kafka_topic: Kafka topic

        Args:
            schema: Original schema
            metadata_fields: Optional custom metadata fields (name -> type)

        Returns:
            Schema with metadata fields added
        """
        if metadata_fields is None:
            metadata_fields = {
                "_cdc_timestamp": pa.timestamp('us'),
                "_cdc_operation": pa.string(),
                "_ingestion_timestamp": pa.timestamp('us'),
                "_kafka_offset": pa.int64(),
                "_kafka_partition": pa.int32(),
                "_kafka_topic": pa.string(),
            }

        # Convert existing schema fields to list
        fields = list(schema)

        # Add metadata fields (if not already present)
        for name, dtype in metadata_fields.items():
            if name not in [f.name for f in fields]:
                fields.append(pa.field(name, dtype, nullable=True))

        return pa.schema(fields)

    @staticmethod
    def is_compatible(existing_schema: pa.Schema, new_schema: pa.Schema) -> bool:
        """
        Check if a new schema is compatible with an existing schema.

        Compatible means:
        1. New schema doesn't remove existing fields
        2. New schema doesn't change existing field types to incompatible types

        Args:
            existing_schema: Existing table schema
            new_schema: New schema to validate

        Returns:
            True if compatible, False otherwise
        """
        existing_fields = {field.name: field.type for field in existing_schema}

        for field in new_schema:
            if field.name in existing_fields:
                existing_type = existing_fields[field.name]
                new_type = field.type

                # Check if types are compatible
                if not SchemaInferrer._types_compatible(existing_type, new_type):
                    logger.warning(
                        "schema_incompatible_type_change",
                        field=field.name,
                        existing_type=str(existing_type),
                        new_type=str(new_type)
                    )
                    return False

        return True

    @staticmethod
    def _types_compatible(existing_type: pa.DataType, new_type: pa.DataType) -> bool:
        """
        Check if two types are compatible for schema evolution.

        Args:
            existing_type: Existing field type
            new_type: New field type

        Returns:
            True if compatible
        """
        # Same type is always compatible
        if existing_type == new_type:
            return True

        # Numeric widening is compatible (int32 -> int64, int -> float)
        if pa.types.is_integer(existing_type) and pa.types.is_integer(new_type):
            return True  # Integer widening allowed
        if pa.types.is_integer(existing_type) and pa.types.is_floating(new_type):
            return True  # Int to float allowed
        if pa.types.is_floating(existing_type) and pa.types.is_floating(new_type):
            return True  # Float precision change allowed

        # String types are always compatible with themselves
        if pa.types.is_string(existing_type) and pa.types.is_string(new_type):
            return True

        # List types - check element compatibility
        if pa.types.is_list(existing_type) and pa.types.is_list(new_type):
            return SchemaInferrer._types_compatible(
                existing_type.value_type,
                new_type.value_type
            )

        # Struct types - new struct can add fields but shouldn't remove or change existing
        if pa.types.is_struct(existing_type) and pa.types.is_struct(new_type):
            existing_field_names = {f.name for f in existing_type}
            new_field_names = {f.name for f in new_type}

            # Check if existing fields are preserved
            if not existing_field_names.issubset(new_field_names):
                return False  # Fields were removed

            # Check type compatibility for overlapping fields
            for existing_field in existing_type:
                new_field = new_type.field(existing_field.name)
                if new_field is not None:
                    if not SchemaInferrer._types_compatible(existing_field.type, new_field.type):
                        return False

            return True

        # Null type is compatible with anything
        if pa.types.is_null(existing_type) or pa.types.is_null(new_type):
            return True

        # Otherwise, incompatible
        return False

    @staticmethod
    def get_schema_diff(schema1: pa.Schema, schema2: pa.Schema) -> Dict[str, Any]:
        """
        Get the difference between two schemas.

        Args:
            schema1: First schema
            schema2: Second schema

        Returns:
            Dictionary with:
            - added_fields: Fields in schema2 but not in schema1
            - removed_fields: Fields in schema1 but not in schema2
            - changed_fields: Fields with different types
        """
        fields1 = {field.name: field.type for field in schema1}
        fields2 = {field.name: field.type for field in schema2}

        added_fields = {
            name: str(dtype)
            for name, dtype in fields2.items()
            if name not in fields1
        }

        removed_fields = {
            name: str(dtype)
            for name, dtype in fields1.items()
            if name not in fields2
        }

        changed_fields = {}
        for name in fields1.keys() & fields2.keys():
            if fields1[name] != fields2[name]:
                changed_fields[name] = {
                    "from": str(fields1[name]),
                    "to": str(fields2[name])
                }

        return {
            "added_fields": added_fields,
            "removed_fields": removed_fields,
            "changed_fields": changed_fields,
        }
