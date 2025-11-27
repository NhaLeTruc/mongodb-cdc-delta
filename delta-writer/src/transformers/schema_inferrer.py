"""Schema inference for MongoDB documents to Delta Lake tables.

This module handles inferring Delta Lake schemas from MongoDB documents,
supporting schema evolution and merging.
"""

from typing import List, Dict, Any, Optional
import pyarrow as pa
import structlog

from .bson_to_delta import BSONToDeltaConverter

logger = structlog.get_logger(__name__)


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
