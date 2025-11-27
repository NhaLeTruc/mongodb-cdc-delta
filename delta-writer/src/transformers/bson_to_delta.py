"""MongoDB BSON to Delta Lake (PyArrow) type conversion.

This module handles the conversion of MongoDB BSON types to Delta Lake/PyArrow types,
following the type mapping strategy defined in research.md.
"""

import base64
from datetime import datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
from bson import ObjectId, Decimal128
from bson.binary import Binary

import structlog

logger = structlog.get_logger(__name__)


class BSONToDeltaConverter:
    """Converts MongoDB BSON types to Delta Lake (PyArrow) types."""

    @staticmethod
    def convert_value(value: Any, field_name: str = "unknown") -> Any:
        """
        Convert a MongoDB BSON value to a Delta Lake compatible value.

        Args:
            value: The BSON value to convert
            field_name: The field name (for logging purposes)

        Returns:
            The converted value suitable for PyArrow/Delta Lake

        Raises:
            ValueError: If the value type cannot be converted
        """
        if value is None:
            return None

        # ObjectId → String (hex representation)
        if isinstance(value, ObjectId):
            return str(value)

        # Decimal128 → Python Decimal (preserved precision)
        if isinstance(value, Decimal128):
            return value.to_decimal()

        # Binary → Base64 string
        if isinstance(value, Binary):
            return base64.b64encode(bytes(value)).decode('utf-8')

        # datetime → keep as-is (PyArrow handles datetime)
        if isinstance(value, datetime):
            return value

        # Array/List → recursively convert elements
        if isinstance(value, list):
            return [BSONToDeltaConverter.convert_value(item, f"{field_name}[]") for item in value]

        # Nested document → recursively convert fields
        if isinstance(value, dict):
            return {
                key: BSONToDeltaConverter.convert_value(val, f"{field_name}.{key}")
                for key, val in value.items()
            }

        # Primitive types (str, int, float, bool) → keep as-is
        if isinstance(value, (str, int, float, bool)):
            return value

        # Decimal → keep as-is
        if isinstance(value, Decimal):
            return value

        # Unsupported type
        logger.warning(
            "unsupported_bson_type",
            field=field_name,
            type=type(value).__name__,
            value_str=str(value)[:100]
        )
        return str(value)  # Fallback to string representation

    @staticmethod
    def infer_pyarrow_type(value: Any) -> pa.DataType:
        """
        Infer PyArrow type from a MongoDB BSON value.

        Args:
            value: The BSON value to infer type from

        Returns:
            PyArrow DataType
        """
        if value is None:
            return pa.null()

        if isinstance(value, ObjectId):
            return pa.string()

        if isinstance(value, Decimal128):
            return pa.decimal128(38, 10)  # Standard precision for Decimal128

        if isinstance(value, Decimal):
            return pa.decimal128(38, 10)

        if isinstance(value, Binary):
            return pa.string()  # Store as base64 string

        if isinstance(value, datetime):
            return pa.timestamp('us')  # Microsecond precision

        if isinstance(value, bool):
            return pa.bool_()

        if isinstance(value, int):
            # MongoDB uses Int32 and Int64
            if -2147483648 <= value <= 2147483647:
                return pa.int32()
            return pa.int64()

        if isinstance(value, float):
            return pa.float64()

        if isinstance(value, str):
            return pa.string()

        if isinstance(value, list):
            if not value:
                # Empty list - default to list of strings
                return pa.list_(pa.string())
            # Infer type from first element
            element_type = BSONToDeltaConverter.infer_pyarrow_type(value[0])
            return pa.list_(element_type)

        if isinstance(value, dict):
            if not value:
                # Empty dict - return empty struct
                return pa.struct([])
            # Infer struct type from dictionary fields
            fields = []
            for key, val in value.items():
                field_type = BSONToDeltaConverter.infer_pyarrow_type(val)
                fields.append(pa.field(key, field_type))
            return pa.struct(fields)

        # Default to string for unknown types
        logger.warning(
            "unknown_type_defaulting_to_string",
            type=type(value).__name__
        )
        return pa.string()

    @staticmethod
    def convert_document(doc: dict) -> dict:
        """
        Convert an entire MongoDB document to Delta Lake compatible format.

        Args:
            doc: MongoDB document

        Returns:
            Converted document with Delta Lake compatible values
        """
        return {
            key: BSONToDeltaConverter.convert_value(val, key)
            for key, val in doc.items()
        }

    @staticmethod
    def merge_pyarrow_types(type1: pa.DataType, type2: pa.DataType) -> pa.DataType:
        """
        Merge two PyArrow types to find the widest compatible type.

        This is used for schema evolution when the same field has different types
        across documents.

        Args:
            type1: First PyArrow type
            type2: Second PyArrow type

        Returns:
            The widest compatible PyArrow type

        Examples:
            int32 + int64 → int64
            int32 + float64 → float64
            string + int32 → string (fallback)
        """
        # Same type → return as-is
        if type1 == type2:
            return type1

        # Null types → use the non-null type
        if pa.types.is_null(type1):
            return type2
        if pa.types.is_null(type2):
            return type1

        # Numeric type widening
        numeric_hierarchy = [
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.float32(),
            pa.float64(),
        ]

        try:
            idx1 = numeric_hierarchy.index(type1)
            idx2 = numeric_hierarchy.index(type2)
            return numeric_hierarchy[max(idx1, idx2)]
        except ValueError:
            pass  # Not both numeric types

        # List types → merge element types
        if pa.types.is_list(type1) and pa.types.is_list(type2):
            merged_value_type = BSONToDeltaConverter.merge_pyarrow_types(
                type1.value_type,
                type2.value_type
            )
            return pa.list_(merged_value_type)

        # Struct types → merge fields
        if pa.types.is_struct(type1) and pa.types.is_struct(type2):
            # Collect all field names
            all_fields = {}

            for field in type1:
                all_fields[field.name] = field.type

            for field in type2:
                if field.name in all_fields:
                    # Merge overlapping fields
                    all_fields[field.name] = BSONToDeltaConverter.merge_pyarrow_types(
                        all_fields[field.name],
                        field.type
                    )
                else:
                    # Add new field
                    all_fields[field.name] = field.type

            return pa.struct([
                pa.field(name, dtype)
                for name, dtype in all_fields.items()
            ])

        # String types → prefer large_string over string
        if pa.types.is_string(type1) or pa.types.is_string(type2):
            if pa.types.is_large_string(type1) or pa.types.is_large_string(type2):
                return pa.large_string()
            return pa.string()

        # Incompatible types → fallback to string
        logger.warning(
            "incompatible_types_falling_back_to_string",
            type1=str(type1),
            type2=str(type2)
        )
        return pa.string()
