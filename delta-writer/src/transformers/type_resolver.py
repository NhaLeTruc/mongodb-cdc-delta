"""Type resolution and conflict handling for schema evolution.

This module provides advanced type resolution strategies for handling
type conflicts during schema evolution in Delta Lake tables.
"""

from typing import Dict, Tuple, Optional
from enum import Enum
import pyarrow as pa
import structlog

logger = structlog.get_logger(__name__)


class TypeResolutionStrategy(Enum):
    """Strategy for resolving type conflicts."""

    WIDEN = "widen"  # Always widen to the broader type
    STRICT = "strict"  # Fail on type conflicts
    FALLBACK = "fallback"  # Fall back to string on conflicts


class TypeCompatibilityMatrix:
    """
    Matrix defining type compatibility and resolution rules.

    This class provides a structured way to determine if two types
    are compatible and what the resulting merged type should be.
    """

    def __init__(self):
        """Initialize the type compatibility matrix."""
        # Numeric type hierarchy (ordered from narrow to wide)
        self.numeric_hierarchy = [
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.float32(),
            pa.float64(),
        ]

        # Define explicit compatibility rules
        self.compatibility_rules: Dict[Tuple[str, str], pa.DataType] = {}
        self._build_compatibility_rules()

    def _build_compatibility_rules(self):
        """Build the compatibility rules matrix."""
        # Numeric type widening rules
        for i, type1 in enumerate(self.numeric_hierarchy):
            for j, type2 in enumerate(self.numeric_hierarchy):
                # The wider type wins
                wider_type = self.numeric_hierarchy[max(i, j)]
                key = (str(type1), str(type2))
                self.compatibility_rules[key] = wider_type

        # String type rules
        self.compatibility_rules[("string", "string")] = pa.string()
        self.compatibility_rules[("string", "large_string")] = pa.large_string()
        self.compatibility_rules[("large_string", "string")] = pa.large_string()
        self.compatibility_rules[("large_string", "large_string")] = pa.large_string()

        # Boolean rules
        self.compatibility_rules[("bool", "bool")] = pa.bool_()

        # Timestamp rules
        self.compatibility_rules[("timestamp[us]", "timestamp[us]")] = pa.timestamp('us')
        self.compatibility_rules[("timestamp[ms]", "timestamp[ms]")] = pa.timestamp('ms')
        self.compatibility_rules[("timestamp[us]", "timestamp[ms]")] = pa.timestamp('us')
        self.compatibility_rules[("timestamp[ms]", "timestamp[us]")] = pa.timestamp('us')

        logger.debug(
            "type_compatibility_matrix_initialized",
            rules_count=len(self.compatibility_rules)
        )

    def are_compatible(self, type1: pa.DataType, type2: pa.DataType) -> bool:
        """
        Check if two types are compatible.

        Args:
            type1: First type
            type2: Second type

        Returns:
            True if types are compatible
        """
        # Same type is always compatible
        if type1 == type2:
            return True

        # Null is compatible with anything
        if pa.types.is_null(type1) or pa.types.is_null(type2):
            return True

        # Check numeric compatibility
        if self._is_numeric_type(type1) and self._is_numeric_type(type2):
            return True

        # Check string compatibility
        if pa.types.is_string(type1) and pa.types.is_string(type2):
            return True

        # Check list compatibility (if element types are compatible)
        if pa.types.is_list(type1) and pa.types.is_list(type2):
            return self.are_compatible(type1.value_type, type2.value_type)

        # Check struct compatibility
        if pa.types.is_struct(type1) and pa.types.is_struct(type2):
            return True  # Structs can be merged

        # Check timestamp compatibility
        if pa.types.is_timestamp(type1) and pa.types.is_timestamp(type2):
            return True

        # Otherwise, not compatible
        return False

    def _is_numeric_type(self, dtype: pa.DataType) -> bool:
        """Check if a type is numeric."""
        return pa.types.is_integer(dtype) or pa.types.is_floating(dtype)

    def get_merged_type(self, type1: pa.DataType, type2: pa.DataType) -> Optional[pa.DataType]:
        """
        Get the merged type for two compatible types.

        Args:
            type1: First type
            type2: Second type

        Returns:
            Merged type, or None if types are not compatible
        """
        if type1 == type2:
            return type1

        # Check the compatibility matrix
        key = (str(type1), str(type2))
        if key in self.compatibility_rules:
            return self.compatibility_rules[key]

        # Try reverse key
        reverse_key = (str(type2), str(type1))
        if reverse_key in self.compatibility_rules:
            return self.compatibility_rules[reverse_key]

        return None


class TypeResolver:
    """
    Advanced type resolution for schema evolution.

    This class provides methods to resolve type conflicts using
    configurable strategies and maintains metrics about type resolutions.
    """

    def __init__(self, strategy: TypeResolutionStrategy = TypeResolutionStrategy.WIDEN):
        """
        Initialize the type resolver.

        Args:
            strategy: Default resolution strategy
        """
        self.strategy = strategy
        self.compatibility_matrix = TypeCompatibilityMatrix()
        self.resolution_count = 0
        self.widening_count = 0
        self.fallback_count = 0
        self.strict_failures = 0

        logger.info(
            "type_resolver_initialized",
            strategy=strategy.value
        )

    def merge_pyarrow_types(
        self,
        type1: pa.DataType,
        type2: pa.DataType,
        strategy: Optional[TypeResolutionStrategy] = None
    ) -> pa.DataType:
        """
        Merge two PyArrow types using the specified resolution strategy.

        This is an enhanced version of the original merge_pyarrow_types
        from BSONToDeltaConverter with configurable strategies.

        Args:
            type1: First PyArrow type
            type2: Second PyArrow type
            strategy: Resolution strategy (uses default if not specified)

        Returns:
            The merged PyArrow type

        Raises:
            ValueError: If types are incompatible in STRICT mode
        """
        self.resolution_count += 1
        strategy = strategy or self.strategy

        # Same type → return as-is
        if type1 == type2:
            logger.debug(
                "type_resolution_identical",
                type=str(type1)
            )
            return type1

        # Null types → use the non-null type
        if pa.types.is_null(type1):
            logger.debug(
                "type_resolution_null_to_type",
                result_type=str(type2)
            )
            return type2
        if pa.types.is_null(type2):
            logger.debug(
                "type_resolution_null_to_type",
                result_type=str(type1)
            )
            return type1

        if strategy == TypeResolutionStrategy.STRICT:
            # STRICT mode: fail on any type conflict
            self.strict_failures += 1
            logger.error(
                "type_resolution_strict_failure",
                type1=str(type1),
                type2=str(type2)
            )
            raise ValueError(
                f"STRICT mode: Incompatible types {type1} and {type2}"
            )

        elif strategy == TypeResolutionStrategy.WIDEN:
            # WIDEN mode: use compatibility matrix and type widening
            result = self._resolve_with_widening(type1, type2)
            if result is not None:
                self.widening_count += 1
                logger.info(
                    "type_resolution_widened",
                    type1=str(type1),
                    type2=str(type2),
                    result=str(result)
                )
                return result
            else:
                # Fall back to string if widening not possible
                self.fallback_count += 1
                logger.warning(
                    "type_resolution_fallback_to_string",
                    type1=str(type1),
                    type2=str(type2)
                )
                return pa.string()

        else:  # TypeResolutionStrategy.FALLBACK
            # FALLBACK mode: always fall back to string on conflicts
            self.fallback_count += 1
            logger.info(
                "type_resolution_fallback",
                type1=str(type1),
                type2=str(type2),
                result="string"
            )
            return pa.string()

    def _resolve_with_widening(
        self,
        type1: pa.DataType,
        type2: pa.DataType
    ) -> Optional[pa.DataType]:
        """
        Resolve type conflict using widening rules.

        Args:
            type1: First type
            type2: Second type

        Returns:
            Merged type, or None if widening not possible
        """
        # Try compatibility matrix first
        merged = self.compatibility_matrix.get_merged_type(type1, type2)
        if merged is not None:
            return merged

        # List types → merge element types
        if pa.types.is_list(type1) and pa.types.is_list(type2):
            merged_value_type = self.merge_pyarrow_types(
                type1.value_type,
                type2.value_type,
                TypeResolutionStrategy.WIDEN
            )
            return pa.list_(merged_value_type)

        # Struct types → merge fields
        if pa.types.is_struct(type1) and pa.types.is_struct(type2):
            return self._merge_struct_types(type1, type2)

        # String types
        if pa.types.is_string(type1) or pa.types.is_string(type2):
            if pa.types.is_large_string(type1) or pa.types.is_large_string(type2):
                return pa.large_string()
            return pa.string()

        # No widening rule found
        return None

    def _merge_struct_types(self, type1: pa.DataType, type2: pa.DataType) -> pa.DataType:
        """
        Merge two struct types.

        Args:
            type1: First struct type
            type2: Second struct type

        Returns:
            Merged struct type
        """
        # Collect all field names and types
        all_fields = {}

        for field in type1:
            all_fields[field.name] = field.type

        for field in type2:
            if field.name in all_fields:
                # Merge overlapping fields
                all_fields[field.name] = self.merge_pyarrow_types(
                    all_fields[field.name],
                    field.type,
                    TypeResolutionStrategy.WIDEN
                )
            else:
                # Add new field
                all_fields[field.name] = field.type

        return pa.struct([
            pa.field(name, dtype)
            for name, dtype in all_fields.items()
        ])

    def get_statistics(self) -> Dict[str, int]:
        """
        Get type resolution statistics.

        Returns:
            Dictionary with resolution metrics
        """
        return {
            "resolution_count": self.resolution_count,
            "widening_count": self.widening_count,
            "fallback_count": self.fallback_count,
            "strict_failures": self.strict_failures
        }

    def reset_statistics(self):
        """Reset all statistics counters to zero."""
        self.resolution_count = 0
        self.widening_count = 0
        self.fallback_count = 0
        self.strict_failures = 0
        logger.info("type_resolver_statistics_reset")

    def is_widening_safe(self, from_type: pa.DataType, to_type: pa.DataType) -> bool:
        """
        Check if widening from one type to another is safe (no data loss).

        Args:
            from_type: Original type
            to_type: Target type

        Returns:
            True if widening is safe
        """
        # Same type is always safe
        if from_type == to_type:
            return True

        # Null to anything is safe
        if pa.types.is_null(from_type):
            return True

        # Check numeric hierarchy
        if self.compatibility_matrix._is_numeric_type(from_type) and \
           self.compatibility_matrix._is_numeric_type(to_type):
            try:
                idx1 = self.compatibility_matrix.numeric_hierarchy.index(from_type)
                idx2 = self.compatibility_matrix.numeric_hierarchy.index(to_type)
                return idx2 >= idx1  # Safe if to_type is wider or same
            except ValueError:
                return False

        # String to large_string is safe
        if pa.types.is_string(from_type) and pa.types.is_large_string(to_type):
            return True

        # List element type widening
        if pa.types.is_list(from_type) and pa.types.is_list(to_type):
            return self.is_widening_safe(from_type.value_type, to_type.value_type)

        # Struct type additions are safe
        if pa.types.is_struct(from_type) and pa.types.is_struct(to_type):
            from_fields = {f.name: f.type for f in from_type}
            to_fields = {f.name: f.type for f in to_type}

            # All from_type fields must exist in to_type with safe widening
            for field_name, field_type in from_fields.items():
                if field_name not in to_fields:
                    return False  # Field removed, not safe
                if not self.is_widening_safe(field_type, to_fields[field_name]):
                    return False

            return True

        # Otherwise, not safe
        return False

    def get_type_category(self, dtype: pa.DataType) -> str:
        """
        Get the category of a data type.

        Args:
            dtype: PyArrow data type

        Returns:
            Category name (e.g., 'integer', 'float', 'string', 'struct', 'list')
        """
        if pa.types.is_integer(dtype):
            return "integer"
        elif pa.types.is_floating(dtype):
            return "float"
        elif pa.types.is_string(dtype):
            return "string"
        elif pa.types.is_boolean(dtype):
            return "boolean"
        elif pa.types.is_timestamp(dtype):
            return "timestamp"
        elif pa.types.is_date(dtype):
            return "date"
        elif pa.types.is_decimal(dtype):
            return "decimal"
        elif pa.types.is_list(dtype):
            return "list"
        elif pa.types.is_struct(dtype):
            return "struct"
        elif pa.types.is_null(dtype):
            return "null"
        else:
            return "other"


# Backwards compatibility: Import in bson_to_delta.py can still work
def merge_pyarrow_types(type1: pa.DataType, type2: pa.DataType) -> pa.DataType:
    """
    Backwards compatible wrapper for type merging.

    This function maintains compatibility with existing code that imports
    merge_pyarrow_types from bson_to_delta.

    Args:
        type1: First PyArrow type
        type2: Second PyArrow type

    Returns:
        The merged PyArrow type
    """
    resolver = TypeResolver(strategy=TypeResolutionStrategy.WIDEN)
    return resolver.merge_pyarrow_types(type1, type2)
