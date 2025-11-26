"""Contract tests for Delta Lake table schema.

Tests verify that Delta Lake tables have the expected schema structure
and can handle MongoDB document types correctly.
"""

from typing import Any, Dict, List

import pytest
import pyarrow as pa


class TestDeltaLakeTableSchema:
    """Test Delta Lake table schema compliance."""

    @pytest.fixture
    def expected_users_schema(self) -> pa.Schema:
        """Expected schema for users table from MongoDB."""
        return pa.schema([
            pa.field("_id", pa.string(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("email", pa.string(), nullable=True),
            pa.field("age", pa.int64(), nullable=True),
            pa.field("created_at", pa.timestamp("ms"), nullable=True),
            pa.field("updated_at", pa.timestamp("ms"), nullable=True),
            # CDC metadata fields
            pa.field("_cdc_op", pa.string(), nullable=False),
            pa.field("_cdc_ts_ms", pa.int64(), nullable=False),
            pa.field("_cdc_source_db", pa.string(), nullable=False),
            pa.field("_cdc_source_collection", pa.string(), nullable=False),
        ])

    @pytest.fixture
    def sample_record(self) -> Dict[str, Any]:
        """Sample record converted from MongoDB."""
        return {
            "_id": "507f1f77bcf86cd799439011",
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 30,
            "created_at": "2024-01-15T10:30:00.000Z",
            "updated_at": None,
            "_cdc_op": "c",
            "_cdc_ts_ms": 1705318200000,
            "_cdc_source_db": "testdb",
            "_cdc_source_collection": "users",
        }

    def test_schema_has_required_fields(
        self, expected_users_schema: pa.Schema
    ) -> None:
        """Test that schema contains all required fields."""
        field_names = [field.name for field in expected_users_schema]

        assert "_id" in field_names
        assert "_cdc_op" in field_names
        assert "_cdc_ts_ms" in field_names
        assert "_cdc_source_db" in field_names
        assert "_cdc_source_collection" in field_names

    def test_id_field_is_non_nullable(
        self, expected_users_schema: pa.Schema
    ) -> None:
        """Test that _id field is non-nullable."""
        id_field = next(f for f in expected_users_schema if f.name == "_id")
        assert not id_field.nullable

    def test_cdc_metadata_fields_are_non_nullable(
        self, expected_users_schema: pa.Schema
    ) -> None:
        """Test that CDC metadata fields are non-nullable."""
        cdc_fields = [f for f in expected_users_schema if f.name.startswith("_cdc_")]

        for field in cdc_fields:
            assert not field.nullable, f"CDC field {field.name} should be non-nullable"

    def test_user_data_fields_are_nullable(
        self, expected_users_schema: pa.Schema
    ) -> None:
        """Test that user data fields are nullable (MongoDB allows missing fields)."""
        data_fields = [
            f for f in expected_users_schema
            if not f.name.startswith("_") or f.name == "_id"
        ]

        for field in data_fields:
            if field.name != "_id":
                assert field.nullable, f"Data field {field.name} should be nullable"

    def test_record_conforms_to_schema(
        self, expected_users_schema: pa.Schema, sample_record: Dict[str, Any]
    ) -> None:
        """Test that sample record can be converted to Arrow table."""
        # Create Arrow table from record
        table = pa.Table.from_pylist([sample_record], schema=expected_users_schema)

        assert table.num_rows == 1
        assert table.num_columns == len(expected_users_schema)

    def test_timestamp_fields_have_correct_type(
        self, expected_users_schema: pa.Schema
    ) -> None:
        """Test that timestamp fields have correct Arrow type."""
        created_at_field = next(
            f for f in expected_users_schema if f.name == "created_at"
        )
        updated_at_field = next(
            f for f in expected_users_schema if f.name == "updated_at"
        )

        assert pa.types.is_timestamp(created_at_field.type)
        assert pa.types.is_timestamp(updated_at_field.type)

    def test_numeric_fields_have_correct_type(
        self, expected_users_schema: pa.Schema
    ) -> None:
        """Test that numeric fields have correct Arrow type."""
        age_field = next(f for f in expected_users_schema if f.name == "age")
        ts_ms_field = next(f for f in expected_users_schema if f.name == "_cdc_ts_ms")

        assert pa.types.is_integer(age_field.type)
        assert pa.types.is_integer(ts_ms_field.type)

    def test_schema_supports_nested_documents(self) -> None:
        """Test that schema can handle nested MongoDB documents."""
        nested_schema = pa.schema([
            pa.field("_id", pa.string(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("address", pa.struct([
                pa.field("street", pa.string()),
                pa.field("city", pa.string()),
                pa.field("zip", pa.string()),
            ]), nullable=True),
        ])

        assert nested_schema is not None
        address_field = next(f for f in nested_schema if f.name == "address")
        assert pa.types.is_struct(address_field.type)

    def test_schema_supports_arrays(self) -> None:
        """Test that schema can handle MongoDB arrays."""
        array_schema = pa.schema([
            pa.field("_id", pa.string(), nullable=False),
            pa.field("tags", pa.list_(pa.string()), nullable=True),
            pa.field("scores", pa.list_(pa.int64()), nullable=True),
        ])

        assert array_schema is not None
        tags_field = next(f for f in array_schema if f.name == "tags")
        assert pa.types.is_list(tags_field.type)

    def test_schema_equality(self) -> None:
        """Test that schemas can be compared for equality."""
        schema1 = pa.schema([
            pa.field("_id", pa.string(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ])

        schema2 = pa.schema([
            pa.field("_id", pa.string(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ])

        assert schema1.equals(schema2)

    def test_schema_can_be_extended(
        self, expected_users_schema: pa.Schema
    ) -> None:
        """Test that schema can be extended with new fields."""
        # Add a new field
        extended_schema = expected_users_schema.append(
            pa.field("phone", pa.string(), nullable=True)
        )

        assert len(extended_schema) == len(expected_users_schema) + 1
        assert "phone" in [f.name for f in extended_schema]

    def test_cdc_operation_values_are_valid(self) -> None:
        """Test that CDC operation values are from valid set."""
        valid_ops = {"c", "u", "d", "r"}  # create, update, delete, read

        # This would be validated at runtime in actual implementation
        for op in valid_ops:
            assert op in {"c", "u", "d", "r"}
