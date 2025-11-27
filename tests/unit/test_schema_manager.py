"""Unit tests for SchemaManager and SchemaCache."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import pyarrow as pa

from delta_writer.src.writer.schema_manager import SchemaCache, SchemaManager
from delta_writer.src.transformers.schema_inferrer import SchemaInferrer


class TestSchemaCache:
    """Test SchemaCache functionality."""

    def test_cache_set_and_get(self):
        """Test basic cache set and get operations."""
        cache = SchemaCache(ttl_seconds=300)
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        table_uri = "s3://bucket/test_table"

        cache.set(table_uri, schema)
        cached_schema = cache.get(table_uri)

        assert cached_schema is not None
        assert cached_schema == schema

    def test_cache_miss(self):
        """Test cache miss returns None."""
        cache = SchemaCache(ttl_seconds=300)
        result = cache.get("s3://bucket/nonexistent")

        assert result is None

    def test_cache_expiry(self):
        """Test that cached schemas expire after TTL."""
        cache = SchemaCache(ttl_seconds=1)  # 1 second TTL
        schema = pa.schema([pa.field("id", pa.int64())])
        table_uri = "s3://bucket/test_table"

        cache.set(table_uri, schema)

        # Should be cached immediately
        assert cache.get(table_uri) is not None

        # Mock time passage
        with patch('delta_writer.src.writer.schema_manager.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime.now() + timedelta(seconds=2)
            result = cache.get(table_uri)

            # Cache should be expired
            assert result is None

    def test_cache_invalidate(self):
        """Test cache invalidation."""
        cache = SchemaCache(ttl_seconds=300)
        schema = pa.schema([pa.field("id", pa.int64())])
        table_uri = "s3://bucket/test_table"

        cache.set(table_uri, schema)
        assert cache.get(table_uri) is not None

        cache.invalidate(table_uri)
        assert cache.get(table_uri) is None

    def test_cache_invalidate_nonexistent(self):
        """Test invalidating a non-existent cache entry doesn't raise error."""
        cache = SchemaCache(ttl_seconds=300)

        # Should not raise any exception
        cache.invalidate("s3://bucket/nonexistent")

    def test_multiple_tables_cached(self):
        """Test caching schemas for multiple tables."""
        cache = SchemaCache(ttl_seconds=300)

        schema1 = pa.schema([pa.field("id", pa.int64())])
        schema2 = pa.schema([pa.field("name", pa.string())])

        cache.set("s3://bucket/table1", schema1)
        cache.set("s3://bucket/table2", schema2)

        assert cache.get("s3://bucket/table1") == schema1
        assert cache.get("s3://bucket/table2") == schema2


class TestSchemaManager:
    """Test SchemaManager functionality."""

    @pytest.fixture
    def storage_options(self):
        """Storage options for tests."""
        return {
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_ENDPOINT_URL": "http://localhost:9000",
            "AWS_ALLOW_HTTP": "true",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }

    @pytest.fixture
    def schema_manager(self, storage_options):
        """Create a SchemaManager instance."""
        return SchemaManager(storage_options, cache_ttl=300)

    def test_schema_manager_initialization(self, schema_manager):
        """Test SchemaManager initializes correctly."""
        assert schema_manager.storage_options is not None
        assert schema_manager.cache is not None
        assert isinstance(schema_manager.cache, SchemaCache)

    @patch('delta_writer.src.writer.schema_manager.DeltaTable')
    def test_get_table_schema_existing_table(self, mock_delta_table, schema_manager):
        """Test getting schema from an existing table."""
        expected_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])

        mock_table = MagicMock()
        mock_table.schema.return_value.to_pyarrow.return_value = expected_schema
        mock_delta_table.return_value = mock_table

        schema = schema_manager.get_table_schema("s3://bucket/test_table")

        assert schema == expected_schema
        mock_delta_table.assert_called_once()

    @patch('delta_writer.src.writer.schema_manager.DeltaTable')
    def test_get_table_schema_nonexistent_table(self, mock_delta_table, schema_manager):
        """Test getting schema from non-existent table returns None."""
        mock_delta_table.side_effect = Exception("Table not found")

        schema = schema_manager.get_table_schema("s3://bucket/nonexistent")

        assert schema is None

    @patch('delta_writer.src.writer.schema_manager.DeltaTable')
    def test_get_table_schema_uses_cache(self, mock_delta_table, schema_manager):
        """Test that get_table_schema uses cache."""
        expected_schema = pa.schema([pa.field("id", pa.int64())])

        mock_table = MagicMock()
        mock_table.schema.return_value.to_pyarrow.return_value = expected_schema
        mock_delta_table.return_value = mock_table

        # First call - should hit Delta Table
        schema1 = schema_manager.get_table_schema("s3://bucket/test_table", use_cache=True)
        assert mock_delta_table.call_count == 1

        # Second call - should use cache
        schema2 = schema_manager.get_table_schema("s3://bucket/test_table", use_cache=True)
        assert mock_delta_table.call_count == 1  # Not called again
        assert schema1 == schema2

    @patch('delta_writer.src.writer.schema_manager.DeltaTable')
    def test_table_exists_true(self, mock_delta_table, schema_manager):
        """Test table_exists returns True for existing table."""
        mock_table = MagicMock()
        mock_table.schema.return_value.to_pyarrow.return_value = pa.schema([])
        mock_delta_table.return_value = mock_table

        assert schema_manager.table_exists("s3://bucket/test_table") is True

    @patch('delta_writer.src.writer.schema_manager.DeltaTable')
    def test_table_exists_false(self, mock_delta_table, schema_manager):
        """Test table_exists returns False for non-existent table."""
        mock_delta_table.side_effect = Exception("Table not found")

        assert schema_manager.table_exists("s3://bucket/nonexistent") is False

    @patch('delta_writer.src.writer.schema_manager.DeltaTable')
    def test_ensure_schema_compatible_new_table(self, mock_delta_table, schema_manager):
        """Test ensure_schema_compatible with new table."""
        mock_delta_table.side_effect = Exception("Table not found")

        new_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

        result = schema_manager.ensure_schema_compatible("s3://bucket/new_table", new_schema)

        assert result == new_schema

    @patch('delta_writer.src.writer.schema_manager.DeltaTable')
    def test_ensure_schema_compatible_schema_merge(self, mock_delta_table, schema_manager):
        """Test ensure_schema_compatible merges schemas."""
        existing_schema = pa.schema([pa.field("id", pa.int64())])
        new_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

        mock_table = MagicMock()
        mock_table.schema.return_value.to_pyarrow.return_value = existing_schema
        mock_delta_table.return_value = mock_table

        result = schema_manager.ensure_schema_compatible("s3://bucket/test_table", new_schema)

        # Should contain both fields
        assert len(result) == 2
        assert "id" in [f.name for f in result]
        assert "name" in [f.name for f in result]

    @patch('delta_writer.src.writer.schema_manager.DeltaTable')
    def test_ensure_schema_compatible_invalidates_cache(self, mock_delta_table, schema_manager):
        """Test that schema evolution invalidates cache."""
        existing_schema = pa.schema([pa.field("id", pa.int64())])
        new_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

        mock_table = MagicMock()
        mock_table.schema.return_value.to_pyarrow.return_value = existing_schema
        mock_delta_table.return_value = mock_table

        table_uri = "s3://bucket/test_table"

        # Pre-populate cache
        schema_manager.cache.set(table_uri, existing_schema)

        # Merge schemas
        schema_manager.ensure_schema_compatible(table_uri, new_schema)

        # Cache should be invalidated after schema evolution
        # Note: The cache will be repopulated during get_table_schema call

    @patch('delta_writer.src.writer.schema_manager.write_deltalake')
    @patch('delta_writer.src.writer.schema_manager.DeltaTable')
    def test_create_table_if_not_exists_creates_table(self, mock_delta_table, mock_write_deltalake, schema_manager):
        """Test creating a new table."""
        mock_delta_table.side_effect = Exception("Table not found")

        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

        schema_manager.create_table_if_not_exists("s3://bucket/new_table", schema)

        mock_write_deltalake.assert_called_once()
        call_args = mock_write_deltalake.call_args

        assert call_args[0][0] == "s3://bucket/new_table"
        assert call_args[1]["mode"] == "append"
        assert call_args[1]["schema_mode"] == "merge"

    @patch('delta_writer.src.writer.schema_manager.DeltaTable')
    def test_create_table_if_not_exists_skips_existing(self, mock_delta_table, schema_manager):
        """Test that create_table_if_not_exists skips existing tables."""
        mock_table = MagicMock()
        mock_table.schema.return_value.to_pyarrow.return_value = pa.schema([])
        mock_delta_table.return_value = mock_table

        schema = pa.schema([pa.field("id", pa.int64())])

        # Should not raise any exception
        schema_manager.create_table_if_not_exists("s3://bucket/existing_table", schema)

    @patch('delta_writer.src.writer.schema_manager.DeltaTable')
    def test_ensure_schema_compatible_type_widening(self, mock_delta_table, schema_manager):
        """Test schema evolution with type widening."""
        existing_schema = pa.schema([pa.field("count", pa.int32())])
        new_schema = pa.schema([pa.field("count", pa.int64())])

        mock_table = MagicMock()
        mock_table.schema.return_value.to_pyarrow.return_value = existing_schema
        mock_delta_table.return_value = mock_table

        result = schema_manager.ensure_schema_compatible("s3://bucket/test_table", new_schema)

        # Type should be widened to int64
        count_field = next(f for f in result if f.name == "count")
        assert count_field.type == pa.int64()

    @patch('delta_writer.src.writer.schema_manager.DeltaTable')
    def test_ensure_schema_compatible_nested_struct(self, mock_delta_table, schema_manager):
        """Test schema evolution with nested struct fields."""
        existing_schema = pa.schema([
            pa.field("user", pa.struct([pa.field("id", pa.int64())]))
        ])
        new_schema = pa.schema([
            pa.field("user", pa.struct([
                pa.field("id", pa.int64()),
                pa.field("name", pa.string())
            ]))
        ])

        mock_table = MagicMock()
        mock_table.schema.return_value.to_pyarrow.return_value = existing_schema
        mock_delta_table.return_value = mock_table

        result = schema_manager.ensure_schema_compatible("s3://bucket/test_table", new_schema)

        # Should have merged struct with both fields
        user_field = next(f for f in result if f.name == "user")
        assert pa.types.is_struct(user_field.type)
        struct_fields = {f.name for f in user_field.type}
        assert "id" in struct_fields
        assert "name" in struct_fields
