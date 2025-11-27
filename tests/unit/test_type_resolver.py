"""Unit tests for type conflict resolution in BSONToDeltaConverter."""

import pytest
import pyarrow as pa

from delta_writer.src.transformers.bson_to_delta import BSONToDeltaConverter


class TestTypeResolution:
    """Test type conflict resolution and merging logic."""

    def test_merge_identical_types(self):
        """Test merging identical types returns same type."""
        result = BSONToDeltaConverter.merge_pyarrow_types(pa.int64(), pa.int64())
        assert result == pa.int64()

        result = BSONToDeltaConverter.merge_pyarrow_types(pa.string(), pa.string())
        assert result == pa.string()

    def test_merge_null_types(self):
        """Test merging with null type returns non-null type."""
        result = BSONToDeltaConverter.merge_pyarrow_types(pa.null(), pa.int64())
        assert result == pa.int64()

        result = BSONToDeltaConverter.merge_pyarrow_types(pa.string(), pa.null())
        assert result == pa.string()

    def test_numeric_widening_int32_to_int64(self):
        """Test int32 widens to int64."""
        result = BSONToDeltaConverter.merge_pyarrow_types(pa.int32(), pa.int64())
        assert result == pa.int64()

        result = BSONToDeltaConverter.merge_pyarrow_types(pa.int64(), pa.int32())
        assert result == pa.int64()

    def test_numeric_widening_int_to_float(self):
        """Test int widens to float."""
        result = BSONToDeltaConverter.merge_pyarrow_types(pa.int32(), pa.float64())
        assert result == pa.float64()

        result = BSONToDeltaConverter.merge_pyarrow_types(pa.int64(), pa.float32())
        assert result == pa.float32()

    def test_numeric_widening_hierarchy(self):
        """Test numeric type hierarchy widening."""
        # int8 -> int16
        result = BSONToDeltaConverter.merge_pyarrow_types(pa.int8(), pa.int16())
        assert result == pa.int16()

        # int16 -> int32
        result = BSONToDeltaConverter.merge_pyarrow_types(pa.int16(), pa.int32())
        assert result == pa.int32()

        # int32 -> int64
        result = BSONToDeltaConverter.merge_pyarrow_types(pa.int32(), pa.int64())
        assert result == pa.int64()

        # int64 -> float32
        result = BSONToDeltaConverter.merge_pyarrow_types(pa.int64(), pa.float32())
        assert result == pa.float32()

        # float32 -> float64
        result = BSONToDeltaConverter.merge_pyarrow_types(pa.float32(), pa.float64())
        assert result == pa.float64()

    def test_list_type_merging(self):
        """Test merging list types with different element types."""
        list_int32 = pa.list_(pa.int32())
        list_int64 = pa.list_(pa.int64())

        result = BSONToDeltaConverter.merge_pyarrow_types(list_int32, list_int64)

        assert pa.types.is_list(result)
        assert result.value_type == pa.int64()  # Element type widened

    def test_list_type_element_type_widening(self):
        """Test list element types widen appropriately."""
        list_int = pa.list_(pa.int32())
        list_float = pa.list_(pa.float64())

        result = BSONToDeltaConverter.merge_pyarrow_types(list_int, list_float)

        assert pa.types.is_list(result)
        assert result.value_type == pa.float64()

    def test_struct_type_merging_new_fields(self):
        """Test merging struct types adds new fields."""
        struct1 = pa.struct([pa.field("id", pa.int64())])
        struct2 = pa.struct([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])

        result = BSONToDeltaConverter.merge_pyarrow_types(struct1, struct2)

        assert pa.types.is_struct(result)
        field_names = {f.name for f in result}
        assert "id" in field_names
        assert "name" in field_names

    def test_struct_type_merging_overlapping_fields(self):
        """Test merging struct types with overlapping fields."""
        struct1 = pa.struct([pa.field("count", pa.int32())])
        struct2 = pa.struct([pa.field("count", pa.int64())])

        result = BSONToDeltaConverter.merge_pyarrow_types(struct1, struct2)

        assert pa.types.is_struct(result)
        count_field = result.field("count")
        assert count_field.type == pa.int64()  # Type widened

    def test_struct_type_merging_complex(self):
        """Test merging complex struct types."""
        struct1 = pa.struct([
            pa.field("id", pa.int32()),
            pa.field("active", pa.bool_())
        ])
        struct2 = pa.struct([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("count", pa.int32())
        ])

        result = BSONToDeltaConverter.merge_pyarrow_types(struct1, struct2)

        assert pa.types.is_struct(result)
        field_names = {f.name for f in result}
        assert field_names == {"id", "active", "name", "count"}

        # Check id was widened
        id_field = result.field("id")
        assert id_field.type == pa.int64()

    def test_nested_struct_merging(self):
        """Test merging nested struct types."""
        struct1 = pa.struct([
            pa.field("user", pa.struct([
                pa.field("id", pa.int32())
            ]))
        ])
        struct2 = pa.struct([
            pa.field("user", pa.struct([
                pa.field("id", pa.int64()),
                pa.field("email", pa.string())
            ]))
        ])

        result = BSONToDeltaConverter.merge_pyarrow_types(struct1, struct2)

        assert pa.types.is_struct(result)
        user_field = result.field("user")
        assert pa.types.is_struct(user_field.type)

        user_struct_fields = {f.name for f in user_field.type}
        assert "id" in user_struct_fields
        assert "email" in user_struct_fields

        # Check nested id was widened
        id_field = user_field.type.field("id")
        assert id_field.type == pa.int64()

    def test_string_type_merging(self):
        """Test merging string types."""
        result = BSONToDeltaConverter.merge_pyarrow_types(pa.string(), pa.string())
        assert result == pa.string()

        result = BSONToDeltaConverter.merge_pyarrow_types(pa.string(), pa.large_string())
        assert result == pa.large_string()

        result = BSONToDeltaConverter.merge_pyarrow_types(pa.large_string(), pa.string())
        assert result == pa.large_string()

    def test_incompatible_types_fallback_to_string(self):
        """Test incompatible types fall back to string."""
        result = BSONToDeltaConverter.merge_pyarrow_types(pa.int64(), pa.string())
        assert result == pa.string()

        result = BSONToDeltaConverter.merge_pyarrow_types(pa.bool_(), pa.int32())
        assert result == pa.string()

        result = BSONToDeltaConverter.merge_pyarrow_types(pa.timestamp('us'), pa.int64())
        assert result == pa.string()

    def test_list_vs_non_list_fallback(self):
        """Test list type vs non-list type falls back to string."""
        result = BSONToDeltaConverter.merge_pyarrow_types(pa.list_(pa.int32()), pa.int32())
        assert result == pa.string()

    def test_struct_vs_non_struct_fallback(self):
        """Test struct type vs non-struct type falls back to string."""
        struct_type = pa.struct([pa.field("id", pa.int64())])
        result = BSONToDeltaConverter.merge_pyarrow_types(struct_type, pa.int64())
        assert result == pa.string()


class TestInferPyArrowType:
    """Test PyArrow type inference from values."""

    def test_infer_null_type(self):
        """Test inferring type from None value."""
        result = BSONToDeltaConverter.infer_pyarrow_type(None)
        assert result == pa.null()

    def test_infer_int32_type(self):
        """Test inferring int32 from small integers."""
        result = BSONToDeltaConverter.infer_pyarrow_type(100)
        assert result == pa.int32()

        result = BSONToDeltaConverter.infer_pyarrow_type(-100)
        assert result == pa.int32()

        result = BSONToDeltaConverter.infer_pyarrow_type(2147483647)  # Max int32
        assert result == pa.int32()

    def test_infer_int64_type(self):
        """Test inferring int64 from large integers."""
        result = BSONToDeltaConverter.infer_pyarrow_type(2147483648)  # Max int32 + 1
        assert result == pa.int64()

        result = BSONToDeltaConverter.infer_pyarrow_type(-2147483649)  # Min int32 - 1
        assert result == pa.int64()

    def test_infer_float_type(self):
        """Test inferring float64 from float values."""
        result = BSONToDeltaConverter.infer_pyarrow_type(3.14)
        assert result == pa.float64()

    def test_infer_bool_type(self):
        """Test inferring bool type."""
        result = BSONToDeltaConverter.infer_pyarrow_type(True)
        assert result == pa.bool_()

        result = BSONToDeltaConverter.infer_pyarrow_type(False)
        assert result == pa.bool_()

    def test_infer_string_type(self):
        """Test inferring string type."""
        result = BSONToDeltaConverter.infer_pyarrow_type("hello")
        assert result == pa.string()

    def test_infer_list_type_empty(self):
        """Test inferring list type from empty list."""
        result = BSONToDeltaConverter.infer_pyarrow_type([])
        assert pa.types.is_list(result)
        assert result.value_type == pa.string()  # Default element type

    def test_infer_list_type_integers(self):
        """Test inferring list type from integer list."""
        result = BSONToDeltaConverter.infer_pyarrow_type([1, 2, 3])
        assert pa.types.is_list(result)
        assert result.value_type == pa.int32()

    def test_infer_list_type_strings(self):
        """Test inferring list type from string list."""
        result = BSONToDeltaConverter.infer_pyarrow_type(["a", "b", "c"])
        assert pa.types.is_list(result)
        assert result.value_type == pa.string()

    def test_infer_struct_type_empty(self):
        """Test inferring struct type from empty dict."""
        result = BSONToDeltaConverter.infer_pyarrow_type({})
        assert pa.types.is_struct(result)
        assert len(result) == 0

    def test_infer_struct_type_simple(self):
        """Test inferring struct type from simple dict."""
        result = BSONToDeltaConverter.infer_pyarrow_type({"id": 1, "name": "test"})
        assert pa.types.is_struct(result)

        field_names = {f.name for f in result}
        assert "id" in field_names
        assert "name" in field_names

        id_field = result.field("id")
        assert id_field.type == pa.int32()

        name_field = result.field("name")
        assert name_field.type == pa.string()

    def test_infer_nested_struct_type(self):
        """Test inferring nested struct type."""
        data = {
            "user": {
                "id": 123,
                "profile": {
                    "name": "John",
                    "age": 30
                }
            }
        }

        result = BSONToDeltaConverter.infer_pyarrow_type(data)
        assert pa.types.is_struct(result)

        user_field = result.field("user")
        assert pa.types.is_struct(user_field.type)

        profile_field = user_field.type.field("profile")
        assert pa.types.is_struct(profile_field.type)

    def test_infer_timestamp_type(self):
        """Test inferring timestamp type from datetime."""
        from datetime import datetime

        result = BSONToDeltaConverter.infer_pyarrow_type(datetime.now())
        assert result == pa.timestamp('us')

    def test_infer_decimal_type(self):
        """Test inferring decimal type."""
        from decimal import Decimal

        result = BSONToDeltaConverter.infer_pyarrow_type(Decimal("123.45"))
        assert result == pa.decimal128(38, 10)

    def test_infer_objectid_type(self):
        """Test inferring type from ObjectId."""
        from bson import ObjectId

        result = BSONToDeltaConverter.infer_pyarrow_type(ObjectId())
        assert result == pa.string()

    def test_infer_binary_type(self):
        """Test inferring type from Binary."""
        from bson.binary import Binary

        result = BSONToDeltaConverter.infer_pyarrow_type(Binary(b"test"))
        assert result == pa.string()  # Binary is converted to base64 string
