"""Tests for join detection utility functions."""

from dataraum.analysis.relationships.joins import (
    ColumnStats,
    JoinAlgorithm,
    _are_types_compatible,
    _determine_cardinality,
    _get_cast_expression,
    _get_type_group,
    _is_temporal_type,
    _select_algorithm,
    _should_compare_columns,
)


class TestGetTypeGroup:
    """Tests for _get_type_group."""

    def test_numeric_types(self):
        assert _get_type_group("BIGINT") == "numeric"
        assert _get_type_group("INTEGER") == "numeric"
        assert _get_type_group("FLOAT") == "numeric"
        assert _get_type_group("DOUBLE") == "numeric"
        assert _get_type_group("DECIMAL") == "numeric"

    def test_string_types(self):
        assert _get_type_group("VARCHAR") == "string"
        assert _get_type_group("TEXT") == "string"

    def test_temporal_types(self):
        assert _get_type_group("DATE") == "temporal"
        assert _get_type_group("TIMESTAMP") == "temporal"
        assert _get_type_group("TIMESTAMP WITH TIME ZONE") == "temporal"

    def test_boolean_types(self):
        assert _get_type_group("BOOLEAN") == "boolean"
        assert _get_type_group("BOOL") == "boolean"

    def test_uuid_type(self):
        assert _get_type_group("UUID") == "uuid"

    def test_unknown_type(self):
        assert _get_type_group("BLOB") is None
        assert _get_type_group(None) is None

    def test_strips_precision(self):
        assert _get_type_group("DECIMAL(18,2)") == "numeric"
        assert _get_type_group("VARCHAR(255)") == "string"

    def test_case_insensitive(self):
        assert _get_type_group("bigint") == "numeric"
        assert _get_type_group("varchar") == "string"


class TestAreTypesCompatible:
    """Tests for _are_types_compatible."""

    def test_same_group_compatible(self):
        assert _are_types_compatible("BIGINT", "INTEGER")
        assert _are_types_compatible("VARCHAR", "TEXT")
        assert _are_types_compatible("DATE", "TIMESTAMP")

    def test_different_groups_incompatible(self):
        assert not _are_types_compatible("VARCHAR", "BIGINT")
        assert not _are_types_compatible("DATE", "INTEGER")

    def test_unknown_types_incompatible(self):
        assert not _are_types_compatible(None, "BIGINT")
        assert not _are_types_compatible("BIGINT", None)
        assert not _are_types_compatible("BLOB", "BIGINT")


class TestIsTemporalType:
    """Tests for _is_temporal_type."""

    def test_temporal(self):
        assert _is_temporal_type("DATE")
        assert _is_temporal_type("TIMESTAMP")

    def test_non_temporal(self):
        assert not _is_temporal_type("VARCHAR")
        assert not _is_temporal_type("BIGINT")
        assert not _is_temporal_type(None)


class TestGetCastExpression:
    """Tests for _get_cast_expression."""

    def test_temporal_casts(self):
        assert _get_cast_expression("created_at", "DATE") == '"created_at"::TIMESTAMP'
        assert _get_cast_expression("ts", "TIMESTAMP") == '"ts"::TIMESTAMP'

    def test_non_temporal_no_cast(self):
        assert _get_cast_expression("name", "VARCHAR") == '"name"'
        assert _get_cast_expression("id", "BIGINT") == '"id"'


class TestDetermineCardinality:
    """Tests for _determine_cardinality."""

    def _stats(self, *, unique: bool, distinct: int = 100, total: int = 100) -> ColumnStats:
        return ColumnStats(
            column_name="col",
            distinct_count=distinct,
            total_count=total,
            is_unique=unique,
        )

    def test_one_to_one(self):
        assert (
            _determine_cardinality(self._stats(unique=True), self._stats(unique=True))
            == "one-to-one"
        )

    def test_one_to_many(self):
        assert (
            _determine_cardinality(self._stats(unique=True), self._stats(unique=False))
            == "one-to-many"
        )

    def test_many_to_one(self):
        assert (
            _determine_cardinality(self._stats(unique=False), self._stats(unique=True))
            == "many-to-one"
        )

    def test_many_to_many(self):
        assert (
            _determine_cardinality(self._stats(unique=False), self._stats(unique=False))
            == "many-to-many"
        )


class TestSelectAlgorithm:
    """Tests for _select_algorithm."""

    def _stats(self, distinct: int) -> ColumnStats:
        return ColumnStats(
            column_name="col",
            distinct_count=distinct,
            total_count=distinct,
            is_unique=True,
        )

    def test_small_uses_exact(self):
        assert _select_algorithm(self._stats(100), self._stats(500)) == JoinAlgorithm.EXACT

    def test_medium_uses_sampled(self):
        assert _select_algorithm(self._stats(50_000), self._stats(50_000)) == JoinAlgorithm.SAMPLED

    def test_large_uses_minhash(self):
        assert (
            _select_algorithm(self._stats(2_000_000), self._stats(2_000_000))
            == JoinAlgorithm.MINHASH
        )


class TestShouldCompareColumns:
    """Tests for _should_compare_columns."""

    def _stats(
        self,
        *,
        distinct: int = 100,
        total: int = 100,
        resolved_type: str | None = "BIGINT",
    ) -> ColumnStats:
        return ColumnStats(
            column_name="col",
            distinct_count=distinct,
            total_count=total,
            is_unique=(distinct == total),
            resolved_type=resolved_type,
        )

    def test_compatible_types_compared(self):
        assert _should_compare_columns(
            self._stats(resolved_type="BIGINT"),
            self._stats(resolved_type="INTEGER"),
        )

    def test_incompatible_types_skipped(self):
        assert not _should_compare_columns(
            self._stats(resolved_type="VARCHAR"),
            self._stats(resolved_type="BIGINT"),
        )

    def test_constant_columns_skipped(self):
        assert not _should_compare_columns(
            self._stats(distinct=1),
            self._stats(distinct=100),
        )

    def test_extreme_cardinality_ratio_skipped(self):
        assert not _should_compare_columns(
            self._stats(distinct=1000),
            self._stats(distinct=2),
        )

    def test_unknown_types_skipped(self):
        assert not _should_compare_columns(
            self._stats(resolved_type=None),
            self._stats(resolved_type="BIGINT"),
        )
