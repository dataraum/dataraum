"""Tests for join detection utility functions."""

from collections.abc import Iterator

import duckdb
import pytest

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
    find_join_columns,
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

    def test_medium_uses_exact(self):
        # The 10K–1M reservoir-sampled band was deleted in DAT-794 (slower than
        # exact AND nondeterministic); everything below 1M distinct is exact.
        assert _select_algorithm(self._stats(50_000), self._stats(50_000)) == JoinAlgorithm.EXACT

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

    def test_boolean_columns_skipped(self):
        """Boolean columns always trivially contain each other ({True, False})."""
        assert not _should_compare_columns(
            self._stats(distinct=2, resolved_type="BOOLEAN"),
            self._stats(distinct=2, resolved_type="BOOLEAN"),
        )


class TestContainmentRescueGate:
    """DAT-725: the containment rescue is gated on the REFERENCED side being a key.

    An FK target must be a key, so 100% (or near-100%) containment into a
    (near-)unique referenced column is FK-shaped evidence at ANY cardinality of
    the contained side — the old ``min_distinct > 10`` floor made a
    low-distinct FK column structurally unproposable, leaving its confirmation
    to the synthesis LLM volunteering the edge (jitter). Trivial mutual
    containment of value-pool columns stays dead: neither side is a key.
    """

    @pytest.fixture
    def conn(self) -> Iterator[duckdb.DuckDBPyConnection]:
        c = duckdb.connect(":memory:")
        try:
            yield c
        finally:
            c.close()

    def test_low_distinct_fk_into_unique_key_is_a_candidate(
        self, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A 2-distinct FK column fully contained in a unique key is rescued.

        Mirrors the observed shape: a fact table referencing only 2 of a
        reference table's 60 unique keys. Jaccard 2/60 ≈ 0.03 is far below
        min_score; containment (2/2 = 1.0) into a unique referenced column
        must carry the pair into the candidate set.
        """
        conn.execute("CREATE TABLE fact AS SELECT (range % 2) + 1 AS ref_code FROM range(100)")
        conn.execute("CREATE TABLE ref AS SELECT range + 1 AS code FROM range(60)")

        candidates = find_join_columns(
            conn,
            "fact",
            "ref",
            ["ref_code"],
            ["code"],
            column_types1={"ref_code": "BIGINT"},
            column_types2={"code": "BIGINT"},
        )

        assert len(candidates) == 1
        (candidate,) = candidates
        assert candidate["join_confidence"] == 1.0
        assert candidate["cardinality"] == "many-to-one"

    def test_containment_into_non_unique_column_stays_dead(
        self, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """The preserved property: value-pool containment does not resurrect.

        A 5-value code column fully contained in another repeating code column
        (40 distinct over 200 rows — uniqueness 0.2, not a key) shares a value
        namespace, not an entity. Jaccard 5/40 = 0.125 sits below min_score and
        the rescue must NOT fire: the referenced side is not a key.
        """
        conn.execute("CREATE TABLE tickets AS SELECT (range % 5) + 1 AS code FROM range(50)")
        conn.execute("CREATE TABLE history AS SELECT (range % 40) + 1 AS code FROM range(200)")

        candidates = find_join_columns(
            conn,
            "tickets",
            "history",
            ["code"],
            ["code"],
            column_types1={"code": "BIGINT"},
            column_types2={"code": "BIGINT"},
        )

        assert candidates == []

    def test_near_unique_referenced_key_tolerates_dirt(
        self, conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A referenced key with a few duplicate-loaded rows still rescues.

        60 distinct over 62 rows (uniqueness ≈ 0.968) clears REF_UNIQUENESS_MIN:
        dirt, not structure. The score stays the honest containment fraction.
        """
        conn.execute("CREATE TABLE fact AS SELECT (range % 2) + 1 AS ref_code FROM range(100)")
        conn.execute(
            "CREATE TABLE ref AS SELECT range + 1 AS code FROM range(60) "
            "UNION ALL SELECT 1 UNION ALL SELECT 2"
        )

        candidates = find_join_columns(
            conn,
            "fact",
            "ref",
            ["ref_code"],
            ["code"],
            column_types1={"ref_code": "BIGINT"},
            column_types2={"code": "BIGINT"},
        )

        assert len(candidates) == 1
        assert candidates[0]["join_confidence"] == 1.0
