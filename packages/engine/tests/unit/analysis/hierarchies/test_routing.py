"""Deterministic class routing (DAT-762 Phase B) — synthetic unit pins.

The full acceptance is the 45-cell scorecard fixture in dataraum-eval
(calibration/unit/test_dimension_identity_routing.py): zero leakage into the
stats-owned classes, full recall on the veto classes. These synthetic tests
pin the shape classifier and each predicate's boundary conditions.
"""

from __future__ import annotations

from dataraum.analysis.hierarchies.routing import (
    FREE_TEXT_DETERMINANT,
    PROXY_BIJECTION,
    QUASI_IDENTIFIER,
    ColumnEvidence,
    classify_shape,
    route_alias,
    route_edge,
)


def _ev(values: list[str], *, n_rows: int = 10_000, n_distinct: int | None = None,
        dtype: str = "VARCHAR") -> ColumnEvidence:
    return ColumnEvidence(
        n_rows=n_rows,
        n_distinct=n_distinct if n_distinct is not None else len(values),
        dtype=dtype,
        sample_values=values,
    )


class TestClassifyShape:
    def test_temporal_by_dtype(self):
        assert classify_shape(_ev(["a", "b"], dtype="TIMESTAMP")) == "temporal"

    def test_temporal_by_iso_values(self):
        assert classify_shape(_ev(["2025-01-01", "2025-02-01"])) == "temporal"

    def test_prose_needs_whitespace_not_just_length(self):
        # A 64-char hash is idlike, not prose — the rel-hm postal-code lesson.
        assert classify_shape(_ev(["a1" * 32, "b2" * 32])) == "idlike"
        assert classify_shape(_ev(["a soft jersey top with long sleeves and a round neck"])) == "prose"

    def test_name_is_digitless_alpha(self):
        assert classify_shape(_ev(["Schumacher", "Hamilton", "Alonso"])) == "name"

    def test_idlike_needs_digits(self):
        assert classify_shape(_ev(["INV-001", "INV-002"])) == "idlike"

    def test_label_is_whitespaced_short_text(self):
        assert classify_shape(_ev(["Dark Blue", "Light Grey"])) == "label"

    def test_empty(self):
        assert classify_shape(_ev([])) == "empty"


class TestRouteEdge:
    def test_temporal_determinant_is_quasi_identifier(self):
        det = _ev(["2025-01-01", "2025-01-02"], n_distinct=800, dtype="DATE")
        dep = _ev(["North", "South"], n_distinct=40)
        assert route_edge(det, dep) == QUASI_IDENTIFIER

    def test_prose_determinant_is_free_text(self):
        det = _ev(["a soft jersey top with long sleeves"], n_rows=190_000, n_distinct=15_000)
        dep = _ev(["Jersey Basic"], n_rows=190_000, n_distinct=20)
        assert route_edge(det, dep) == FREE_TEXT_DETERMINANT

    def test_id_over_tiny_enum_is_quasi_identifier(self):
        det = _ev(["7a3f9c" * 10, "8b4e2d" * 10], n_rows=190_000, n_distinct=50_000)
        dep = _ev(["ACTIVE", "LEFT CLUB"], n_rows=190_000, n_distinct=3)
        assert route_edge(det, dep) == QUASI_IDENTIFIER

    def test_entity_scale_names_are_quasi_identifier(self):
        det = _ev(["Schumacher", "Hamilton"], n_distinct=798)
        dep = _ev(["German", "British"], n_distinct=42)
        assert route_edge(det, dep) == QUASI_IDENTIFIER

    def test_level_scale_names_stay_unrouted(self):
        # Department names ARE legitimate coarse levels (the F3 shape).
        det = _ev(["Jersey", "Trouser"], n_distinct=120)
        dep = _ev(["Garment Upper body"], n_distinct=20)
        assert route_edge(det, dep) is None

    def test_code_determinant_with_fan_in_stays_unrouted(self):
        # The dirty-true hierarchy shape (F1): id-coded fine level, real fan-in.
        det = _ev(["339252", "413029"], n_distinct=16_210, n_rows=190_000)
        dep = _ev(["Dresses Ladies"], n_distinct=114, n_rows=190_000)
        assert route_edge(det, dep) is None

    def test_near_key_determinant_refused(self):
        det = _ev(["2025-01-01 10:00:00"], n_rows=50_000, n_distinct=49_963,
                  dtype="TIMESTAMP")
        dep = _ev(["G1"], n_distinct=500)
        assert route_edge(det, dep) is None


class TestRouteAlias:
    def test_id_timestamp_bijection_is_proxy(self):
        a = _ev(["841", "842"], n_distinct=1_091)
        b = _ev(["2025-03-01"], n_distinct=1_091, dtype="DATE")
        assert route_alias(a, b) == PROXY_BIJECTION

    def test_id_prose_bijection_is_proxy(self):
        # The DAT-761 residue: entry_key <-> desc_entry.
        a = _ev(["JE-000001", "JE-000002"], n_distinct=11_754)
        b = _ev(["monthly accrual posting for the northern region office"],
                n_distinct=11_754)
        assert route_alias(a, b) == PROXY_BIJECTION

    def test_code_label_alias_stays_unrouted(self):
        # A true code<->name alias (the A cells) is never judged.
        a = _ev(["09", "10", "11"], n_distinct=50)
        b = _ev(["Dark Blue", "Light Grey"], n_distinct=50)
        assert route_alias(a, b) is None

    def test_same_shape_pair_stays_unrouted(self):
        a = _ev(["2025-01-01"], n_distinct=1_091, dtype="DATE")
        b = _ev(["2025-01-01"], n_distinct=1_091, dtype="DATE")
        assert route_alias(a, b) is None
