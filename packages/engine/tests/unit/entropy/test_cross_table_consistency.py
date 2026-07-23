"""Tests for cross_table_consistency entropy detector (ADR-0017, DAT-865b/871).

The verdict is recomputed on demand: ``detect`` re-runs each record's
``sql_used`` via ``verdict_from_sql``, and reads the declared ``tolerance`` +
``severity`` + ``source`` (provenance, DAT-735) from the spec (loaded via
``_load_run_specs``, not the record). The unit tests patch BOTH — the verdict
(keyed by ``sql_used``) and the spec map (keyed by ``validation_id``) — so the
scoring + fan-out logic is tested in isolation; ``_score`` is tested directly
against verdicts.

``TestProvenancePooling`` pins DAT-865b's + DAT-871's acceptance at the
READINESS-BAND level (through ``entropy/loss.py`` +
``entropy/views/readiness_context.py``, the REAL shipped ``loss.yaml``
weights), not just the detector's raw score — mirroring the precedent in
``tests/unit/entropy/views/test_readiness_context.py``. DAT-871 caps the
GENERATED tier's contribution at its strongest single witness instead of
summing (same generator + served context ≠ independent evidence); the SEED
tier still sums.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import dataraum.entropy.detectors.computational.cross_table_consistency as ctc
from dataraum.analysis.validation.evaluate import ValidationVerdict
from dataraum.analysis.validation.models import ValidationStatus
from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.views.readiness_context import assemble_readiness_context


@pytest.fixture
def detector() -> ctc.CrossTableConsistencyDetector:
    return ctc.CrossTableConsistencyDetector()


def _verdict(
    status: ValidationStatus, *, deviation: float = 0.0, magnitude: float = 1.0
) -> ValidationVerdict:
    return ValidationVerdict(
        status=status,
        passed=status == ValidationStatus.PASSED,
        message="",
        details={"deviation": deviation, "magnitude": magnitude},
    )


def _spec(severity: str = "critical", tolerance: float = 0.01, source: str = "seed") -> MagicMock:
    spec = MagicMock()
    spec.severity = MagicMock()
    spec.severity.value = severity
    # The typed check definition (DAT-735): tolerance is a float field, not a
    # `parameters` dict entry — mirror the real ValidationSpec the detector reads.
    spec.tolerance = tolerance
    # Provenance (DAT-735/865b): 'seed' (shipped, human-reviewed) is the
    # default here since most tests exercise the pre-865b fan-out/scoring
    # logic, which is provenance-agnostic; provenance-specific tests below
    # pass ``source`` explicitly.
    spec.source = source
    return spec


def _make_result(
    *,
    sql_used: str = "SELECT 0 AS deviation, 1 AS magnitude",
    validation_id: str = "v1",
    columns_used: list[str] | None = None,
) -> MagicMock:
    r = MagicMock()
    r.sql_used = sql_used
    r.validation_id = validation_id
    r.columns_used = columns_used or []
    return r


def _install_verdicts(monkeypatch, mapping: dict[str, ValidationVerdict]) -> None:
    """Patch the on-demand re-run: sql_used → chosen verdict."""
    monkeypatch.setattr(ctc, "verdict_from_sql", lambda _conn, sql, **_kw: mapping[sql])


def _install_specs(monkeypatch, mapping: dict[str, MagicMock]) -> None:
    """Patch the spec load: validation_id → spec (severity + tolerance + source)."""
    monkeypatch.setattr(ctc, "_load_run_specs", lambda _ctx: mapping)


def _make_context(
    validations: list | None = None,
    table_id: str = "t1",
    table_name: str = "orders",
) -> DetectorContext:
    ctx = DetectorContext(table_id=table_id, table_name=table_name, duckdb_conn=MagicMock())
    if validations is not None:
        ctx.analysis_results["validation"] = validations
    return ctx


def _seed_journal_lines(session) -> tuple[str, dict[str, str]]:  # noqa: ANN001
    """Seed a ``journal_lines`` table with credit/debit/account_id columns."""
    from dataraum.storage import Column, Source, Table

    session.add(Source(source_id="src_v", name="src_v", source_type="csv"))
    table = Table(
        table_id="jl",
        source_id="src_v",
        table_name="journal_lines",  # narrow, workspace-unique (DAT-639)
        layer="typed",
    )
    session.add(table)
    session.flush()
    ids: dict[str, str] = {}
    for pos, name in enumerate(("credit", "debit", "account_id")):
        col = Column(table_id="jl", column_name=name, column_position=pos, resolved_type="DOUBLE")
        session.add(col)
        session.flush()
        ids[name] = col.column_id
    return "jl", ids


def _column_context(session, validations: list) -> DetectorContext:  # noqa: ANN001
    ctx = DetectorContext(
        table_id="jl",
        table_name="journal_lines",
        session=session,
        duckdb_conn=MagicMock(),
    )
    ctx.analysis_results["validation"] = validations
    return ctx


class TestScore:
    """The uniform deviation/magnitude scoring (ADR-0017)."""

    def test_passed_returns_zero(self) -> None:
        assert ctc._score(_verdict(ValidationStatus.PASSED), "critical") == 0.0

    def test_inconclusive_returns_zero(self) -> None:
        # Execution error / unbound / non-conforming output = ignorance, never a
        # risk measurement (the old 0.5 banded clean tables, DAT-439).
        assert ctc._score(_verdict(ValidationStatus.ERROR), "critical") == 0.0

    def test_failed_critical_is_categorical(self) -> None:
        # A CRITICAL identity failing beyond tolerance scores 1.0 regardless of
        # the relative magnitude: a 10% TB↔GL break scored as a rate was
        # invisible below the band while every GL deliverable was provably wrong.
        v = _verdict(ValidationStatus.FAILED, deviation=50_000, magnitude=50_000_000)
        assert ctc._score(v, "critical") == 1.0

    def test_failed_noncritical_is_relative_discrepancy(self) -> None:
        # $50k on $50M → honest 0.001 (no boost, DAT-442).
        v = _verdict(ValidationStatus.FAILED, deviation=50_000, magnitude=50_000_000)
        assert ctc._score(v, "high") == pytest.approx(0.001, abs=1e-4)

    def test_rate_scores_as_itself(self) -> None:
        # aggregate: deviation = rate, magnitude = 1 → 0.05.
        v = _verdict(ValidationStatus.FAILED, deviation=0.05, magnitude=1.0)
        assert ctc._score(v, "high") == pytest.approx(0.05, abs=1e-6)

    def test_zero_magnitude_falls_back(self) -> None:
        v = _verdict(ValidationStatus.FAILED, deviation=100, magnitude=0)
        assert ctc._score(v, "high") == 1.0


class TestDetectNoResults:
    def test_no_validations_returns_zero(self, detector: ctc.CrossTableConsistencyDetector) -> None:
        objects = detector.detect(_make_context(validations=[]))
        assert len(objects) == 1
        assert objects[0].score == 0.0

    def test_no_data_loaded(self, detector: ctc.CrossTableConsistencyDetector) -> None:
        objects = detector.detect(_make_context())
        assert len(objects) == 1
        assert objects[0].score == 0.0


class TestDetectFailures:
    def test_single_critical_failure(
        self, detector: ctc.CrossTableConsistencyDetector, monkeypatch
    ) -> None:
        """A lone (default seed-source) critical failure pools to reliability ×
        check_score = 0.95 × 1.0 (DAT-865b), not the raw categorical 1.0."""
        _install_specs(monkeypatch, {"v1": _spec("critical")})
        _install_verdicts(monkeypatch, {"s1": _verdict(ValidationStatus.FAILED, deviation=1)})
        ctx = _make_context(validations=[_make_result(sql_used="s1", validation_id="v1")])
        objects = detector.detect(ctx)
        assert len(objects) == 1
        assert objects[0].score == pytest.approx(0.95, abs=1e-6)

    def test_passed_check_contributes_nothing(
        self, detector: ctc.CrossTableConsistencyDetector, monkeypatch
    ) -> None:
        """A passed check alongside a failed one contributes no witness — the
        table's score comes entirely from the failing check's pooled evidence."""
        _install_specs(monkeypatch, {"v1": _spec("critical"), "v2": _spec("high")})
        _install_verdicts(
            monkeypatch,
            {
                "pass": _verdict(ValidationStatus.PASSED),
                "rate": _verdict(ValidationStatus.FAILED, deviation=0.05, magnitude=1.0),
            },
        )
        ctx = _make_context(
            validations=[
                _make_result(sql_used="pass", validation_id="v1"),
                _make_result(sql_used="rate", validation_id="v2"),
            ]
        )
        objects = detector.detect(ctx)
        # One seed witness: reliability 0.95 × honest rate 0.05.
        assert objects[0].score == pytest.approx(0.0475, abs=1e-6)

    def test_two_failures_pool_additively_at_table_grain(
        self, detector: ctc.CrossTableConsistencyDetector, monkeypatch
    ) -> None:
        """Two independent failing checks corroborate the SAME table-level
        "broken" claim (DAT-865b, senior review) — the table object pools
        every failing witness, not just the worst one, exactly like the
        column fan-out; ``readiness_context.py`` reads a ``table:`` target the
        same way it reads a ``column:`` one, so leaving this grain on the raw
        worst-score would let an unvetted check block the table through a
        side door even after the column fan-out was fixed."""
        _install_specs(monkeypatch, {"v_rate": _spec("high"), "v_critical": _spec("critical")})
        _install_verdicts(
            monkeypatch,
            {
                "rate": _verdict(ValidationStatus.FAILED, deviation=0.05, magnitude=1.0),
                "crit": _verdict(ValidationStatus.FAILED, deviation=1.0),
            },
        )
        ctx = _make_context(
            validations=[
                _make_result(sql_used="rate", validation_id="v_rate"),
                _make_result(sql_used="crit", validation_id="v_critical"),
            ]
        )
        objects = detector.detect(ctx)
        # Two seed witnesses: 0.95×0.05 + 0.95×1.0 = 0.9975 (same pooled
        # additive math as the column fan-out — same underlying witnesses).
        assert objects[0].score == pytest.approx(0.9975, abs=1e-6)

    def test_evidence_per_check(
        self, detector: ctc.CrossTableConsistencyDetector, monkeypatch
    ) -> None:
        _install_specs(monkeypatch, {"v1": _spec("critical"), "v2": _spec("high")})
        _install_verdicts(
            monkeypatch,
            {
                "pass": _verdict(ValidationStatus.PASSED),
                "fail": _verdict(ValidationStatus.FAILED, deviation=1),
            },
        )
        ctx = _make_context(
            validations=[
                _make_result(sql_used="pass", validation_id="v1"),
                _make_result(sql_used="fail", validation_id="v2"),
            ]
        )
        evidence = detector.detect(ctx)[0].evidence
        assert len(evidence) == 2
        assert evidence[0]["validation_id"] == "v1"
        assert evidence[1]["validation_id"] == "v2"


class TestColumnFanOut:
    """Failed checks band the columns their SQL touched.

    The band must reach the columns deliverable metrics flow through — a
    ``table:`` row joins to nothing downstream (scoreboard baseline:
    0 prevented / 8 wrong-delivered with the GL unbanded).

    Column scores are now the pooled evidence_mass (DAT-865b) — with the
    default ``source="seed"`` spec (reliability 0.95, the detector's
    ``DEFAULT_RELIABILITIES`` fallback), a single fully-confident witness
    (check_score 1.0) pools to ``0.95``, not the raw ``1.0`` — see
    ``TestProvenancePooling`` for the seed-vs-generated readiness-band tests.
    """

    def test_failed_check_bands_its_columns(
        self, detector: ctc.CrossTableConsistencyDetector, session, monkeypatch
    ) -> None:  # noqa: ANN001
        """Own-table columns matched by exact narrow name; foreign tables ignored;
        hallucinated columns dropped; column_id rides in evidence."""
        _, ids = _seed_journal_lines(session)
        _install_specs(monkeypatch, {"v1": _spec("critical")})
        _install_verdicts(monkeypatch, {"s1": _verdict(ValidationStatus.FAILED, deviation=0.1)})
        ctx = _column_context(
            session,
            [
                _make_result(
                    validation_id="v1",
                    sql_used="s1",
                    columns_used=[
                        "journal_lines.credit",  # ours
                        "journal_lines.debit",  # ours
                        "trial_balance.debit_balance",  # other table — not ours
                        "journal_lines.ghost",  # hallucinated column
                    ],
                )
            ],
        )
        objects = detector.detect(ctx)

        by_target = {o.target: o for o in objects}
        assert "column:journal_lines.credit" in by_target
        assert "column:journal_lines.debit" in by_target
        assert len(objects) == 3  # table object + 2 columns (ghost + foreign dropped)
        credit = by_target["column:journal_lines.credit"]
        # critical → categorical check_score 1.0, pooled through the default
        # seed reliability (0.95): a lone witness's evidence_mass = r × score.
        assert credit.score == pytest.approx(0.95, abs=1e-6)
        assert credit.evidence[0]["column_id"] == ids["credit"]
        assert credit.evidence[0]["source"] == "seed"
        assert credit.witnesses[0].reliability == pytest.approx(0.95, abs=1e-6)

    def test_passed_checks_band_nothing(
        self, detector: ctc.CrossTableConsistencyDetector, session, monkeypatch
    ) -> None:  # noqa: ANN001
        _seed_journal_lines(session)
        _install_specs(monkeypatch, {"v1": _spec("critical")})
        _install_verdicts(monkeypatch, {"sp": _verdict(ValidationStatus.PASSED)})
        ctx = _column_context(
            session,
            [
                _make_result(
                    validation_id="v1", sql_used="sp", columns_used=["journal_lines.credit"]
                )
            ],
        )
        objects = detector.detect(ctx)
        assert len(objects) == 1  # the table object only
        assert objects[0].score == 0.0

    def test_worst_score_wins_per_column(
        self, detector: ctc.CrossTableConsistencyDetector, session, monkeypatch
    ) -> None:  # noqa: ANN001
        """Two failing checks touching the same column → one object, pooled
        score (DAT-865b), both checks in evidence."""
        _, ids = _seed_journal_lines(session)
        _install_specs(monkeypatch, {"v_rate": _spec("high"), "v_critical": _spec("critical")})
        _install_verdicts(
            monkeypatch,
            {
                "rate": _verdict(ValidationStatus.FAILED, deviation=0.05, magnitude=1.0),
                "crit": _verdict(ValidationStatus.FAILED, deviation=1.0),
            },
        )
        ctx = _column_context(
            session,
            [
                _make_result(
                    validation_id="v_rate",
                    sql_used="rate",
                    columns_used=["journal_lines.credit"],
                ),
                _make_result(
                    validation_id="v_critical",
                    sql_used="crit",
                    columns_used=["journal_lines.credit"],
                ),
            ],
        )
        objects = detector.detect(ctx)
        column_objs = [o for o in objects if o.target.startswith("column:")]
        assert len(column_objs) == 1
        # Two seed witnesses pool additively: evidence_mass = 0.95×0.05 +
        # 0.95×1.0 = 0.9975 (both checks corroborate the same "broken" claim).
        assert column_objs[0].score == pytest.approx(0.9975, abs=1e-6)
        assert {e["validation_id"] for e in column_objs[0].evidence} == {"v_rate", "v_critical"}


class TestProvenancePooling:
    """DAT-865b + DAT-871 acceptance, pinned at the READINESS-BAND level.

    Feeds the real detector's output through the REAL shipped
    ``entropy/loss.yaml`` weights (``assemble_readiness_context``) — not just
    the detector's raw score — mirroring the precedent in
    ``tests/unit/entropy/views/test_readiness_context.py``. DAT-871 corrects
    DAT-865b's original additive-corroboration assumption for the GENERATED
    tier specifically (``test_generated_failures_cap_at_strongest_witness``)
    — same-draw generated checks share one generator + served context, so
    they are not independent evidence the way distinct seed templates are.
    """

    _CREDIT = "column:journal_lines.credit"
    # The TABLE-scoped target (DAT-865b, senior review): readiness_context.py
    # rolls a ``table:`` target up through the identical loss path as a
    # ``column:`` one (no special-casing) — and the cockpit's per-table "why"
    # tooling reads exactly this row — so a fix scoped only to the column
    # fan-out would leave the original DAT-865 bug live at table grain.
    _TABLE = "table:journal_lines"

    def test_seeded_critical_alone_blocks(
        self, detector: ctc.CrossTableConsistencyDetector, session, monkeypatch
    ) -> None:  # noqa: ANN001
        """(1) A seeded critical check failing alone → blocked (today's
        behavior), at BOTH the column and table grain."""
        _seed_journal_lines(session)
        _install_specs(monkeypatch, {"v1": _spec("critical", source="seed")})
        _install_verdicts(monkeypatch, {"s1": _verdict(ValidationStatus.FAILED, deviation=1)})
        ctx = _column_context(
            session,
            [
                _make_result(
                    validation_id="v1", sql_used="s1", columns_used=["journal_lines.credit"]
                )
            ],
        )
        readiness = assemble_readiness_context(detector.detect(ctx))
        assert readiness.columns[self._CREDIT].readiness == "blocked"
        assert readiness.columns[self._TABLE].readiness == "blocked"

    @pytest.mark.parametrize(
        ("severity", "deviation"),
        [
            ("critical", 1.0),  # categorical check_score 1.0 — the worst case
            ("high", 0.9),  # a severe but non-critical honest rate
        ],
    )
    def test_generated_failure_alone_never_blocks(
        self,
        detector: ctc.CrossTableConsistencyDetector,
        session,
        monkeypatch,
        severity: str,
        deviation: float,
    ) -> None:  # noqa: ANN001
        """(2) A generated check failing alone (any severity) → investigate,
        NEVER blocked — the DAT-865 clean-corpus scenario — at BOTH the
        column and table grain (the table grain is the exact surface DAT-865
        originally hit: a lone unvetted check categorically blocking)."""
        _seed_journal_lines(session)
        _install_specs(monkeypatch, {"v1": _spec(severity, source="generated")})
        _install_verdicts(
            monkeypatch, {"s1": _verdict(ValidationStatus.FAILED, deviation=deviation)}
        )
        ctx = _column_context(
            session,
            [
                _make_result(
                    validation_id="v1", sql_used="s1", columns_used=["journal_lines.credit"]
                )
            ],
        )
        readiness = assemble_readiness_context(detector.detect(ctx))
        assert readiness.columns[self._CREDIT].readiness == "investigate"
        assert readiness.columns[self._TABLE].readiness == "investigate"

    @pytest.mark.parametrize("n_generated", [2, 3])
    def test_generated_failures_cap_at_strongest_witness(
        self,
        detector: ctc.CrossTableConsistencyDetector,
        session,
        monkeypatch,
        n_generated: int,
    ) -> None:  # noqa: ANN001
        """(2/5, DAT-871) Two-or-more full-strength GENERATED failures
        corroborating the SAME claim do NOT escalate additively — a sweep
        re-run found exactly this (two semantically-wrong generated checks
        pooling 0.5+0.5=1.0, categorically blocking clean data: the DAT-865
        bug reopened through corroboration). Same-draw generated checks share
        one generator + one served context, so they are not independent
        logic; the detector caps their contribution at the single strongest
        witness. Pinned at N=2 AND N=3 to prove the cap holds "at any N", not
        just the reported N=2 case — score stays exactly 0.5 (never grows
        with N), NEVER crosses into 'blocked' at either grain, and every
        witness still persists (no evidence hiding)."""
        _seed_journal_lines(session)
        specs = {f"v{i}": _spec("critical", source="generated") for i in range(n_generated)}
        verdicts = {
            f"s{i}": _verdict(ValidationStatus.FAILED, deviation=1) for i in range(n_generated)
        }
        _install_specs(monkeypatch, specs)
        _install_verdicts(monkeypatch, verdicts)
        ctx = _column_context(
            session,
            [
                _make_result(
                    validation_id=f"v{i}", sql_used=f"s{i}", columns_used=["journal_lines.credit"]
                )
                for i in range(n_generated)
            ],
        )
        objects = detector.detect(ctx)
        column_obj = next(o for o in objects if o.target == self._CREDIT)
        table_obj = next(o for o in objects if o.target == self._TABLE)
        # N generated witnesses (reliability 0.5 each, full check_score 1.0)
        # cap at their single strongest: evidence_mass = max(0.5, ...) = 0.5,
        # NOT a sum — regardless of N.
        assert column_obj.score == pytest.approx(0.5, abs=1e-6)
        assert table_obj.score == pytest.approx(0.5, abs=1e-6)
        # (5) All N witnesses persisted at both grains — the cap changes how
        # mass reaches the score, never whether a witness is recorded.
        assert len(column_obj.witnesses) == n_generated
        assert len(table_obj.witnesses) == n_generated
        # Persisted evidence stays honest about the CAPPED mass regardless of
        # N: pool_evidence_mass never reports the uncapped sum (would be
        # n_generated × 0.5), and pool_ignorance is derived from that SAME
        # capped 0.5 (1 / (1 + 0.5)), never from the uncapped figure.
        assert column_obj.evidence[0]["pool_evidence_mass"] == pytest.approx(0.5, abs=1e-6)
        assert column_obj.evidence[0]["pool_ignorance"] == pytest.approx(1 / 1.5, abs=1e-6)
        assert column_obj.evidence[0]["pool_conflict"] == 0.0
        readiness = assemble_readiness_context(objects)
        # risk = 0.8 (aggregation_intent weight) × 0.5 = 0.4 ≤ medium_upper
        # (0.6) → 'investigate', never 'blocked', at either grain.
        assert readiness.columns[self._CREDIT].readiness == "investigate"
        assert readiness.columns[self._TABLE].readiness == "investigate"

    def test_seed_and_generated_escalate_additively(
        self, detector: ctc.CrossTableConsistencyDetector, session, monkeypatch
    ) -> None:  # noqa: ANN001
        """(3) A seeded critical failure PLUS a generated failure still
        escalates past 'blocked' additively — only within-generated-tier
        corroboration is capped (DAT-871), seed-tier mass still sums with
        anything else. evidence_mass = 0.95 (seed) + 0.5 (generated) = 1.45,
        clamped to the existing [0, 1] score range → 1.0 → blocked."""
        _seed_journal_lines(session)
        _install_specs(
            monkeypatch,
            {
                "v_seed": _spec("critical", source="seed"),
                "v_gen": _spec("critical", source="generated"),
            },
        )
        _install_verdicts(
            monkeypatch,
            {
                "s_seed": _verdict(ValidationStatus.FAILED, deviation=1),
                "s_gen": _verdict(ValidationStatus.FAILED, deviation=1),
            },
        )
        ctx = _column_context(
            session,
            [
                _make_result(
                    validation_id="v_seed",
                    sql_used="s_seed",
                    columns_used=["journal_lines.credit"],
                ),
                _make_result(
                    validation_id="v_gen", sql_used="s_gen", columns_used=["journal_lines.credit"]
                ),
            ],
        )
        objects = detector.detect(ctx)
        column_obj = next(o for o in objects if o.target == self._CREDIT)
        table_obj = next(o for o in objects if o.target == self._TABLE)
        assert column_obj.score == pytest.approx(1.0, abs=1e-6)
        assert table_obj.score == pytest.approx(1.0, abs=1e-6)
        assert len(column_obj.witnesses) == 2
        assert len(table_obj.witnesses) == 2
        # The persisted mass is the UNCLAMPED 1.45 (0.95 + 0.5) — score clamps
        # to [0, 1], but pool_evidence_mass stays honest about the actual
        # (unbounded) capped mass, per PoolResult's own evidence_mass
        # contract; ignorance is derived from that same 1.45.
        assert column_obj.evidence[0]["pool_evidence_mass"] == pytest.approx(1.45, abs=1e-6)
        assert column_obj.evidence[0]["pool_ignorance"] == pytest.approx(1 / 2.45, abs=1e-6)
        assert column_obj.evidence[0]["pool_conflict"] == 0.0
        readiness = assemble_readiness_context(objects)
        assert readiness.columns[self._CREDIT].readiness == "blocked"
        assert readiness.columns[self._TABLE].readiness == "blocked"

    def test_duplicate_columns_used_does_not_double_count_seed(
        self, detector: ctc.CrossTableConsistencyDetector, session, monkeypatch
    ) -> None:  # noqa: ANN001
        """A single SEED check naming the same column twice in ``columns_used``
        (nothing upstream forbids it) must contribute exactly ONE witness —
        not two — or it would silently double its own evidence_mass (0.95 →
        clamped 1.0): the seed tier still sums additively (DAT-871), so
        double-counting one check as two witnesses is a live SCORING bug for
        THIS tier (unlike the generated tier below, whose max-cap makes a
        duplicate score-inert) — and it would collide on
        ``ClaimWitnessRecord``'s ``(target, claim_field, witness_id,
        run_id)`` unique constraint regardless of tier."""
        _seed_journal_lines(session)
        _install_specs(monkeypatch, {"v1": _spec("critical", source="seed")})
        _install_verdicts(monkeypatch, {"s1": _verdict(ValidationStatus.FAILED, deviation=1)})
        ctx = _column_context(
            session,
            [
                _make_result(
                    validation_id="v1",
                    sql_used="s1",
                    columns_used=["journal_lines.credit", "journal_lines.credit"],
                )
            ],
        )
        objects = detector.detect(ctx)
        column_obj = next(o for o in objects if o.target == self._CREDIT)
        # Exactly one witness (reliability 0.95), not two (which would double
        # to 1.9, clamped to 1.0 — a different exact score, still 'blocked'
        # either way at this severity, so the score assertion is what bites).
        assert len(column_obj.witnesses) == 1
        assert column_obj.score == pytest.approx(0.95, abs=1e-6)
        readiness = assemble_readiness_context(objects)
        assert readiness.columns[self._CREDIT].readiness == "blocked"

    def test_duplicate_columns_used_does_not_double_count_generated(
        self, detector: ctc.CrossTableConsistencyDetector, session, monkeypatch
    ) -> None:  # noqa: ANN001
        """Same guard for a GENERATED check. The tier's MAX cap (DAT-871)
        means a duplicate name can't inflate the SCORE here (``max(0.5, 0.5)
        == 0.5`` either way) — but persistence must still record exactly ONE
        witness, not two, or it collides on ``ClaimWitnessRecord``'s unique
        constraint and silently overstates how many checks corroborated this
        column if a second, genuinely distinct generated check later joins
        it."""
        _seed_journal_lines(session)
        _install_specs(monkeypatch, {"v1": _spec("critical", source="generated")})
        _install_verdicts(monkeypatch, {"s1": _verdict(ValidationStatus.FAILED, deviation=1)})
        ctx = _column_context(
            session,
            [
                _make_result(
                    validation_id="v1",
                    sql_used="s1",
                    columns_used=["journal_lines.credit", "journal_lines.credit"],
                )
            ],
        )
        objects = detector.detect(ctx)
        column_obj = next(o for o in objects if o.target == self._CREDIT)
        assert len(column_obj.witnesses) == 1
        assert column_obj.score == pytest.approx(0.5, abs=1e-6)
        readiness = assemble_readiness_context(objects)
        assert readiness.columns[self._CREDIT].readiness == "investigate"


class TestDetectorProperties:
    def test_detector_id(self, detector: ctc.CrossTableConsistencyDetector) -> None:
        assert detector.detector_id == "cross_table_consistency"

    def test_scope(self, detector: ctc.CrossTableConsistencyDetector) -> None:
        assert detector.scope == "table"

    def test_layer(self, detector: ctc.CrossTableConsistencyDetector) -> None:
        assert str(detector.layer) == "computational"

    def test_required_analyses(self, detector: ctc.CrossTableConsistencyDetector) -> None:
        assert str(detector.required_analyses[0]) == "validation"
