"""Cross-table consistency entropy detector.

Consumes ValidationResultRecord (the grounded ``sql_used`` + declared params).
The verdict is **recomputed on demand** (ADR-0017): this detector re-runs each
check's run-versioned ``sql_used`` against current data and scores the fresh
verdict — it never reads a stored pass/fail (a stored verdict goes stale on
re-import, the SQL does not).

Scope: table-level, with COLUMN-grain objects fanned out for failed checks
(DAT-432): a failed reconciliation bands the columns its SQL actually
touched (``columns_used``), so the band reaches the columns deliverable
metrics flow through — not just an aggregate ``table:`` row. The table row is
not inert either, though (DAT-865b correction below): ``readiness_context.py``
rolls it up through the identical loss path as a column.

Score semantics (DAT-442 honesty + the scoreboard finding below):
- A failed CRITICAL check is CATEGORICAL: score 1.0. The spec's own tolerance
  already decided pass/fail, so "failed critical" means a declared identity is
  broken beyond its declared tolerance — the magnitude stays in evidence as
  the diagnostic. (Honest rates put the injected 10% TB↔GL break at risk
  0.8×0.10 = 0.08 — invisible below the 0.3 band — while every GL-derived
  deliverable number was measurably wrong: 0 prevented / 8 wrong-delivered.)
- Non-critical failures score the honest relative discrepancy ``deviation /
  magnitude`` (no boost, DAT-442) — uniform across check types now that the SQL
  output is contracted (ADR-0017), no per-check_type rate matching.
- ERROR/inconclusive (or unbound) scores 0.0 + a ``validation_unassessed``
  warning: an unassessed check is ignorance, not measured risk — the old 0.5
  turned LLM SQL-generation nondeterminism into clean-table false alarms.

``_score`` itself is UNCHANGED by DAT-865b below — the measured evidence for a
critical failure is still categorically 1.0. What changed is how that
per-check evidence reaches BOTH the table object's and the column fan-out's
score.

Aggregation (DAT-865b, ADR-0009 witness pool): a sweep finding (DAT-865)
caught one semantically-wrong GENERATED validation check — an LLM-authored
rule from agentic induction (DAT-735), never human-reviewed — failing on
CLEAN data and categorically blocking 15 columns, indistinguishable from a
real SEEDED (shipped, human-reviewed) critical failure. The lead's ruling: the
categorical 1.0 stays (it is the honest per-check evidence) — what must change
is that a target's score comes from POOLING its failing checks as
provenance-weighted witnesses, not from taking the raw worst one. Each failing
check becomes a fully-confident ``Witness`` on a "broken vs intact" claim
(a failed check is a definitive fact, not a graded uncertainty); its own
evidence strength (``_score``'s value) and its provenance-derived trust
(``ValidationSpec.source`` — 'seed' vs 'generated', DAT-735, already threaded
through the existing ``_load_run_specs`` spec map, no new plumbing) both fold
into the witness's ``reliability`` — pool()'s weighting axis — so the honest,
ungraded magnitude (DAT-442) is never distorted by the claim-space entropy
math. A LONE witness's only lever into the loss path is ``evidence_mass``
(``pool()`` forces ``conflict=0.0`` at n=1): a lone unvetted (generated) check
can no longer single-handedly block a target, and a lone vetted (seed)
critical failure still clears the blocked band.

Correlated corroboration is NOT independent evidence (DAT-871): a sweep
re-run caught TWO semantically-wrong generated checks pooling additively to
0.5+0.5=1.0 — categorically blocked, the exact DAT-865 failure mode reopened
through corroboration instead of a lone witness. The checks were never
independent: same generator (agentic induction, DAT-735), same served
context, drawn in the same run — sibling checks over the same graph, not
separately-reviewed logic. SEED checks corroborate additively (distinct,
human-reviewed templates ARE independent logic — ``Σ rᵢ·check_scoreᵢ``, no
hand-tuned special case). GENERATED-tier checks (including the
provenance-unknown fallback, ``_UNKNOWN_SOURCE_FALLBACK``) contribute at most
their single STRONGEST witness — ``max(rᵢ·check_scoreᵢ)``, never a sum — so N
generated-only failures can never band a target blocked, at any N. The two
tiers still combine additively WITH each other (a seeded critical plus a
generated failure escalates past the blocked band, DAT-871 acceptance #3) —
only the within-generated-tier sum is replaced by a max. See
``_capped_evidence_mass``.

BOTH grains pool (senior review, DAT-865b): the TABLE-scoped object and the
COLUMN fan-out (DAT-432) are NOT independent severities — ``readiness_context.py``
rolls a ``table:`` target up the SAME loss table as a ``column:`` one (no
special-casing), and the cockpit's per-table "why" tooling reads that row
directly. Leaving the table grain on the raw ``max(scores)`` would have let the
exact DAT-865 bug through a side door (one unvetted generated check still
categorically blocking the table even after the column fan-out was fixed). The
table object therefore pools ALL of this table's failing-check witnesses (one
shared "is this table's cross-table reconciliation broken" claim); each column
object pools the SUBSET whose check named that column — the same witnesses,
grouped at two grains, never two different claims.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any, NamedTuple

from sqlalchemy import select

from dataraum.analysis.validation.evaluate import (
    DEFAULT_TOLERANCE,
    ValidationVerdict,
    verdict_from_sql,
)
from dataraum.analysis.validation.models import ValidationStatus
from dataraum.core.logging import get_logger
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject, WitnessClaim
from dataraum.entropy.pooling import PoolResult, Witness, pool

logger = get_logger(__name__)

# The pooled claim (ADR-0009, DAT-865b): does this target's (table's or
# column's) cross-table reconciliation hold? Every FAILING check is a witness
# leaning fully "broken"; PASSING checks contribute nothing (score 0.0, no
# witness) — same as before pooling existed, so an all-clean target still
# bands nothing.
CLAIM_SPACE: tuple[str, str] = ("broken", "intact")

# Neutral fallback reliabilities — used only when the run has no calibrated
# artifact threaded in (direct/test callers). The SHIPPED values live in
# dataraum-config/entropy/reliabilities.yaml (placeholder priors, DAT-450/865b),
# loaded via ``load_data`` and threaded through ``context.analysis_results``.
# Keyed by the check's PROVENANCE (``ValidationSpec.source``, DAT-735): 'seed'
# is the shipped, human-reviewed vertical config; 'generated' is agentic
# induction's proposal over the served graph — unvetted until a human confirms
# it (the DAT-865 finding this fixes).
DEFAULT_RELIABILITIES: dict[str, float] = {
    "seed": 0.95,
    "generated": 0.5,
}

# A result whose validation_id has no resolvable spec, OR whose spec carries
# neither 'seed' nor 'generated' (a deleted/superseded check; a frame-2
# `validation` TEACH-authored check, DAT-441/core/overlay.py, which lands with
# ValidationSpec's own "config" default since a teach payload carries no
# `source` key — a LIVE production path, not merely a legacy/test artifact)
# never inherits seed-level trust — fall back to the lower tier. This
# under-trusts a genuine deliberate human teach relative to derived_value's
# human_declaration (0.875); a documented, uncalibrated placeholder choice,
# not a bug — see DEFAULT_RELIABILITIES above for the two calibrated tiers.
_UNKNOWN_SOURCE_FALLBACK = "generated"


def _score(verdict: ValidationVerdict, severity: str) -> float:
    """Score a recomputed validation verdict (ADR-0017).

    The verdict is recomputed on demand from the contracted SQL output, so its
    ``details`` carry a uniform ``deviation``/``magnitude`` — no per-check_type
    branching, no column-name guessing.

    Args:
        verdict: The freshly recomputed verdict (deviation/magnitude in details).
        severity: The declared severity (from the result record).

    Returns:
        Score between 0.0 (passed / unassessed) and 1.0 (critical failure).
    """
    if verdict.passed:
        return 0.0

    if verdict.status != ValidationStatus.FAILED:
        # INCONCLUSIVE (the SQL ran but didn't honor the contract) or UNBOUND
        # (no sql_used) — ignorance, never a risk measurement. The old 0.5
        # banded CLEAN tables on nondeterministic SQL failures (DAT-439); the
        # caller logs the unassessed check.
        return 0.0

    if severity == "critical":
        # Categorical: a CRITICAL identity failed beyond its declared
        # tolerance — the books don't reconcile. The relative magnitude stays
        # in evidence; scoring it as a rate hid provably-wrong deliverables.
        return 1.0

    # Honest relative discrepancy (no boost, DAT-442): deviation / magnitude.
    deviation = abs(float(verdict.details.get("deviation", 0) or 0))
    magnitude = abs(float(verdict.details.get("magnitude", 1) or 0)) or 1.0
    return min(1.0, deviation / magnitude)


def _tier(source: str | None) -> str:
    """Resolve a check's POOLING TIER from its provenance (DAT-871).

    Literal 'seed' (shipped, human-reviewed — distinct templates, independent
    logic) is the seed tier; everything else — 'generated' and any provenance
    without a dedicated calibrated entry (``_UNKNOWN_SOURCE_FALLBACK``) —
    falls into the generated tier: same generator + same served context,
    correlated rather than independent. This is the same key resolution
    ``_reliability_for_source`` uses to look up a NUMBER; here it decides
    which side of the additive/max split (``_capped_evidence_mass``) a
    witness's mass falls on.
    """
    return source if source in DEFAULT_RELIABILITIES else _UNKNOWN_SOURCE_FALLBACK


def _reliability_for_source(source: str | None, reliabilities: Mapping[str, float]) -> float:
    """The pooling reliability for one check, keyed by its PROVENANCE.

    ``source`` is the typed ``ValidationSpec.source`` the detector already
    reads off the spec map — 'seed' or 'generated' (DAT-735) — never
    string-sniffed. Anything else (no resolvable spec, or a spec whose
    ``source`` is neither — e.g. a live frame-2 teach-authored check, see
    ``_UNKNOWN_SOURCE_FALLBACK``) falls back to the GENERATED tier: unknown
    provenance never gets seed-level trust.
    """
    key = _tier(source)
    return reliabilities.get(key, DEFAULT_RELIABILITIES[key])


class _TieredWitness(NamedTuple):
    """A check's witness plus its pooling TIER (DAT-871).

    ``pool()`` itself stays generic (ADR-0009 machinery other detectors are
    calibrated on) and knows nothing of tiers — this wrapper is local to this
    detector's OWN consumption of ``Witness``, letting ``_capped_evidence_mass``
    split additive (seed) from capped (generated) contributions without
    threading a tier concept through the pooling engine.
    """

    witness: Witness
    tier: str


def _check_witness(
    validation_id: str,
    source: str | None,
    check_score: float,
    reliabilities: Mapping[str, float],
) -> _TieredWitness:
    """One failing check's witness on a target's "broken" claim (ADR-0009).

    The distribution is always fully confident — ``(broken=1.0, intact=0.0)``
    — because a FAILED check (beyond its declared tolerance) is a definitive
    binary fact, not a graded uncertainty; grading lives in ``check_score``
    itself (still 1.0 for a critical failure, the honest deviation/magnitude
    ratio otherwise, per ``_score``). Both the check's own evidence strength
    AND its provenance-derived trust fold into ``reliability`` — pool()'s
    weighting axis — rather than the distribution: encoding magnitude into the
    distribution instead would run it through the claim-space entropy math,
    which is wildly nonlinear near 0.5 and would distort DAT-442's honest,
    no-boost rate. Folding it into reliability instead keeps a witness's
    contribution to ``evidence_mass`` exactly ``reliability(source) ×
    check_score`` — linear. Independent (seed) corroboration escalates
    additively (``Σ rᵢ·check_scoreᵢ``); correlated (generated) corroboration
    does NOT — it is capped at its single strongest witness instead
    (``_capped_evidence_mass``, DAT-871). A LONE witness's only lever into the
    loss path is that same effective mass (``pool()`` forces ``conflict=0.0``
    at n=1, DAT-865b).
    """
    reliability = _reliability_for_source(source, reliabilities) * max(0.0, min(1.0, check_score))
    witness = Witness(
        witness_id=f"validation:{validation_id}",
        distribution=(1.0, 0.0),
        reliability=reliability,
    )
    return _TieredWitness(witness=witness, tier=_tier(source))


def _capped_evidence_mass(witnesses: Sequence[_TieredWitness]) -> float:
    """The effective evidence mass driving CTC's score (DAT-871).

    Seed-tier witnesses are distinct, human-reviewed templates — independent
    logic — so they corroborate additively, exactly like ``pool()``'s own
    ``evidence_mass`` would compute: ``Σ reliability``. (Every CTC witness
    carries a one-hot ``(1.0, 0.0)`` distribution — a failed check is a
    definitive fact, never a graded uncertainty — so ``pool()``'s per-witness
    ``certainty`` term is always exactly 1, and its ``evidence_mass``
    contribution collapses to just ``reliability``; this lets the sum be
    computed directly here without duplicating pool()'s certainty math or
    touching ``pool()`` itself.)

    Generated-tier witnesses (agentic induction's own proposals, DAT-735,
    including the provenance-unknown fallback) are NOT independent evidence:
    a same-draw batch shares one generator and one served context, so a
    sweep re-run found two semantically-wrong generated checks corroborating
    additively to a categorical block on clean data (DAT-871) — the DAT-865
    bug reopened through corroboration. Their contribution is capped at the
    single STRONGEST generated witness instead of summed, so N generated-only
    failures can never band a target blocked, at any N.

    The two tiers still combine additively WITH each other — a seeded
    critical plus a generated failure escalates past the blocked band
    (acceptance #3) — only the within-generated-tier sum becomes a max.

    Unbounded above (like ``pool()``'s own ``evidence_mass`` — see
    ``PoolResult``); callers clamp to ``[0, 1]`` for the ``EntropyObject``
    score.
    """
    seed_mass = math.fsum(tw.witness.reliability for tw in witnesses if tw.tier == "seed")
    generated_masses = [tw.witness.reliability for tw in witnesses if tw.tier != "seed"]
    generated_mass = max(generated_masses) if generated_masses else 0.0
    return seed_mass + generated_mass


def _witness_claims(witnesses: Sequence[_TieredWitness]) -> list[WitnessClaim]:
    """The persisted provenance trace (ADR-0009) for one target's witnesses.

    Shared by the table-scoped object and every column fan-out object — both
    grains pool the same underlying witnesses (DAT-865b), just grouped
    differently, so they share one claim_field. ALL witnesses persist here
    regardless of tier or the DAT-871 cap — the cap changes how a witness's
    mass reaches the SCORE, never whether it is recorded (no evidence
    hiding).
    """
    return [
        WitnessClaim(
            claim_field="cross_table_consistency",
            witness_id=tw.witness.witness_id,
            distribution=dict(zip(CLAIM_SPACE, tw.witness.distribution, strict=True)),
            reliability=tw.witness.reliability,
        )
        for tw in witnesses
    ]


def _load_run_specs(context: DetectorContext) -> dict[str, Any]:
    """Load this run's validation specs (severity + tolerance) from config.

    The verdict's tolerance and the critical-rule severity are declared config,
    not stored on the record (ADR-0017). The run's vertical is read from a
    validation lifecycle artifact via the shared session — the entropy/detect
    layer is otherwise vertical-free. Returns ``{}`` (graceful, never raises) when
    the run/session/vertical can't be resolved; consumers fall back to defaults.
    """
    if context.session is None or context.run_id is None:
        return {}

    from sqlalchemy import select

    from dataraum.analysis.validation.config import load_all_validation_specs
    from dataraum.lifecycle.db_models import LifecycleArtifact

    artifact = (
        context.session.execute(
            select(LifecycleArtifact)
            .where(
                LifecycleArtifact.artifact_type == "validation",
                LifecycleArtifact.run_id == context.run_id,
            )
            .limit(1)
        )
        .scalars()
        .first()
    )
    vertical = (artifact.teaches or {}).get("vertical") if artifact else None
    return load_all_validation_specs(vertical, context.session) if vertical else {}


class CrossTableConsistencyDetector(EntropyDetector):
    """Detect entropy from cross-table validation failures.

    Table-scoped detector that scores validation check results. Produces one
    EntropyObject per table PLUS one per COLUMN a failing check touched
    (DAT-432) — both pooled, provenance-weighted evidence_mass over the
    checks each grain covers (DAT-865b; ``readiness_context.py`` rolls a
    ``table:`` target up identically to a ``column:`` one, so both grains
    must resist a lone unvetted check the same way).
    """

    detector_id = "cross_table_consistency"
    layer = Layer.COMPUTATIONAL
    dimension = Dimension.RECONCILIATION
    sub_dimension = SubDimension.CROSS_TABLE_CONSISTENCY
    scope = "table"
    required_analyses = [AnalysisKey.VALIDATION]
    description = "Cross-table reconciliation failures from validation checks"

    def load_data(self, context: DetectorContext) -> None:
        """Load validation results that involve this table + calibrated reliabilities."""
        if context.session is None or not context.table_id:
            return

        from dataraum.analysis.validation.db_models import ValidationResultRecord
        from dataraum.entropy.reliabilities import get_reliability_config

        # ValidationResultRecord.table_ids is a JSON list of table_ids involved.
        # We need results where our table_id appears in that list.
        # SQLAlchemy JSON containment varies by backend; load all and filter.
        # Run-versioned since DAT-438: on the detect path scope to THIS run's
        # rows (the DetectorContext.run_id contract); ``None`` (test/legacy
        # callers outside the workflow) adds no filter.
        stmt = select(ValidationResultRecord)
        if context.run_id is not None:
            stmt = stmt.where(ValidationResultRecord.run_id == context.run_id)
        all_results = list(context.session.execute(stmt).scalars().all())

        matching = [r for r in all_results if context.table_id in (r.table_ids or [])]

        if matching:
            context.analysis_results["validation"] = matching

        # Calibrated witness reliabilities (DAT-450/865b); empty → the
        # detector's neutral DEFAULT_RELIABILITIES fallback.
        context.analysis_results["reliabilities"] = get_reliability_config().for_measurement(
            self.detector_id
        )

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Score validation results for this table.

        Both the TABLE-scoped object and the COLUMN fan-out objects (below)
        pool their failing checks as provenance-weighted witnesses (DAT-865b):
        the table object pools EVERY failing witness (one shared "is this
        table's cross-table reconciliation broken" claim); each column object
        pools the subset whose check named that column. Same witnesses, two
        groupings — see the module docstring for why the table grain can't be
        left on the raw worst-score.
        """
        results: list[Any] = context.get_analysis("validation", [])
        if not results:
            return [
                self.create_entropy_object(
                    context=context,
                    score=0.0,
                    evidence=[{"reason": "no_validation_results"}],
                )
            ]

        # The verdict's tolerance + the critical-rule severity are declared config
        # (ADR-0017), read from the spec — never stored on the record. The run's
        # vertical comes from a validation lifecycle artifact via the shared session.
        specs = _load_run_specs(context)
        reliabilities: Mapping[str, float] = context.get_analysis("reliabilities", None) or {}

        evidence: list[dict[str, Any]] = []
        table_witnesses: list[_TieredWitness] = []
        # Per column: the check-level evidence entries + the witnesses pooled
        # from them (DAT-865b) — replaces the old (worst_score, entries) pair.
        per_column: dict[str, tuple[list[dict[str, Any]], list[_TieredWitness]]] = {}

        for result in results:
            spec = specs.get(result.validation_id)
            tolerance = (
                spec.tolerance
                if spec is not None and spec.tolerance is not None
                else DEFAULT_TOLERANCE
            )
            severity = spec.severity.value if spec is not None else "info"
            # Provenance (DAT-735, no new plumbing — already on the spec the
            # detector reads): 'seed' (shipped, human-reviewed) vs 'generated'
            # (agentic induction's unvetted proposal). Unknown → the lower tier.
            source = spec.source if spec is not None else _UNKNOWN_SOURCE_FALLBACK

            # Recompute the verdict on demand (ADR-0017): re-run the run-versioned
            # ``sql_used`` against current data rather than read a stored pass/fail
            # that goes stale on re-import. check_type isn't needed (the score is
            # uniform deviation/magnitude); the message uses a generic label.
            verdict = verdict_from_sql(context.duckdb_conn, result.sql_used, tolerance=tolerance)
            score = _score(verdict, severity)
            if verdict.status == ValidationStatus.ERROR:
                logger.warning(
                    "validation_unassessed",
                    validation_id=result.validation_id,
                    table=context.table_name,
                )
            entry = {
                "validation_id": result.validation_id,
                "status": verdict.status.value,
                "severity": severity,
                "source": source,
                "passed": verdict.passed,
                "score": score,
                "message": verdict.message,
            }
            evidence.append(entry)
            if score > 0.0:
                witness = _check_witness(result.validation_id, source, score, reliabilities)
                table_witnesses.append(witness)
                for col_name in self._own_columns_used(context, result):
                    entries, witnesses = per_column.get(col_name, ([], []))
                    entries.append(dict(entry))
                    witnesses.append(witness)
                    per_column[col_name] = (entries, witnesses)

        # Capped evidence mass (DAT-865b + DAT-871) — the same lever as the
        # column fan-out: a lone unvetted (generated) check can't saturate the
        # table's score, seed failures still escalate additively, and
        # corroborating GENERATED failures cap at their strongest single
        # witness rather than summing (see ``_capped_evidence_mass``). No
        # ``pool()`` call needed at this grain — nothing here reads
        # conflict/ignorance. Clamp: the mass itself is unbounded above.
        final_score = min(1.0, _capped_evidence_mass(table_witnesses))

        table_object = self.create_entropy_object(
            context=context,
            score=final_score,
            evidence=evidence,
        )
        table_object.witnesses = _witness_claims(table_witnesses)

        objects = [table_object]
        objects.extend(self._column_objects(context, per_column))
        return objects

    @staticmethod
    def _own_columns_used(context: DetectorContext, result: Any) -> list[str]:
        """The failing check's ``columns_used`` entries that name THIS table.

        Entries are LLM-declared ``"table.column"`` strings. Table names are
        workspace-unique and narrow (DAT-639 — no ``src_<digest>__`` prefix), so a
        single exact match is correct and unambiguous: there is exactly one table
        of a given name in the workspace, so this can't cross-claim another
        source's same-named table.

        Deduplicated (DAT-865b, senior review): nothing upstream guarantees a
        check's declared ``columns_used`` names a column at most once — the
        pre-pooling code took ``max(worst, score)`` per occurrence (idempotent
        under a repeat), but pooling SUMS a witness's reliability contribution
        into ``evidence_mass``, so a repeated name would double-count one
        check as two independent witnesses (defeating "a lone unvetted check
        never blocks alone") and collide on ``ClaimWitnessRecord``'s
        ``(target, claim_field, witness_id, run_id)`` unique constraint at
        persistence. One witness per check per column, always.
        """
        table_name = context.table_name or ""
        out: list[str] = []
        seen: set[str] = set()
        for ref in getattr(result, "columns_used", None) or []:
            table_part, _, column_part = ref.partition(".")
            if column_part and table_part == table_name and column_part not in seen:
                seen.add(column_part)
                out.append(column_part)
        return out

    def _column_objects(
        self,
        context: DetectorContext,
        per_column: dict[str, tuple[list[dict[str, Any]], list[_TieredWitness]]],
    ) -> list[EntropyObject]:
        """Column-grain objects for the columns failing checks touched.

        The band must reach the columns deliverable metrics flow through —
        readiness is read per-column far more often than per-table (DAT-432).
        ``column_id`` rides in evidence so the engine anchors the record;
        names the LLM declared but the table doesn't have are dropped
        (hallucination guard).

        The score is the column's CAPPED evidence mass (DAT-865b + DAT-871,
        ``_capped_evidence_mass``), not the raw worst score: a lone unvetted
        (generated) check can no longer single-handedly saturate a column's
        score, a lone vetted (seed) critical failure still does, seed
        witnesses corroborate additively, and corroborating GENERATED
        witnesses cap at their strongest single one rather than summing —
        see ``_check_witness`` / ``_capped_evidence_mass``. ``pool()`` is
        still called (unmodified) for ``conflict``/``ignorance`` — informative
        provenance fields nothing in the loss path reads for this detector —
        but its ``evidence_mass`` is discarded in favor of the capped one, so
        the persisted ``pool_evidence_mass`` stays honest about what actually
        produced the score.
        """
        if context.session is None or not context.table_id or not per_column:
            return []

        from dataraum.storage import Column as ColumnModel

        col_ids = {
            col.column_name: col.column_id
            for col in context.session.execute(
                select(ColumnModel).where(ColumnModel.table_id == context.table_id)
            ).scalars()
        }
        objects: list[EntropyObject] = []
        for col_name, (entries, witnesses) in sorted(per_column.items()):
            column_id = col_ids.get(col_name)
            if column_id is None:
                logger.warning(
                    "validation_column_unknown", table=context.table_name, column=col_name
                )
                continue
            # conflict/ignorance only (DAT-871): the score comes from the
            # capped mass below, not this PoolResult's own evidence_mass.
            result: PoolResult = pool([tw.witness for tw in witnesses])
            capped_mass = _capped_evidence_mass(witnesses)
            # Clamp: the capped mass is unbounded above (seed contributions
            # still sum), but EntropyObject.score is a [0, 1] risk measure.
            score = min(1.0, capped_mass)
            objects.append(
                EntropyObject(
                    layer=self.layer,
                    dimension=self.dimension,
                    sub_dimension=self.sub_dimension,
                    target=f"column:{context.table_name}.{col_name}",
                    score=score,
                    evidence=[
                        {
                            **entry,
                            # BOTH ids: the engine's _extract_column_id anchors a
                            # record only when an entry carries column_id AND
                            # table_id (review wave-1: without table_id every
                            # fan-out row persisted with column_id=NULL and the
                            # cockpit's per-column evidence reads missed them).
                            "column_id": column_id,
                            "table_id": context.table_id,
                            "_table_name": context.table_name,
                            "_column_name": col_name,
                            # Pooled provenance (DAT-865b/DAT-871): one pool per
                            # column, so the same numbers ride every entry it
                            # covers. ``pool_evidence_mass`` is the CAPPED mass
                            # (what actually produced ``score``), not pool()'s
                            # raw uncapped sum — persisted evidence must not
                            # claim more mass than the score was computed from.
                            "pool_evidence_mass": capped_mass,
                            "pool_ignorance": result.ignorance,
                            "pool_conflict": result.conflict,
                        }
                        for entry in entries
                    ],
                    witnesses=_witness_claims(witnesses),
                    detector_id=self.detector_id,
                )
            )
        return objects
