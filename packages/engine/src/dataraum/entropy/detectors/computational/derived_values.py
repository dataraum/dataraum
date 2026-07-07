"""Derived value detector — formula mismatch rate + the formula adjudication.

Two signals per derived column (docs/architecture/entropy.md, derived-value second witness):

* ``obj.score`` is the WORSE of two honest statistics: the formula-mismatch rate
  ``1 − match_rate`` of the best GRADED formula, and the pooled name-vs-data
  identity conflict on a hygiene-passing claim (wholesale divergence: the data
  follows B perfectly, so the mismatch leg is 0.0, while the name advertises A —
  the disagreement IS the entropy). Both legs share the hypothesis hygiene gate.
  The mismatch leg is grounded because grading is data:
  the rate is measured over the actual rows; the LLM only chose which identity
  to test (DAT-442 reset: no boost; a 10% mismatch scores 0.10). The eval
  asserts this as the ordering "injected separates from clean"; severity per
  intent lives in the loss table.
* The POOLED second axis (``entropy/measurements/derived_value.py``): per
  canonical formula identity, the data witness (discovered/graded match rate)
  vs the LLM's name-based formula hypothesis (``derived_formula_hypothesis``
  from ``semantic_per_column``). The pooled ``formula_conflict`` /
  ``formula_ignorance`` ride in evidence — loss.yaml scores them as secondary
  signals — and the witness distributions persist via ``obj.witnesses``.

A column with neither a discovered formula nor a graded hypothesis emits
nothing (absence of a formula is ignorance, not a 100%-broken column). An
UNGRADED hypothesis never moves the scalar — the hallucination guard abstains
when its source columns don't resolve, so an LLM guess alone cannot flag a
column (no deterministic override, no unilateral LLM claim).

Teach routing (DAT-447, Option B): every emitted evidence entry carries a
``validation`` teach suggestion — the user resolves a contested formula by
DECLARING the expected one (an expected formula IS a check, executed every run
by the validation phase). The declaration comes back through
``load_declared_formula`` as the ``human_declaration`` witness on the declared
formula's claim, row-graded like any hypothesis: corroborated → the claim's
conflict collapses; violated → the human is contested, never obeyed.

Match-quality status thresholds remain in config/entropy/thresholds.yaml.
"""

from __future__ import annotations

from typing import Any

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import Dimension, Layer, SubDimension
from dataraum.entropy.measurements.derived_value import (
    CLAIM_SPACE,
    canonicalize_discovered,
    measure_derived_value,
    parse_formula,
)
from dataraum.entropy.models import EntropyObject, WitnessClaim


def _derived_entries(correlation: Any, column_name: str) -> list[dict[str, Any]]:
    """This column's discovered-formula entries from the correlation analysis."""
    if hasattr(correlation, "derived_columns"):
        raw = correlation.derived_columns or []
    elif isinstance(correlation, dict):
        raw = correlation.get("derived_columns", [])
    else:
        raw = []
    entries: list[dict[str, Any]] = []
    for dc in raw:
        if hasattr(dc, "derived_column_name"):
            if dc.derived_column_name != column_name:
                continue
            entries.append(
                {
                    "formula": getattr(dc, "formula", None),
                    "match_rate": getattr(dc, "match_rate", None),
                    "derivation_type": getattr(dc, "derivation_type", None),
                    "source_column_names": getattr(dc, "source_column_names", []) or [],
                }
            )
        elif isinstance(dc, dict):
            if dc.get("derived_column_name") != column_name:
                continue
            entries.append(
                {
                    "formula": dc.get("formula"),
                    "match_rate": dc.get("match_rate"),
                    "derivation_type": dc.get("derivation_type"),
                    "source_column_names": dc.get("source_column_names", []) or [],
                }
            )
    return entries


class DerivedValueDetector(EntropyDetector):
    """Formula mismatch rate, pooled with the LLM-hypothesis + declaration witnesses."""

    detector_id = "derived_value"
    layer = Layer.COMPUTATIONAL
    dimension = Dimension.DERIVED_VALUES
    sub_dimension = SubDimension.FORMULA_MATCH
    # No required_analyses: either witness path may be absent — load_data reads
    # what exists (correlation rows are session-run-written; the semantic
    # hypothesis is add_source-written) and detect() measures what it got.
    required_analyses = []
    description = (
        "Measures reliability of derived column formulas (data vs LLM hypothesis vs declaration)"
    )

    def load_data(self, context: DetectorContext) -> None:
        """Load discovered formulas, the LLM hypothesis, the declaration + grading.

        A NOVEL hypothesis (parsed, not self-referential, not matching any
        discovered formula) is row-graded over the typed table with the same
        statistic the discovery uses — "the data grounds the LLM hypothesis".
        A user-declared expected formula (the ``validation`` teach, DAT-447)
        loads alongside and a novel declared formula is row-graded the same
        way, so a declaration the data violates is honestly contested rather
        than silently trusted.
        """
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import (
            load_correlation,
            load_declared_formula,
            load_hypothesis_match_rate,
            load_semantic,
        )
        from dataraum.entropy.reliabilities import get_reliability_config

        correlation = load_correlation(
            context.session, context.column_id, context.column_name, run_id=context.run_id
        )
        if correlation is not None:
            context.analysis_results["correlation"] = correlation
        semantic = load_semantic(
            context.session, context.column_id, context.run_id, context.base_runs
        )
        if semantic is not None:
            context.analysis_results["semantic"] = semantic
        context.analysis_results["reliabilities"] = get_reliability_config().for_measurement(
            self.detector_id
        )
        declaration = load_declared_formula(
            context.session, context.table_name, context.column_name
        )
        if declaration is not None:
            context.analysis_results["declaration"] = declaration

        discovered_identities = {
            c.identity
            for entry in _derived_entries(correlation or {}, context.column_name)
            if (c := canonicalize_discovered(entry)) is not None
        }
        focal = context.column_name.strip().lower()

        hyp = parse_formula((semantic or {}).get("derived_formula_hypothesis"))
        # A discovered formula is already graded by its own match rate; only a
        # NOVEL, non-self-referential hypothesis gets the loader's row grading.
        if (
            hyp is not None
            and focal not in hyp.operands
            and hyp.identity not in discovered_identities
        ):
            grading = load_hypothesis_match_rate(
                context.session,
                context.column_id,
                context.duckdb_conn,
                hyp.operands,
                hyp.operation,
            )
            if grading is not None:
                context.analysis_results["hypothesis_grading"] = grading

        decl = parse_formula((declaration or {}).get("formula"))
        if (
            declaration is not None
            and decl is not None
            and focal not in decl.operands
            and decl.identity not in discovered_identities
        ):
            hypothesis_grading = context.analysis_results.get("hypothesis_grading")
            if hyp is not None and decl.identity == hyp.identity and hypothesis_grading:
                # Same canonical identity → the same row statistic; reuse it.
                declaration["match_rate"] = hypothesis_grading.get("match_rate")
            else:
                decl_grading = load_hypothesis_match_rate(
                    context.session,
                    context.column_id,
                    context.duckdb_conn,
                    decl.operands,
                    decl.operation,
                )
                if decl_grading is not None:
                    declaration["match_rate"] = decl_grading.get("match_rate")

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Adjudicate the column's formula claims; emit one witnessed object.

        Args:
            context: Detector context with correlation/semantic analysis results.

        Returns:
            List with a single EntropyObject, or empty when no formula claim
            exists for this column.
        """
        correlation = context.get_analysis("correlation", {})
        semantic = context.get_analysis("semantic", {}) or {}
        reliabilities = context.get_analysis("reliabilities", None) or None
        grading = context.get_analysis("hypothesis_grading", {}) or {}
        declaration = context.get_analysis("declaration", None) or None

        discovered = _derived_entries(correlation, context.column_name)
        hypothesis = None
        if semantic.get("derived_formula_hypothesis"):
            hypothesis = {
                "formula": semantic["derived_formula_hypothesis"],
                "confidence": semantic.get("derived_formula_confidence"),
                "match_rate": grading.get("match_rate"),
            }

        adjudications = measure_derived_value(
            context.table_name,
            context.column_name,
            discovered,
            hypothesis,
            declaration=declaration,
            reliabilities=reliabilities,
        )
        if not adjudications:
            return []

        # Honest mismatch rate of the best GRADED formula — discovered or
        # hypothesis, because grading is DATA: the match rate is measured over
        # the actual rows; the LLM only chose WHICH identity to test (the same
        # division of labor as validation SQL). Hallucinations never get here —
        # unresolvable source columns abstain upstream. Without the hypothesis
        # leg, an injection that pushes the discovered formula below the
        # persistence cut scored 0.0 while 13% of rows measurably violated the
        # identity (the batch-1 recall miss on journal_lines.net_amount).
        detector_config = get_entropy_config().detector("derived_value")
        # Statistical hygiene on the hypothesis leg of the SCALAR (review
        # wave-1): a low-confidence guess ("guessing among several plausible
        # formulas") or a handful of gradable rows must not band a clean column
        # whose true derivation is simply richer than two terms. The pooled
        # axis keeps weighing every graded hypothesis — this gates the scalar.
        hyp_min_rows = int(detector_config.get("hypothesis_min_rows", 20))
        hyp_min_confidence = float(detector_config.get("hypothesis_min_confidence", 0.5))
        hypothesis_scalar_ok = (
            hypothesis is not None
            and int(grading.get("total") or 0) >= hyp_min_rows
            and float(hypothesis.get("confidence") or 0.0) >= hyp_min_confidence
        )
        # A DECLARED slot is first-class in both score legs (DAT-447): the user
        # deliberately chose the identity (no guessy-confidence hygiene to
        # apply) and its grading is data — a declared check the rows violate is
        # the quality finding, not silent trust in the human.
        graded_rates = [
            a.match_rate
            for a in adjudications
            if a.discovered or a.declared or hypothesis_scalar_ok
        ]
        best_rate = max((r for r in graded_rates if r is not None), default=None)
        scalar = max(0.0, min(1.0, 1.0 - best_rate)) if best_rate is not None else 0.0
        # The name-vs-data identity conflict joins the score (wave-2 cal corpus
        # finding): under WHOLESALE divergence every row follows formula B, so
        # the best graded formula matches perfectly and the scalar is 0.0 while
        # the NAMED claim carries the real entropy — the LLM hypothesis (what
        # the name advertises) leans holds, the row grading says fails, pooled
        # C ≈ 0.8. That disagreement was evidence-only and thus invisible to
        # the loss rollup — a silent false negative on 3/3 wholesale columns
        # (detection-derived-cal-v1). The conflict leg honours the same
        # hypothesis-hygiene gate as the scalar: a low-confidence guess or a
        # thin grading sample must not band a clean column through this door
        # either (review wave-1 blocker).
        # CLOSURE (DAT-447): once the user has DECLARED the expected formula,
        # the identity question has its human answer — the declared claim IS
        # the column's identity risk, so the conflict leg aggregates over the
        # declared slot(s) only. This is aggregation semantics, not an
        # override: every witness still votes on every claim (the name-vs-data
        # conflict on the hypothesis claim stays in evidence — it is a NAMING
        # finding once the formula is settled), and a VIOLATED declaration
        # bands harder, not softer (row grading fails vs the human's holds →
        # high pooled conflict on exactly the claim the human anchored).
        # Without this, a correct declaration left the column banded forever —
        # a teach that cannot close, breaking the eval contract's "stable".
        declared_slots = [a for a in adjudications if a.declared]
        conflict_pool = declared_slots or [
            a for a in adjudications if a.discovered or hypothesis_scalar_ok
        ]
        identity_conflict = max((a.result.conflict for a in conflict_pool), default=0.0)
        score = max(scalar, identity_conflict)

        # Match-quality labels (display only), configurable thresholds.
        match_exact = detector_config.get("match_exact", 0.99)
        match_near_exact = detector_config.get("match_near_exact", 0.95)
        match_approximate = detector_config.get("match_approximate", 0.80)

        def _status(rate: float | None) -> str | None:
            if rate is None:
                return None
            if rate >= match_exact:
                return "exact"
            if rate >= match_near_exact:
                return "near_exact"
            if rate >= match_approximate:
                return "approximate"
            return "poor"

        # Option B design call (Philipp, 2026-06-11): the user's "the expected
        # formula for this column IS X" declaration rides the EXISTING
        # `validation` teach rather than a new expected_formula family — the
        # applier is proven, and the declaration doubles as a continuously
        # executed check (the validation phase runs it every run). Caveat: this
        # conflates a quality check with a semantic declaration about a column —
        # revisit if concept-level formula declarations arrive in the ontology.
        # Always-emit, like temporal_behavior: no thresholds, no gating — the
        # consumer surfaces the suggestion per band. The payload names the
        # column and the check intent only; it picks NO truth (the user
        # declares; both candidate formulas already ride the per-claim
        # evidence).
        teach: dict[str, Any] = {
            "type": "validation",
            "check": "expected_formula",
            "table": context.table_name,
            "column": context.column_name,
        }
        evidence: list[dict[str, Any]] = []
        for adj in adjudications:
            entry: dict[str, Any] = {
                "claim_field": adj.claim_field,
                "formula": adj.formula_display,
                "formula_canonical": adj.formula,
                "discovered": adj.discovered,
                "hypothesized": adj.hypothesized,
                "declared": adj.declared,
                "match_rate": adj.match_rate,
                # Loss-readable secondary signals (loss.yaml scores the worst
                # value of each key across evidence; "conflict" would alias to
                # obj.score — the mismatch rate — hence the distinct names).
                "formula_conflict": adj.result.conflict,
                "formula_ignorance": adj.result.ignorance,
                "posterior": dict(zip(CLAIM_SPACE, adj.result.posterior, strict=False)),
                "teach_suggestion": teach,
            }
            status = _status(adj.match_rate) if adj.discovered else None
            if status is not None:
                entry["status"] = status
            evidence.append(entry)
        # Keep the legacy display keys on the first discovered entry's source.
        for entry, adj in zip(evidence, adjudications, strict=True):
            if adj.discovered:
                src = next(
                    (
                        e.get("source_column_names")
                        for e in discovered
                        if e.get("formula") == adj.formula_display
                    ),
                    None,
                )
                entry["source_columns"] = src or []
                dtype = next(
                    (
                        e.get("derivation_type")
                        for e in discovered
                        if e.get("formula") == adj.formula_display
                    ),
                    None,
                )
                if dtype is not None:
                    entry["derivation_type"] = dtype

        obj = self.create_entropy_object(context=context, score=score, evidence=evidence)
        obj.witnesses = [
            WitnessClaim(
                claim_field=adj.claim_field,
                witness_id=w.witness_id,
                distribution=dict(zip(CLAIM_SPACE, w.distribution, strict=True)),
                reliability=w.reliability,
            )
            for adj in adjudications
            for w in adj.witnesses
        ]
        return [obj]
