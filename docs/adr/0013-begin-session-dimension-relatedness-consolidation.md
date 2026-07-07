# ADR-0013 — begin_session consolidation: dimension catalog, inline witness aggregation, sticky enriched-view shape

- **Status:** Accepted (consolidated 2026-07-02 from the original decision and the amendments of 2026-06-17 and 2026-06-20; one listed cut is only half-executed — `dimensional_entropy` lost its loss row (`loss.yaml` DEMOTED) but the detector is still registered (`entropy/detectors/base.py`), and `grain_safe` was since removed as always-true — the deregistration is an open direct-fix item)
- **Date:** 2026-06-16
- **Ticket:** DAT-514 (epic), DAT-535 (spike), DAT-516 (shape stability), DAT-536/537/543/545
- **Design doc:** Confluence DD/36798466

## Context

DAT-514 was framed as a delete of a presumed-dead slice → temporal → lineage tail in
begin_session. Investigation falsified the premise: that substrate feeds the
`structural_reconciliation` witness of the stock/flow measurement — the only
data-grounded vote in that adjudication — whose resolved label drives the SQL agents'
end-of-period-vs-SUM choice. The work became a consolidation.

Two kill-gate evals settled the contested pieces:

- **`structural_reconciliation` — keep.** Non-redundant: corrects 2 of 5
  backed-but-ambiguously-named columns the name-based witnesses get wrong, with zero
  reverse flips and correct non-override on broken backing.
- **`dimensional_entropy` (NMI) — cut.** Anti-predictive on the loss path: highest on
  clean intrinsic structure (mutex/alias/FD with no wrong answer) and *lower* as
  corruption grows. Risk lives in violation of *expected* structure, which
  `derived_value` and `relationship_entropy` already own.

A separate defect (DAT-516): the enriched view's column set was re-judged by an LLM every
run. LLMs are not deterministic even at temperature 0, so re-running the same session
could silently change or erase columns downstream SQL depends on. The relationship layer
had silent-accept durability; the view-shape layer had none.

## Decision

1. **begin_session evaluates; consumers materialize.** begin_session produces the
   **dimension catalog**: per-(table, column) dimension declarations with `grain_safe`
   (free — every column on a grain-verified enriched view is safe to GROUP BY by
   construction) and FD/hierarchy edges. Materializing aggregations for reuse is the
   consumers' concern, composed on demand as SQL. One view model: `enriched_view` only —
   no pre-summed aggregation-view substrate.
2. **The `structural_reconciliation` witness stays and aggregates inline** — its
   per-(dimension-value, period) sums are computed in a query over the enriched view,
   not from materialized slice tables. Gate held: verdicts unchanged on the stock/flow
   eval corpus.
3. **A deterministic FD/hierarchy pass** runs over the enriched view:
   `g3(A→B) = 1 − |A| / |(A,B)|`, one SQL pass, no LLM. Directional edges give
   drill-down hierarchies and 1:1 alias collapse; false positives are guarded by
   min-support thresholds and teach confirmation.
4. **The enriched-view shape is decided once and inherited.** The enrichment LLM judges
   only relationships not yet judged, and is skipped when there are none. The verdict
   persists — all judged column pairs plus the exposed join specs — and the shape is
   **monotonic**: columns are added by newly confirmed relationships and removed only by
   an explicit reject or teach, never flipped by a fresh re-judgment. The sticky key is
   the `(from_column_id, to_column_id)` pair; `relationship_id` is a per-run uuid and
   cannot carry a verdict across runs.
5. **Cuts:** the per-value slice materialization (tables, phases, `sql_template`, the
   collision guard that existed only for it), the `dimensional_entropy` detector with its
   loss row and `expected_dependency` teach, and the never-consumed Pearson/Spearman
   correlations.

## Consequences

- Kept: the witness, `reconcile`, and the aggregation-lineage models; `derived_value`;
  cross-table relationships; enriched views. `SliceDefinition` is repurposed as the
  dimension catalog — a consumer sweep showed it load-bearing in cycle, validation, and
  graph context, well beyond slicing.
- Downstream SQL can rely on enriched-view columns not vanishing between runs; the
  sticky shape closes the cross-run half of stability (the in-run half was already the
  atomic CHECKPOINT).
- A hierarchy/alias teach is net-new. The deterministic GROUP-BY composer and the
  stock/flow "rescue" from the original framing were dropped.
- Eval caveat: both kill-gate results are small-n directional reads; the corpora should
  widen before the derived reliabilities are treated as final.
