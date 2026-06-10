# LANE L2 — relationship pool (lane/relationship-pool)

Two commits: the Part A root-cause fix (teach-shadowing silent no-fire) and the
Part B measurement pack (`relationship_discovery`). All work inside
`packages/engine` + `packages/dataraum-config`; no shared module was modified
beyond the registration line in `detectors/base.py`.

## Part A — root cause (what the bug actually is)

The REPORTED DAT-408-era bug (`load_data` storing `analysis_results["relationship"]`
singular vs the `AnalysisKey.RELATIONSHIPS` gate) is **already fixed on this
branch** (`relationship_entropy.py:69` stores under the declared key; verified by
driving the full post-step wiring on SQLite — an llm row with RI evidence fires
at the honest orphan rate 0.20).

The bug that is REAL today is the same *data-availability* class, one layer down:

- `analysis/relationships/materialize.py:103` writes teach-materialized rows
  (`manual` from an `add` overlay, `keeper` from a `keep` overlay) with
  `evidence={"source": "config_overlay", "action": ...}` — **no RI metrics**.
- `entropy/detectors/loaders.py` (`load_relationship_for_pair`) picks the pair's
  representative by precedence `manual > keeper > llm > candidate`, so the
  overlay row **shadows** the measured candidate/llm rows.
- `relationship_entropy.detect()` (relationship_entropy.py:95-103) finds no
  `left_referential_integrity` / `orphan_count` on the representative and
  returns `[]` — silently. The orphan-rate measurement disappears **exactly when
  the user teaches the relationship** (recall = 0 on every taught pair, forever,
  no error anywhere).

Fix (minimal, in the detectors' own loader): the representative keeps its
identity (method, confidence, confirmation); the measured RI evidence keys are
backfilled from the highest-precedence row of the pair that carries them, with
`ri_evidence_source` recording the donor. No measured row anywhere → unchanged
honest silence. Reproduced + pinned by
`tests/unit/entropy/test_relationship_pair_evidence.py` (drives the real
engine→snapshot→loader→detector wiring on SQLite).

## Part B — verdict: BUILT (`relationship_discovery`)

Entry criterion verified from code: the structural `candidate` rows DO carry a
usable value-overlap statistic — `evidence.join_confidence` =
max(Jaccard, containment) over actual values plus `statistical_confidence`
(`analysis/relationships/joins.py`, persisted by `detector._store_candidates`),
alongside evaluated RI metrics. Grounding probe (real pooling engine, observed
live values from the DAT-405 runs): pooled conflict separates by a margin —
injected-confirmed (jc 0.59, llm 0.9) C=0.147 vs clean-confirmed C=0.013
(+0.134); keeper-over-weak-overlap C=0.333 vs clean keeper C=0.000 (+0.333).
Pinned as ordering tests.

The 7 pieces:

1. **Claim space** — `("genuine", "spurious")`,
   `entropy/measurements/relationship_discovery.py`.
2. **Witness extractors** — pure, abstain-to-uniform, all reading EXISTING rows
   on the focal directional pair: `value_overlap` (the data witness — the
   candidate row's jc damped by its own statistical_confidence; reads values,
   never names), `llm_judgment` (llm row confidence), `manual_curation` (manual
   row), `keeper_retention` (keeper row). The `_leaning` mechanism is the
   temporal_behavior convention; no tunable constants — all inputs are measured
   row values.
3. **Detector shell** — `entropy/detectors/structural/relationship_discovery.py`,
   relationship target grain (mirrors relationship_entropy), gate key == stored
   key (`AnalysisKey.RELATIONSHIPS`), score = pooled conflict C, witnesses
   attached as `WitnessClaim`s. Registered in `detectors/base.py` (my line) and
   declared on `semantic_per_table` in `dataraum-config/pipeline.yaml`. New
   loader `load_relationship_rows_for_pair` (per-method rows) in
   `entropy/detectors/loaders.py`. New enum member
   `SubDimension.RELATIONSHIP_DISCOVERY` in `entropy/dimensions.py`.
4. **Config rows** — `dataraum-config/entropy/loss.yaml` (`relationship_discovery`
   conflict/ignorance weights per intent) and
   `dataraum-config/entropy/reliabilities.yaml` (4 witness r's). Both explicitly
   marked UNCALIBRATED placeholder priors in comments; the file-level provenance
   block says `calibrated: true` for null_semantics — the per-measurement
   comment is the calibrated:false marker (same precedent as
   structural_reconciliation). The rig needs a generative relationship family
   (clean FK / orphan-broken FK / spurious-overlap pair) to measure them.
5. **Resolve write-back** — NOT built: **no consumer field exists** for the
   genuineness verdict. `Relationship.is_confirmed` is the human flag, not a
   resolve target; `SemanticAnnotation` is column-grain. Candidate consumer
   surface: a contested/genuineness field on the Relationship row (or the
   cockpit reading the entropy object + claim_witnesses directly).
6. **Teach applier** — none NEW needed: the existing relationship overlay family
   (`add`/`keep`/`reject`) is the teach surface, and it already re-enters the
   measurement as witnesses (manual/keeper rows are re-materialized every run).
   Candidate teach extension for the integrator: treat an explicit `confirm`
   overlay as the human witness too — today confirm only flips `is_confirmed`
   evidence and join-path resolution, it never materializes a row, so a
   confirm-taught pair's human witness is silent.
7. **Eval row** — `calibration/detector_coverage.yaml` lives in the EVAL repo
   (not this worktree). Proposed entry: flip `relationship_entropy`'s
   `entry_criterion` note to point at `relationship_discovery` as the built pool
   (disposition: candidate → pooled), witnesses as above, recall fixture =
   detection-v1's `break_referential_integrity` asserted on C-ordering
   (injected-confirmed > clean-confirmed + margin), precision = clean catalog
   quiet, plus a reliability-rig block once the generative relationship family
   exists (`/evolve-testdata`).

## Shared-change requests (integrator)

1. **`entropy/engine.py` relationship scope drops witness provenance.** Only the
   column scope collects `_make_witness_records` (engine.py:131-139); the
   relationship branch persists the EntropyObjectRecord but silently discards
   `obj.witnesses` — `relationship_discovery`'s `WitnessClaim`s never reach
   `claim_witnesses`. Fix is mechanical: collect witness records in the
   relationship branch exactly like the column branch (anchor table_id/column_id
   to the from-endpoint, as the object record does). engine.py is lane-off-limits,
   so this is yours.
2. **`entropy/snapshot.py` `_REL_METHOD_PRECEDENCE` lacks `keeper`** (manual 3 /
   llm 2 / candidate 1 — snapshot.py:130) while `loaders.py` ranks manual 4 /
   keeper 3 / llm 2 / candidate 1. Not load-bearing today (the resolved fields
   are identical across a pair's rows), but it is the same DAT-408-era
   inconsistency family — worth aligning when you touch snapshot.py.

## Known gap (design-level, recorded not patched)

A candidate the LLM **declined** never becomes a focal pair: engine.py
enumerates `detection_method != 'candidate'` pairs only (the DAT-405
defined-catalog contract). So the disagreement "data says strong overlap, the
selector said no" is structurally unmeasurable at the relationship grain — the
pool only sees declines on pairs that are in the catalog for another reason
(keeper/manual), where it carries `llm_confirmed_this_run: false` in evidence
and the llm witness ABSTAINS (a decline's strength is uncalibrated; asserting
one-hot spurious without rig data would be an invented constant). If the
program wants the pure-decline claim measured, that is a focal-pair enumeration
change (engine.py, shared) + a rig calibration of the decline witness — not a
lane-local edit.

## Adjacent issue verified — join_path_determinism

Confirmed FIXED from code, no change made: `relations.py:43-45` loads via
`load_session_relationships`, which filters `detection_method != 'candidate'`
(loaders.py), and engine.py's focal-pair enumeration does the same — bare
candidates are never counted as ambiguity (the PR #207 fix is intact on this
branch). Remaining known gap is eval-side and unchanged: no fixture exercises
genuine two-distinct-FK ambiguity (DAT-419).
