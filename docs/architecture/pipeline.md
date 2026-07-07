# Pipeline

How data becomes analyzable metadata: frame → add_source → begin_session →
operating model. The engine runs the pipeline as Temporal workflows
(`packages/engine/src/dataraum/worker/workflows.py`); the cockpit triggers them
and reads the results from the workspace schema.

## Grounding has one input: the frame's concepts

- The cockpit's frame stage writes `concept` rows into the workspace config
  overlay **before** `add_source` runs; the engine materializes them onto the
  vertical's ontology (upsert by name) and grounds columns against the result.
  The engine performs grounding only — no concept induction lives in it.
- Semantic grounding against zero concepts fails loud, naming the missing frame
  step; an unknown vertical fails loud too. There is no silent empty grounding.
- The concept payload is a cross-package contract: the engine's ontology-concept
  model and the cockpit's frame output change together.

## Sources are dumb; content is identity

- One uploaded file = one **content-keyed** source (`src_<digest>`) carrying
  exactly one staged object. One DB connection = one **name-keyed** source
  carrying its synthesized recipe and a `recipe_hash`. The cockpit `select` tool
  is the only producer of both hashes; the engine treats them as opaque tokens
  and never recomputes one.
- Same bytes → same source → import skips on presence (changed bytes mint a new
  digest, hence a new source and a fresh import). A DB source skips only while
  `recipe_hash` matches the `imported_recipe_hash` witness stamped at import: an
  identical re-select is an intentional no-op; a changed recipe triggers an
  in-place re-import-with-replace (full teardown of that source's tables and
  derived metadata) — never a silent stale read.
- Raw tables get **narrow, workspace-unique names** (no source prefix — the
  source is a provenance wrapper, not a namespace) and carry no `run_id`: the
  content hash is the raw version axis. A cross-source name collision fails loud.
- No relatedness is inferred at upload; it emerges at session composition.

## A run ingests a set; source identity dies at import

- `addSourceWorkflow` ingests 1–N sources: the import phase runs once per
  source, then everything downstream is source-free and session-scoped. Stages
  past import address typed `table_id`s, never a `source_id` — do not bind a
  post-import stage to a source.
- Staging is **VARCHAR-first**: every object loads as VARCHAR; typing infers
  types and mints typed tables, routing failed casts to quarantine tables. A
  failed cast is never a pipeline failure.
- One child workflow per raw table (typing → per-table analytics); detectors run
  once in the parent's terminal `detect` step, and the terminal promote flips
  the snapshot head to the run.

## The session is the only unit the user names

- `begin_session` composes a user-selected set of typed tables — across any
  sources and runs — into an analytical session: a sequential cross-table chain
  (relationships → LLM confirmation → teach overlays → surrogate mint →
  enriched views → value layer → detect → promote), no fan-out. Discovering
  that combined data holds nothing is the product working.
- begin_session **evaluates; consumers materialize**: its durable output is the
  dimension catalog (`analysis/slicing/db_models.py` — per-(table, column)
  dimension declarations plus FD/hierarchy edges); aggregations are composed on
  demand as SQL. One view model exists — the enriched view; there is no
  pre-summed aggregation-view substrate.
- A deterministic g3 functional-dependency pass over the enriched view
  (`analysis/hierarchies/`) yields drill-down hierarchies and 1:1 alias
  collapse — no LLM; a teach can assert or suppress a structure.
- The stock/flow `structural_reconciliation` witness aggregates **inline** —
  per-(dimension, period) sums queried over the enriched view, never from
  materialized slice tables.

## The enriched-view shape is decided once and inherited

- The enrichment LLM judges only column pairs not yet judged (and is skipped
  when there are none). The verdict persists keyed by the
  `(from_column_id, to_column_id)` pair, and the shape is **monotonic**: columns
  are added by newly confirmed relationships and removed only by an explicit
  reject or teach — never flipped by a fresh re-judgment. A judged pair is
  re-offered only when the evidence dossier its verdict was made on changes.

## The catalog speaks single-column; composite keys are cured at the source

- Every defined relationship is one column pair. No multi-column join machinery
  exists anywhere — do not add any.
- Detection is evidence, the LLM is the judge: a greedy pre-pass
  (`analysis/relationships/composite.py`) probes each many-to-many candidate —
  anchor on the strongest pair, fuse the co-present pair that most reduces join
  multiplicity — and accepts only when the composite's measured cardinality
  collapses out of many-to-many, else abstains. A hit rides the candidate feed
  as a hint; nothing is auto-created from statistics.
- A confirmed composite persists as a `surrogate_key_intents` row (run-versioned,
  keyed by a direction-neutral digest of its component pairs), and the
  `surrogate_mint` phase cures it: both typed tables are re-materialized by
  wrapping the typing recipe's DDL with one deterministic hash column —
  `md5` over the components cast to VARCHAR, `|`-delimited, **NULL-propagating**
  (any NULL component yields a NULL surrogate, so a LEFT JOIN misses; NULL never
  joins NULL). See `analysis/relationships/surrogate.py`.
- The column name (`_sk__<components>`) is deterministic in the component set
  and the column row upserts by `(table_id, column_name)`, so `column_id` — and
  every overlay keyed on it — is stable across runs. ONE ordinary relationship
  persists on the surrogate pair, FK-side-first by measured cardinality. The
  mint owns the `_sk__*` namespace: a surrogate neither re-confirmed nor still
  referenced is reconciled away, physical and metadata.
- The mint abstains on divergent component types, float components, a composite
  whose cardinality does not collapse, a vanished component, or a missing typing
  recipe: the worst case is "no column minted", never a wrong join.

## Everything derived is run-versioned

- Every workflow execution mints a `run_id`; derived metadata coexists across
  runs and the terminal promote names the current run. A teach re-run is a full
  re-run under a fresh `run_id` — there is no partial replay scope.
- `relationship_id` is a per-run uuid — never key cross-run state on it; the
  cross-run-stable identity is the `(from_column_id, to_column_id)` pair.
