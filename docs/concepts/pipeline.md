# The pipeline & phases

Under the journey's analysis stages is a pipeline of focused **phases** — roughly twenty of
them — run by the engine. Each phase does one job with the right [method](approach.md), and
after each, the relevant [detectors](measurement.md) update the entropy scores. This page is
the map of those phases.

The phases are grouped into the three engine analysis stages. How they are *scheduled* (as
Temporal workflows and activities) is the
[platform architecture](../platform/architecture.md)'s concern; here we care about what
each one does.

## Two ground rules

**VARCHAR-first staging.** Everything loads as text into a `raw` table — no parsing at the
door. Type inference happens later, as its own phase. A value that can't be cast to its
inferred type goes to a **quarantine** table; it is never silently dropped and never fails
the run. This is what lets messy real-world sources in without a brittle import step.

**Run-versioned, promoted at the end.** Every phase *appends* run-stamped rows rather than
overwriting. A terminal **promote** step flips a per-stage pointer to the new run only once
the run has succeeded, so a failed or partial run never becomes visible. "The current state"
is always a clean, complete run. (The mechanics — heads, promoted read views, idempotent
writers — are in [ADR-0008](../adr/0008-promoted-read-views.md) and
[ADR-0010](../adr/0010-failure-contract-idempotent-writers.md).)

## add_source — raw sources to typed, annotated data

Import runs once per source; the rest run **per table**, then a reduce step annotates
columns and a terminal step measures.

| Phase | Method | Does |
|---|---|---|
| `import` | deterministic | Load a source into a `raw` (VARCHAR) table |
| `typing` | deterministic | Infer column types; build `typed` + `quarantine` tables |
| `statistics` | statistical | Profile each column — counts, nulls, cardinality, distributions |
| `column_eligibility` | deterministic | Score which columns are worth analyzing |
| `statistical_quality` | statistical | Outliers (IQR, isolation forest), Benford's law |
| `temporal` | statistical | Granularity and gap analysis for time columns |
| `semantic_per_column` | **LLM** | Annotate each column — role, entity, term, concept binding |
| *detect* | — | Run the column-grain detectors; promote |

## begin_session — typed tables to an analytical workspace

| Phase | Method | Does |
|---|---|---|
| `begin_session_select` | deterministic | Validate the chosen table scope for the session |
| `session_materialize_overlays` | deterministic | Fold durable relationship teaches into the run |
| `relationships` | deterministic | Value-overlap, cardinality, and join detection |
| `semantic_per_table` | **LLM** | Classify tables (fact/dimension, grain), confirm relationships, author catalogue-grain column semantics |
| `surrogate_mint` | deterministic | Fuse confirmed composite keys into hash key columns; record the single-column join |
| `enriched_views` | **LLM** | Score and build grain-preserving join views |
| `slicing` | **LLM** | Identify the categorical dimensions you can slice by |
| `aggregation_lineage` | deterministic | Reconcile measures across facts (sum-consistency) |
| `dimension_hierarchies` | deterministic | Drill-down hierarchies and alias discovery over views |
| `drivers` | deterministic | Rank the drivers behind each measure |
| *detect → keepers → promote* | — | Relationship-grain readiness; lift accepted results; promote |

## operating_model — intent to executed model

| Phase | Method | Does |
|---|---|---|
| `operating_model_resolve` | deterministic | Pin the base-run map and table set once for the run |
| `validation` | **LLM** | Generate and execute the declared rules; move them through their lifecycle |
| `business_cycles` | **LLM** | Detect and bind multi-table processes; execute |
| `metrics` | **LLM** | Compose declared measures as calculation graphs; execute |

Each artifact in this stage is tracked through **declared → grounded → executed** — see
[the learnable surface](learnable-surface.md).

## Where the LLM is, and isn't

Of the phases above, only the marked ones call the LLM, and each is **scoped narrowly** — per
column, per table, per declared artifact — never "here is everything, figure it out." The
bulk of the runtime is deterministic and statistical work the model never touches. That
scoping is both a cost choice (small, cheap calls) and a trust choice: the LLM is asked
focused questions whose answers can be checked against the data by a detector.

## After every phase: detectors

A phase doesn't just write metadata — it triggers the detectors whose inputs it just
produced, which re-measure entropy for the affected columns, relationships, tables, or views.
By the time a stage finishes, its readiness signal is already current. That feedback loop —
measure, surface *why*, teach, re-run — is the subject of [measurement & detectors](measurement.md).
