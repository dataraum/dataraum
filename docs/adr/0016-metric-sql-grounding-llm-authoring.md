# ADR-0016 — Metric SQL: feed deterministic grounding, let the LLM author

- **Status:** Accepted
- **Date:** 2026-06-25
- **Ticket:** DAT-543 (epic), DAT-616/620/631 (the implementing arc)
- **Design doc:** Confluence DD space (DAT-543 epic)

## Context

The metric path produces *silently wrong* SQL on long/transactional data: where a concept
like `revenue` is a row filter (not a column), the graph agent improvises a predicate
(`WHERE account_type ILIKE '%cost%'`), matches the wrong rows, and returns a plausible
number that renders green. Two reflexes were tried and **both failed**:

- A **deterministic SQL builder** for metrics. The metric-SQL shape space (sum, ratio,
  end-of-period, window, multi-column, signed measures, CTEs) is open-ended; every fixed
  builder has died on the tail. (`build_enriched_view_sql` is deterministic *only* because
  its shape is fixed — one grain-preserving LEFT JOIN — which is not the metric case.)
- A **value-space verifier** (support>0, `value>0`, scalar judge). These are structurally
  blind to *wrong-but-non-empty* results — they certify the generator's own blind spot.

## Decision

**The LLM authors the metric SQL — that is the design, not a fallback.** What we make
deterministic is the **grounding evidence fed to the agent**, never the SQL it emits:
*deterministic evidence + LLM authoring + human teach.*

1. **Feed, don't starve.** Serve the grounding the engine already computes — `top_values` +
   `distinct_count` (complete value-set enumeration), driver `ranked_dimensions` /
   `interesting_slices` / `target_type`, ontology `indicators`/`exclude_patterns` — into
   *both* SQL authors (engine GraphAgent and cockpit answer agent). The agent fills a
   **named-context contract + a blueprint library** (a few-shot exemplar per shape class);
   shape coverage is *taught as blueprints*, not hoped for. It never invents a filter.
2. **A deterministic builder is legitimate only for fixed-shape SQL** (enriched_views) and
   nothing else. Do not build `build_extract_sql`.
3. **Grounding is the trust boundary**, not a verified LLM output: a teach-confirmed,
   driver-evidenced, **fall-loud-on-drift** `concept → (measure expression, predicate/value-set)`
   binding. A live value absent from the confirmed set → the dependent metric is
   *inconclusive-with-reason*, never computed on a stale partition.
4. **Verification is mostly gone.** Keep `verifier.py` (value-bound + NULL floor) as a cheap
   post-execution **sanity floor only** — not the honest-fail gate. The scalar LLM judge is
   out. Execution self-consistency is *deferred, not rejected* — it is the eventual best
   validation, but only **after** the feed lands (pre-feed it just entrenches the
   modal-wrong guess).

## Consequences

- The metric stops improvising because it is *fed*, not because of a checker.
- Routing is deterministic on **confidence**, not SQL shape: teach-confirmed binding +
  structural match (sqlglot: emitted predicate ⊆ confirmed binding) + executes → auto-accept;
  else → `SqlEditor`/teach (the overwrite *is* a teaching).
- Known-open edges this ADR does not close: composite-key joins (many-to-many fan-out,
  DAT-277), and consuming the grounding confidence so a low-confidence binding cannot render
  a green `state=executed` (DAT-631). These are tracked under epic DAT-543, not here.
