# ADR-0021 — Source model: per-object content-keyed sources; the run ingests a set; the session is the only named unit

- **Status:** Accepted (records shipped reality)
- **Date:** 2026-07-07
- **Ticket:** DAT-420 (epic; also DAT-401, DAT-422, DAT-596, DAT-639)
- **Design doc:** Confluence DD/30900226

## Context

The engine's `Source` row conflated a re-readable *origin* with the user's
*selected set of inputs*, and the ingest run was bound to exactly one source. That
single coupling produced a cluster of defects: re-uploading one dataset under three
names made three peer "sources" (inventory noise), source identity was an arbitrary
unique name so same-name-new-bytes was silently skipped (presence-only skip →
silent staling), and the content digest that uploads already carried was thrown
away as identity.

## Decision

- **A source is the dumb thing you upload or connect.** One uploaded file = one
  **content-keyed** source (`src_<digest>`) carrying exactly one staged object; one
  DB connection = one **name-keyed** source carrying its synthesized recipe and a
  `recipe_hash` minted by the cockpit `select` tool (the only producer — the engine
  never recomputes hashes; they are opaque tokens of one writer). No relatedness is
  inferred at upload.
- **The run (`add_source`) ingests a set of sources**, executing the import phase
  once per source. Everything past import is source-free and session-scoped:
  source identity **dies at the add_source boundary** — downstream stages address
  typed `table_id`s, never a `source_id`.
- **The content hash is identity and the skip axis.** Same bytes → same source →
  skip (a presence check is correct because changed bytes mint a new digest → a new
  source → a fresh import). DB sources skip only while `recipe_hash` matches the
  imported witness — an identical re-select is an intentional no-op; a *changed*
  recipe triggers in-place re-import-with-replace (full teardown of the source's
  tables and derived metadata), never a silent stale read. Raw tables therefore
  carry no `run_id`: the hash is the raw version axis; `run_id` versions derived
  metadata only.
- **Raw tables get narrow, workspace-unique names** — no source prefix; the source
  is an atomic wrapper for provenance, not a namespace.
- **The session (`begin_session`) is the only unit the user names**, composed from
  any tables across any runs. Relatedness emerges at session composition — never
  asserted at upload; discovering that combined data holds nothing is the product
  working.

**Rejected:** a synthetic "uploads" umbrella source with a `source_uri` column and a
swapped unique constraint (a churn cascade through every source-keyed import path —
the per-object source dissolves it while reusing existing machinery); one named
source per upload batch (over-reads intent, and a batch has no single content hash
to key on); treating the identical-recipe skip as a staleness bug (it is the
witness contract, not a poor-man's update vector).

## Consequences

- Deduplication is structural (content addressing), not procedural; inventory
  noise ends at the root; silent staling is impossible for uploads and loud for
  recipes.
- Forbidden: binding pipeline stages after import to a source; using the source as
  a table namespace; a second producer minting or recomputing content/recipe
  hashes.
- Open: garbage collection of superseded objects (the hash already provides the
  axis) and surfacing "changed since you analysed it" are deferred; `add_source`
  is now a misnomer for an ingest run (rename parked as DAT-427-era residue).
