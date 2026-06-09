# DAT-485 cross-language canonical-SQL spike

Proves snippet identity can be a **canonical SQL form** that the Python producer
(GraphAgent / sqlglot) and the TS consumer (query agent / polyglot) compute
**identically** — so a snippet produced in one language is reusable in the other.

- `corpus.json` — SQL groups that SHOULD canonicalize to one key (LLM variance:
  case, whitespace, keyword-casing, `lake.<layer>.` qualifier) + `distinct` that must not collide.
- `canon-py.py` — sqlglot `parse_one().sql()` + lake-strip (the engine's `canonical_sql`).
- `canon-ts.mjs` — polyglot `transpile(duckdb→duckdb)` + lake-strip.
- `diff.mjs` — within-engine grouping + cross-engine byte-agreement table.

Run: `bun canon-ts.mjs > ts-out.json` ; `uv run --directory ../../../engine python "$PWD/canon-py.py" > py-out.json` ; `bun diff.mjs`

Result: **byte-identical PY≡TS on every member.** Two deferred tail cases — output
alias (prompt-steered to `AS value`) and commutative `WHERE a AND b` vs `b AND a`
(DAT-483 follow-up). The TS side is locked by `src/lib/sql-canonical.test.ts`.
