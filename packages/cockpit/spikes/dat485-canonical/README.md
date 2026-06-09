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

Result on THIS small corpus: byte-identical PY≡TS on every member. **But a 7-class
stress-test (joins/CASE/window/nested/agg/funcs/setops) proved the byte-identical
claim FAILS on common SQL** — sqlglot and polyglot diverge on:
- `DATE_TRUNC('month')` literal-casing (PY → `'MONTH'`, TS → `'month'`)
- `IS NOT NULL` / `IS NOT TRUE` / `NOT BETWEEN` (PY → `NOT x IS NULL`, TS keeps); `NOT LIKE` inverts the other way
- abbreviated window frames (`ROWS 3 PRECEDING` → PY expands, TS keeps)
- `SUBSTRING(.. FROM .. FOR ..)` and `INTERVAL '7 days'` rendering
- polyglot can't parse `EXTRACT(.. FROM ..)` / `TABLESAMPLE 10 PERCENT` (sqlglot can)

**Conclusion:** two native engines do NOT share a canonical key. For cross-language
snippet sharing (P2a / GraphAgent overlap, **DAT-486**) use ONE engine in both —
polyglot bound into Python. P1 is unaffected (polyglot canonicalizes both sides).
Deferred shared gaps: commutative order (**DAT-492**), frame-keyword case. The TS
side is locked by `src/lib/sql-canonical.test.ts`.
