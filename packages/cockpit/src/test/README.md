# Cockpit test harness

Two vitest projects (`vitest.config.ts`):

| project | command | what runs |
| --- | --- | --- |
| `unit` | `bun run test` | pure units — no DB, no containers, no network |
| `integration` | `bun run test:integration` | real infrastructure: a fixture workspace, real DuckDB |

## The standing rule: boundary tests use production-shape fixtures

**A test at a boundary — SQL, persisted JSON, catalog metadata, anything the
engine or the model authored — must use the shape production actually
produces. Never idealized bare-name SQL.**

This is not a style preference. Every high-value cockpit defect of the DAT-671
wave was invisible to unit tests, `tsc` and `build`, and every one of them
hid in the gap between the idealized shape a test used and the real shape:

- `orders` vs **`lake.typed.current_orders_enriched`** — a qualified relation
  reaching a composer that never reduced it emits one quoted identifier and
  dies at bind time.
- `SUM(amount)` vs **`SUM(amount) AS revenue`** — nothing strips a
  model-authored alias, so it becomes `... AS revenue AS "value"`.
- `SUM(amount)` vs **`CASE WHEN COUNT(*) = 0 THEN NULL ELSE SUM(amount) END`**
  — the house empty-aggregation rule wraps *every* scalar. This is the normal
  shape of a real answer, not an edge case.
- `region` vs **`region_id__name`** — the enrichment's `<fk>__<attr>` spelling
  is what the catalog holds, and a model-authored alias like `account_name`
  simply does not match it. Tier A then reports "no axes" on a result that
  visibly has a dimension in it.

A fixture that uses the tidy spelling tests a system we do not ship. When you
add a boundary test, take the shape from `packages/engine/schema.sql`, from a
real smoke run, or from `src/test/seed-catalog.ts` — not from your head.

Corollary: **an empty result is a claim that needs its own assertion.** "No
axes", "not proven", "no checks" and "nothing analyzed yet" all render as
nothing. Assert the *reason*, not just the emptiness — that is precisely how
these shipped silently.

## The fixture workspace

`bun run test:integration` boots ONE throwaway Postgres for the whole run
(`src/test/global-setup.ts`) carrying:

- the **engine's** generated schema — raw run-stamped tables in `engine`
  (`packages/engine/schema.sql`), promoted-read views in `public`
  (`schema_read.sql`). This mirrors `scripts/pull-metadata.sh` exactly,
  because that is the layout the checked-in Drizzle mirror was introspected
  from. Reads resolve unqualified to the views just as the reader role does in
  production (ADR-0008); test writes name `engine.<table>`.
- **cockpit_db** at the current migration head, applied with the real
  `drizzle-kit migrate` against the checked-in migration folder — never a
  hand-copied DDL snapshot, which would silently test yesterday's shape while
  lanes land migrations concurrently.
- one canonical, head-promoted catalog (`src/test/seed-catalog.ts`).

**Head gating bites.** The `current_*` views are head-joined. A row inserted
without its `metadata_snapshot_head` row is invisible, and the surface under
test returns "nothing analyzed yet" rather than failing. When a fixture-backed
read comes back empty, suspect the head first.

### Writing a fixture-backed suite

```ts
const fx = attachFixtureWorkspace();

describe.skipIf(!fx.available)(fx.describeName("my suite"), () => {
  let subject: typeof import("../thing");
  beforeAll(async () => { subject = await import("../thing"); });
});
```

`attachFixtureWorkspace()` must run at module top level and the subject must
be imported **dynamically inside `beforeAll`**: `config.ts` parses its Zod
schema at module eval, so a static import would read the environment before
the fixture DSNs are installed.

### Requirements and skips

Only a **docker daemon** is needed — no compose stack. Without one, every
fixture-backed suite skips *loudly*, naming the reason in the suite title and
a console warning; the run still exits 0. A fixture that fails to **build**
while docker IS present is a hard failure — that means our seeding broke (e.g.
the engine schema no longer applies).

Older suites predating the fixture are gated on `providedByEnvironment(...)`
and still expect a seeded compose stack; they skip unless the environment
supplies real DSNs.

## Two runtime gotchas

1. **The integration project MUST run under `bun --bun`** (wired in
   `package.json`). `src/db/metadata/client.ts` imports `SQL` from `"bun"`,
   which cannot load under Node at all — under a Node runner every
   metadata-backed suite fails at import with `Cannot find package 'bun'`.
   This went unnoticed for a long time because those suites also skipped on an
   unset DSN, so they never reached the import.

2. **The JS `testcontainers` library hangs under Bun** (probed: fine under
   Node, no container and no output after 7 minutes under Bun). Since the
   project must run under Bun, the fixture drives the `docker` CLI directly —
   the same thing `scripts/pull-metadata.sh` already does in CI, with the same
   image. Do not reintroduce the dependency. (The *engine's* Python
   testcontainers is unaffected — different language, different runtime.)

## Config env belongs in one place

`src/test/integration-env.ts` is the single home for what `config.ts` and
`config.base.ts` require to parse. It replaced fourteen hand-copied
`REQUIRED_DEFAULTS` blocks that had drifted apart — when DAT-819 added a
required `BETTER_AUTH_SECRET`, three suites broke at import and the rest only
escaped because their gate skipped them first.

Add a new required config field **there, once**. And gate on
`providedByEnvironment(...)`, never a bare `process.env` read: vitest reuses a
worker process across files, so once any sibling applied the stub, a bare read
is true even on a bare checkout and the suite runs against an unreachable
placeholder instead of skipping.

Reachability is not a sufficient gate for a **credentialed** service either.
Lanes run concurrently here, so a sibling's compose stack may well answer on
the expected port with different credentials — the port probe passes and the
first authenticated call fails.
