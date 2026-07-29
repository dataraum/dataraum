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
- the **operating-model property graph** (`schema_graph.sql`, ADR-0021) —
  applied over those same `public` read views, same token substitution. A
  `CREATE PROPERTY GRAPH` isn't a table or view Drizzle mirrors, so this is
  the only way a fixture-backed suite reaches `GRAPH_TABLE ( ... MATCH ... )`;
  query it through `src/db/metadata/property-graph.ts`'s
  `queryOperatingModelGraph` (DAT-671 R0), never a bespoke client — that
  module's header carries the PG19 gotchas (fixed-depth `MATCH`, `::text`
  element keys, the grant being a separate privilege object from table
  grants).
- **cockpit_db** at the current migration head, applied with the real
  `drizzle-kit migrate` against the checked-in migration folder — never a
  hand-copied DDL snapshot, which would silently test yesterday's shape while
  lanes land migrations concurrently.
- one canonical, head-promoted catalog (`src/test/seed-catalog.ts`).

**Head gating bites.** The `current_*` views are head-joined. A row inserted
without its `metadata_snapshot_head` row is invisible, and the surface under
test returns "nothing analyzed yet" rather than failing. When a fixture-backed
read comes back empty, suspect the head first. Note `current_tables` is gated
per table on a `generation` head, not the `catalog` one.

**The WRITER role is not modelled.** Both metadata DSNs point one superuser at
the read views' `search_path`; production splits a reader role (`ws_<id>_read`)
from a writer role (`ws_<id>`, the control-table verbs — DAT-816). So the
write-surface suites (`snippet-writer`, `concept-write`, `convention-write`,
`teach`) stay compose-gated and are **not** covered by the fixture. Covering
them means modelling the two roles and the raw `ws_<id>` schema; until someone
does, do not assume a green fixture run exercised a metadata write.

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

**Orphan recovery.** Teardown removes the container, but `--rm` fires on the
*daemon* side only when the container exits — a SIGKILLed vitest (or a dead
docker client) leaves it running. Recover with:

```bash
docker rm -f $(docker ps -aq --filter label=dataraum-fixture=1)
```

Older suites predating the fixture are gated on `providedByEnvironment(...)`
and still expect a seeded compose stack; they skip unless the environment
supplies real DSNs.

## The journey workspace (J1-J9)

`journeys.integration.test.ts` is the practitioner-journey acceptance net. It
differs from every other suite here in one way that matters: it calls the
**routes**, not the resolvers — `Route.options.server.handlers.POST({request})`
with a real `Request`, returning a real `Response`. Nothing is mocked, including
the lake.

It runs against its **own workspace**, not the shared catalog above:

| | |
| --- | --- |
| `journey` database | the same engine schema layout, seeded by `seed-journey.ts` |
| `journey_lake_catalog` database | the DuckLake catalog for its lake |
| a temp dir | the lake's `DATA_PATH` (local, not `s3://`) |

All three live in the SAME container — isolation without a second ~6s boot. The
separation is not tidiness: tier A matches the catalog to a result by column
NAME with no fact scoping, so a second `account_id__name` in the shared database
would surface as a duplicate axis and change what the existing tier-A suite
sees.

**The lake is real.** `buildDucklakeAttachSql` hardcodes `ducklake:postgres:`,
so a file-catalog lake cannot be reached through config at all — which is why
the catalog is a Postgres database in the fixture container and the cockpit
reaches it through nothing but `DUCKLAKE_CATALOG_URL` + `DATARAUM_LAKE_PATH`.
Its real `lake.ts` bootstrap then runs: extension load, S3 secret (inert, scoped
to a bucket nothing reads), `ATTACH … READ_ONLY`, `USE lake.typed`.

**The data is real and the answer key is external.** Rows come from the sibling
`dataraum-testdata` corpus; every asserted figure is derived from it with the
SQL quoted beside it in `journey-answer-key.ts` and cross-checked against that
corpus's `ground_truth.yaml`. Numbers are asserted to the cent. A missing corpus
is a **loud skip** (set `DATARAUM_TESTDATA_PATH` to override the upward search);
a corpus that is present but fails to load is a hard failure.

**Red pins.** Journeys describing behaviour we do not have yet land as a PAIR: a
green `it(...)` pinning today's exact behaviour, and an `it.fails(...)` asserting
the target. `it.fails` reports green while it throws and **fails the run the
moment it starts passing**, so the fix cannot land silently — it must be flipped
by deleting `.fails`. The paired green half is what stops `.fails` from masking a
broken fixture. Do not "fix" a red pin by relaxing it; either the product
changed (flip it) or it did not (leave it).

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
`config.base.ts` require to parse. It replaced **eleven** hand-copied
`REQUIRED_DEFAULTS` blocks (twelve counting `portal/lock`'s ad-hoc one-field
stub) that had drifted apart — when DAT-819 added a required
`BETTER_AUTH_SECRET`, three suites broke at import and the rest only escaped
because their gate skipped them first.

Add a new required config field **there, once**. And gate on
`providedByEnvironment(...)`, never a bare `process.env` read: vitest reuses a
worker process across files, so once any sibling applied the stub, a bare read
is true even on a bare checkout and the suite runs against an unreachable
placeholder instead of skipping.

**Every placeholder must be a value no real environment would hold.** A
placeholder that collides with a documented real value makes the gate answer
"no real infrastructure" for exactly the developers who followed the setup
instructions. This already happened: the S3 credential placeholders were
byte-identical to `.env.example`, so `cp .env.example .env` silently skipped
the SeaweedFS round-trip. Hence the `integration-env-invalid` posture.

Reachability is not a sufficient gate for a **credentialed** service either.
Lanes run concurrently here, so a sibling's compose stack may well answer on
the expected port with different credentials — the port probe passes and the
first authenticated call fails.

> **`S3Error: an unexpected error has occurred` on the SeaweedFS suite** is
> almost always `S3_USE_SSL`. It is optional in the schema and defaults to
> **true** (secure-by-default), so supplying S3 credentials from a shell or CI
> secret *without* it points the client at `https://` on a plaintext gateway.
> `.env.example` sets `S3_USE_SSL=false`; `cp .env.example .env` is correct.
> It is deliberately NOT in `integration-env.ts`, which carries only what the
> config schemas *require* to parse.

And give every conditional suite a **named** skip reason (`suiteTitle(...)` /
`fx.describeName(...)`). A bare `describe.skipIf` renders an unexplained
"skipped", indistinguishable from a suite nobody meant to run — which is how a
silently disabled round-trip survives a whole wave.
