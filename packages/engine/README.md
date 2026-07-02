# DataRaum Engine

[![License](https://img.shields.io/github/license/dataraum/dataraum)](../../LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/dataraum/dataraum/ci.yml?branch=main)](https://github.com/dataraum/dataraum/actions)

The Python half of [DataRaum](../../README.md) — the durable analysis engine. It runs as a
**Temporal activity worker** with no HTTP surface: the cockpit starts its workflows by name
and reads the metadata it writes straight from Postgres. What DataRaum is and why it works
this way is in the [platform docs](../../docs/index.md); this README is the package-level
map.

## What it does

The engine owns the three durable analysis stages of the journey, each a Temporal workflow:

- **`add_source`** — imports sources VARCHAR-first (failed casts go to quarantine tables,
  never fail the run), infers types, profiles statistics, and grounds per-column meaning
  with an LLM.
- **`begin_session`** — works out how the tables relate (value overlap → referential-
  integrity evaluation → LLM confirmation), builds enriched join views, finds sliceable
  dimensions and hierarchies, reconciles measures across facts (stock vs. flow), and ranks
  drivers.
- **`operating_model`** — grounds and executes the declared model: validations, business
  cycles, and metrics (LLM-authored SQL over deterministic grounding evidence,
  [ADR-0016](../../docs/adr/0016-metric-sql-grounding-llm-authoring.md)).

After each stage a terminal detector pass measures **entropy** — disagreement between
independent witnesses — and rolls it up into per-intent readiness (*ready / investigate /
blocked*). Every run is versioned: phases append run-stamped rows, and a promote step makes
a run visible only once it has fully succeeded.

Concepts in depth: [pipeline & phases](../../docs/concepts/pipeline.md) ·
[measurement & detectors](../../docs/concepts/measurement.md) ·
[the journey](../../docs/concepts/the-journey.md) ·
[platform architecture](../../docs/platform/architecture.md).

## How it fits the monorepo

One of four packages (see the [root README](../../README.md)):

- **engine** (this package) — Python pipeline, detectors, Temporal activity worker.
- **`../cockpit`** — TanStack Start web app: the chat agent, the canvas, and a co-located
  TS orchestration worker.
- **`../dataraum-config`** — YAML data (entropy contracts, LLM prompts, verticals);
  bind-mounted, never imported.
- **`../infra`** — docker-compose orchestration.

The engine↔cockpit seam is **Postgres + Temporal, nothing else** — no HTTP, no OpenAPI, no
codegen ([ADR-0002](../../docs/adr/0002-engine-no-http-transport.md)). The engine owns the
workspace's `ws_<id>` Postgres schema (SQLAlchemy); the cockpit reads it through a generated
Drizzle mirror. `schema.sql` in this package is the generated offline DDL dump of all
models — regenerate with `uv run python -m dataraum.storage.dump_ddl`, never hand-edit
(CI fails on drift).

## Run it

From the workspace root — full walkthrough in
[Running the stack](../../docs/getting-started/running-the-stack.md):

```bash
docker compose -f packages/infra/docker-compose.yml up -d --wait
# Engine health = the Temporal worker heartbeat (there is no HTTP endpoint):
docker compose -f packages/infra/docker-compose.yml run --rm --no-deps \
  --entrypoint temporal temporal-admin-tools \
  worker list --namespace default --address temporal:7233   # → Status: Running
```

`ANTHROPIC_API_KEY` is a hard dependency of the analysis pipeline — set it in
`packages/infra/.env` before bringing the stack up. LLM provider and prompt configuration
live in `packages/dataraum-config/llm/`.

## Development

Run from this package directory (`packages/engine/`).

```bash
uv sync --group dev                     # install with dev dependencies
uv run pytest --testmon tests/unit -q   # tests (testmon re-runs only what's affected)
uv run mypy src/                        # type check
uv run ruff check src/                  # lint
uv run ruff format --check src/
```

To run the worker directly against an already-running substrate (Postgres, Temporal, object
store), export the env the compose file wires for the `engine-worker` service —
`packages/infra/docker-compose.yml` is the authoritative list (`DATABASE_URL`,
`DUCKLAKE_*`, `DATARAUM_WORKSPACE_ID`, `TEMPORAL_*` with the workspace's task queue per
[ADR-0012](../../docs/adr/0012-per-workspace-tenancy.md), `ANTHROPIC_API_KEY`) — then:

```bash
uv run python -m dataraum.worker.main
```

Two things to know before testing: **e2e tests make real LLM calls** — don't run them
casually; and detector correctness is proven by **calibration** against ground-truth data
(recall on known injections, precision on clean data) in the separate `dataraum-eval`
repo, not by unit tests alone.

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
