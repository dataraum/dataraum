# DataRaum

[![License](https://img.shields.io/github/license/dataraum/dataraum)](LICENSE)

The understanding layer that grounds an organization's operating model in its own data.

A semantic layer tells BI tools what columns are *called*. DataRaum learns what they *mean* — the concepts, relationships, rules, and measures of the organization — and grounds each one in the actual data, with a measured confidence behind it. See the [docs](docs/index.md) for the full picture.

## Monorepo layout

```
packages/
├── engine/          # Python — pipeline, detectors, Temporal activity worker
├── cockpit/         # TypeScript — TanStack Start web UI
├── dataraum-config/ # YAML data — entropy config, LLM prompts, verticals (bind-mounted, never imported)
└── infra/           # docker-compose orchestration
```

Each package has its own README. Start there if you're working in a specific package.

## Status

DataRaum runs as a multi-container platform, isolated per **workspace**:

- **engine** (Python) — a **Temporal activity worker**, no HTTP. Does the durable analysis (`add_source`, `begin_session`, `operating_model`) and writes metadata to the workspace's Postgres schema.
- **cockpit** (TanStack Start) — the web app you use. Hosts the chat agent, renders the results, and orchestrates the journey by triggering engine workflows via Temporal.

Each workspace runs its own pair of those two containers. In front of them sit the **portal** (the cockpit image in a second role — login, membership routing, workspace provisioning) and **Caddy**, which serves the portal on the parent domain and each workspace on its own subdomain.

They share one substrate: **Postgres** (metadata + cockpit state + catalogs), an **S3 object store** (the DuckLake data lake + uploads), and **Temporal** (durable orchestration). No HTTP seam between engine and cockpit — the integration surface is Postgres + Temporal. See the [platform architecture](docs/platform/architecture.md).

## Quick start

```bash
# Set the LLM key
cp packages/infra/.env.example packages/infra/.env
echo "ANTHROPIC_API_KEY=sk-ant-..." >> packages/infra/.env

# Bring up the whole installation (substrate, engine worker + cockpit for the
# default workspace, portal, and the Caddy ingress)
docker compose -f packages/infra/docker-compose.yml up -d --wait

# Engine health = the Temporal worker heartbeat (no HTTP endpoint):
docker compose -f packages/infra/docker-compose.yml run --rm --no-deps \
  --entrypoint temporal temporal-admin-tools \
  worker list --namespace default --address temporal:7233   # → Status: Running

# Sign in at the portal, then open the default workspace from there
open http://dataraum.localhost              # dev@dataraum.dev / dataraum-dev
```

Caddy routes by hostname: the parent domain serves the **portal** (login + your workspaces),
and each workspace has its own subdomain (`http://ws1.dataraum.localhost`). `localhost:3000`
is published for debugging but always `401`s — the session cookie lives on the parent domain.

The one thing that bites on a first run: **port 80 must be free.** Caddy binds it, and if it
can't, `--wait` aborts before the portal ever starts — set `CADDY_HTTP_PORT` *and* a matching
`DATARAUM_PORTAL_ORIGIN` to move it. (`*.localhost` needs no `/etc/hosts` entry.)

Compose defines exactly **one** workspace pair — bootstrap scaffolding, so a fresh install
has something to log into and something for the provisioner to clone. Every other workspace
is created from the portal (**New workspace**) or
`cd packages/cockpit && bun run workspace:create`; compose does not grow a service per
workspace.

Full walkthrough, including troubleshooting: [Running the stack](docs/getting-started/running-the-stack.md).
For UI iteration, run the cockpit dev server outside docker for hot reload — see `packages/cockpit/README.md`.

### Run a released version (published images)

The quick start above **builds** the engine and cockpit from source. To run the
published release images instead — a deploy host, no build toolchain — layer the release
overlay and name the version:

```bash
export DATARAUM_VERSION=1.2.3          # any tag from a GitHub Release
docker compose \
  -f packages/infra/docker-compose.yml -f packages/infra/docker-compose.release.yml \
  --env-file packages/infra/.env up -d --wait --no-build
```

This pulls `ghcr.io/dataraum/{dataraum, dataraum-cockpit, dataraum-cockpit-migrate}` at
that tag. See [Deployment](docs/operations/deployment.md) for the images, schema/migration
handling, and the per-workspace topology.

## Develop

- **Engine (Python):** `cd packages/engine && uv sync --group dev && uv run pytest --testmon tests/unit -q`. See `packages/engine/README.md` and `packages/engine/CLAUDE.md`.
- **Cockpit (TypeScript):** `cd packages/cockpit && bun install && bun --bun run dev` (the `--bun` flag is required). See `packages/cockpit/README.md` and `packages/cockpit/CLAUDE.md`.
- **Pull the engine metadata schema (cockpit):** `cd packages/cockpit && DATARAUM_WORKSPACE_ID=<id> METADATA_DATABASE_URL=<url> bun run db:pull:metadata`. Re-run after the engine adds/changes SQLAlchemy models.

## Documentation

Platform docs live in `docs/` (workspace root) and are published via Zensical. Start at
[`docs/index.md`](docs/index.md), or serve the site locally:

```bash
uv run --project packages/engine zensical serve   # run from the repo root
```

- [Overview](docs/getting-started/overview.md) — what DataRaum does, at a glance
- [The approach](docs/concepts/approach.md) · [the journey](docs/concepts/the-journey.md) · [pipeline & phases](docs/concepts/pipeline.md) · [learnable surface](docs/concepts/learnable-surface.md) · [measurement & detectors](docs/concepts/measurement.md)
- [Platform architecture](docs/platform/architecture.md) — under the hood
- [Decision records](docs/adr/README.md) — the *why* behind the architecture

## License

Apache 2.0 — see [LICENSE](LICENSE).
