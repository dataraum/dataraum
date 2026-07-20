# Deployment

There are two ways to run DataRaum, and they differ only in where the engine and
cockpit images come from:

- **Build from source** — the default `docker-compose.yml` builds both images from this
  checkout. This is the dev / CI / local-smoke path (see the package READMEs).
- **Run published images** — a release publishes versioned images to GHCR; a deploy host
  pulls them and needs no build toolchain or source tree. That is what this page covers.

## The published images

A [GitHub Release](https://github.com/dataraum/dataraum/releases) (tag `vX.Y.Z`) triggers
the `Release` workflow, which builds and pushes three images to the GitHub Container
Registry. Each is tagged with the release version **and** `latest`:

| Image | Role |
|---|---|
| `ghcr.io/dataraum/dataraum` | the engine analysis worker (one container per workspace) |
| `ghcr.io/dataraum/dataraum-cockpit` | the web app + its co-located activity-only worker |
| `ghcr.io/dataraum/dataraum-cockpit-migrate` | one-shot: applies the `cockpit_db` migrations, then exits |

The engine is **not** published to PyPI and the cockpit is **not** published to npm — the
containers are the only distribution artifact. The substrate dependencies
(`postgres`, `temporal*`, `seaweedfs`) are stock upstream images, pinned in the base
compose file; they need no override.

## Run a released version

The base compose file builds; a thin overlay, `docker-compose.release.yml`, swaps the
app services — `engine-worker`, `cockpit`, `cockpit-migrate`, and `portal` — over to the
published tags (`pull_policy: always`). Layer it on top and name the version you want:

```bash
cp packages/infra/.env.example packages/infra/.env
echo "ANTHROPIC_API_KEY=sk-ant-..." >> packages/infra/.env
export DATARAUM_VERSION=1.2.3          # any tag the Release workflow pushed

docker compose \
  -f packages/infra/docker-compose.yml \
  -f packages/infra/docker-compose.release.yml \
  --env-file packages/infra/.env \
  up -d --wait --no-build
```

- **`DATARAUM_VERSION` is required** by the overlay — a deploy must name the tag it runs
  (no implicit `latest`). Set it in the environment or uncomment it in `.env`.
- **The ingress needs a free port 80** (or `CADDY_HTTP_PORT`), and
  **`DATARAUM_PORTAL_ORIGIN` must name the origin users actually reach**, port included.
  Caddy serves the portal on that origin and each workspace on a subdomain of it; the
  value is what workspace links are built from and what better-auth matches request
  origins against, so a mismatch breaks sign-in flows. Set `BETTER_AUTH_SECRET` to a real
  value (`openssl rand -base64 32`) — the committed default is a dev placeholder, and the
  portal and every workspace cockpit must share it.
- **`--no-build` is load-bearing.** The base file's `build:` sections still merge in, so
  without it a failed pull could silently build from whatever source is on the host. With
  it, an unreachable or misspelled tag fails loud instead.
- The engine has **no HTTP healthcheck** — its health is the Temporal worker heartbeat:

  ```bash
  docker compose -f packages/infra/docker-compose.yml run --rm --no-deps \
    --entrypoint temporal temporal-admin-tools \
    worker list --namespace default --address temporal:7233   # → Status: Running
  ```

Only the two compose files, `.env`, and the read-only config tree
(`packages/dataraum-config/`, bind-mounted at `/opt/dataraum/config`) are needed on the
host — no source checkout, no build tools.

## Schema on startup

The two databases are provisioned differently, and neither needs a manual step:

- **`cockpit_db`** (Drizzle-owned) — the `cockpit-migrate` container runs
  `db:migrate:cockpit` **once** before the cockpit serves; the cockpit's start gates on its
  successful completion. This is why the migrate image ships separately: the slim cockpit
  runner carries no `drizzle-kit`.
- **Engine `ws_<id>`** (SQLAlchemy-owned) — the engine worker **self-bootstraps** its
  workspace schema at boot. There is no migration step and no migrate image for it.

## Version-lock the app images

The cockpit bakes its typed read-mirror of the engine schema at build time, so an engine
and a cockpit from different releases can drift at runtime. The release tags all three
images from one commit, and the overlay pins every app service to the **same**
`DATARAUM_VERSION` — deploy one version across the stack; do not hand-pin different tags
per service.

## Scaling: one engine per workspace

A [workspace](../platform/architecture.md#the-per-workspace-model) is the unit of
isolation — its own engine container, Temporal queue (`engine-<id>`), Postgres schema
(`ws_<id>`), DuckLake catalog schema (`ws_<id>` in the installation-wide catalog DB),
and `s3://bucket/<id>/` prefix. Each workspace runs its **own** engine container *and its
own cockpit container* — the cockpit is per-workspace, not one app routing every request.

Do not add workspaces by editing compose. The `engine-worker` + `cockpit` services are the
one bootstrap pair, and they double as the template: the provisioner clones their recorded
container config, overrides the routing knobs and the minted per-workspace secrets, and
registers the Caddy route. Adding a workspace is therefore the portal's **New workspace**
flow (or `bun run workspace:create`), and a var added to those two services flows into
every workspace provisioned afterwards.

The images are plain OCI containers — the compose overlay is the reference topology, not a
requirement. Run them under any orchestrator that gives them the same substrate (one
Postgres, one S3-compatible object store, one Temporal, the config mount, and
`ANTHROPIC_API_KEY`).

See [Platform architecture](../platform/architecture.md) for how these pieces fit
together and why the engine↔cockpit seam is Postgres + Temporal rather than HTTP.
