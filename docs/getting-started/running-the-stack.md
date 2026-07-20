# Running the stack

This page brings DataRaum up **locally, built from source** — the dev and evaluation path.
To run pinned release images on a deploy host instead, see [Deployment](../operations/deployment.md).

## Prerequisites

- **Docker** with Compose v2. That's the only hard requirement — the engine (Python) and
  cockpit (TypeScript) images build inside the stack; you don't need `uv` or `bun` on the
  host just to run it. (You do for [developing](../platform/architecture.md) either package;
  see the package READMEs.)
- An **Anthropic API key**. Semantic analysis and the chat agent need it; the substrate
  boots without it, but the pipeline can't complete.
- **Port 80 free**, or a spare port to put the ingress on. Caddy terminates every URL you
  use and publishes `:80` by default — see [If port 80 is taken](#if-port-80-is-taken).

## Bring it up

Run from the workspace root:

```bash
# One-time: seed the env file and set the LLM key
cp packages/infra/.env.example packages/infra/.env
echo "ANTHROPIC_API_KEY=sk-ant-..." >> packages/infra/.env

# Build + start the whole installation
docker compose -f packages/infra/docker-compose.yml --env-file packages/infra/.env up -d --wait
```

or, equivalently, `make up` from the root.

The first run builds both app images and pulls the substrate images, so it takes a few
minutes; subsequent runs reuse the layers. `--wait` blocks until every service is healthy
(and the one-shot `cockpit-migrate` has applied the `cockpit_db` migrations).

A default `up` — no profiles — gives you the complete installation: Postgres, the object
store, Temporal, **one engine worker and one cockpit** for the default workspace, plus the
**portal** and **Caddy**. See [the container table](../platform/architecture.md#the-containers-concretely)
for what each one is.

### If port 80 is taken

Caddy can't bind, `--wait` aborts, and the `portal` service never starts — the failure
looks like the stack half-came-up. (On macOS the built-in Apache is a common squatter;
`curl -I http://localhost` will name whoever holds it.) Either free the port, or move the
ingress — in `packages/infra/.env`:

```bash
CADDY_HTTP_PORT=8000
DATARAUM_PORTAL_ORIGIN=http://dataraum.localhost:8000
```

**Both lines, same port.** The portal origin is what the session cookie is derived from and
what workspace links are built from; if it disagrees with the published port, sign-in
appears to work and then every workspace link 401s. Every URL below then carries `:8000`.

## Sign in

Caddy routes by hostname, so the entry point is a **domain, not a port**:

| URL | What |
|---|---|
| `http://dataraum.localhost` | the **portal** — sign in, see your workspaces, create new ones |
| `http://ws1.dataraum.localhost` | the default workspace's cockpit |

Start at the portal. Dev stacks seed a credential user from `DATARAUM_DEV_USER_EMAIL` /
`DATARAUM_DEV_USER_PASSWORD` (`dev@dataraum.dev` / `dataraum-dev` by default; unset both to
disable). Sign in, then follow **Open** into the workspace.

Two things worth knowing:

- **`http://localhost:3000` is not the entry point.** The cockpit container still publishes
  it for debugging, but the session cookie lives on the parent domain — a request to
  `localhost` never carries one, so the membership gate returns `401` every time. Use the
  subdomain.
- **`*.localhost` resolves to `127.0.0.1` on its own**, at any depth — no `/etc/hosts`
  edit. Every major browser does this itself, and on macOS the system resolver does too,
  so plain `curl` and Safari work as well. Some Linux resolvers don't; there, add the
  hosts or use `curl --resolve ws1.dataraum.localhost:80:127.0.0.1 …`.

## Verify

The engine is a **Temporal activity worker** with no HTTP surface — its health is the
worker heartbeat, not a port. Check it through Temporal:

```bash
docker compose -f packages/infra/docker-compose.yml run --rm --no-deps \
  --entrypoint temporal temporal-admin-tools \
  worker list --namespace default --address temporal:7233
# → Status: Running   (on the engine-<workspace-id> task queue)
```

The Temporal web UI is at <http://localhost:8080> if you want to watch workflows run.

## Workspaces

A [workspace](../platform/architecture.md#the-per-workspace-model) is the unit of isolation
— its own engine container, cockpit container, subdomain, Temporal queues, Postgres schema,
catalog schema, and object-store prefix.

**One exists already — as bootstrap, not as a feature.** The compose stack defines a single
engine + cockpit pair, and that cockpit seeds "Default Workspace" at boot from its own
`DATARAUM_WORKSPACE_ID`: the registry row, the `ws_<id>` schema, its two metadata roles, and
the dev user's membership. It exists so a fresh install has something to log into and
something for the provisioner to clone — there is no sign-up surface yet, so without it
nobody could get in at all. Treat it as scaffolding.

**Every other workspace is provisioned**, which mints the roles and catalog schema, starts an
engine + cockpit pair, and registers the Caddy route. Compose does *not* grow a service per
workspace. Two equivalent front doors:

- The portal → **New workspace**: name, [vertical](../concepts/approach.md), subdomain.
- The CLI, for scripted or headless setups:

  ```bash
  cd packages/cockpit
  bun run workspace:create -- --name "Controlling" --subdomain ws3 \
      --vertical finance --member dev@dataraum.dev
  bun run workspace:archive -- --id <workspace-id>     # the undo
  ```

  Both ops are idempotent — re-running with the same id converges, so a create that died
  midway resumes instead of duplicating.

## Everyday operations

```bash
make logs                    # tail all services  (or: docker compose … logs -f)
make down                    # stop the stack
```

- **Iterating on the cockpit UI?** Skip the cockpit container and run the dev server
  outside docker for hot reload — bring up only the backend deps and `bun --bun run dev`.
  Bare host dev has no Caddy and no subdomains: the portal origin defaults to
  `http://localhost:3000` and `/` serves that workspace directly. See
  [`packages/cockpit/README.md`](https://github.com/dataraum/dataraum/blob/main/packages/cockpit/README.md).
- **Changed engine or cockpit code and want it in the container?** A plain `up` reuses the
  cached image. Rebuild and recreate the affected service:

  ```bash
  docker compose -f packages/infra/docker-compose.yml up -d --build --force-recreate cockpit
  ```

  This matters for the Temporal workers in particular — the engine worker (which hosts
  all workflows) and the cockpit's activity-only worker alike: their code is baked into
  the image at build time, so without `--build --force-recreate` the old worker keeps
  polling and your change silently never lands.

## Next

- [Deployment](../operations/deployment.md) — run pinned release images on a host, no build.
- [Platform architecture](../platform/architecture.md) — the two-tier split, the substrate,
  and how a run flows.
