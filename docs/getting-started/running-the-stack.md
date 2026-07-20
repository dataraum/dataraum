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
- **Ports 3001, 4317, and 4318 free.** The bundled observability container publishes
  Grafana on `3001` and the two OTLP receivers on `4317`/`4318`. It has no compose profile,
  so a plain `up` starts it; if something on the host already holds one of those ports, `up`
  fails at container start the same way a busy `:80` does. Free the port, or drop the
  service from the `up` if you don't need traces.

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

A default `up` gives you the complete installation: Postgres, the object store, Temporal,
**one engine worker and one cockpit** for the default workspace, the **portal**, **Caddy**,
and the bundled **observability** container (Grafana plus an OTLP collector, both workers
export to it). No service is behind a compose profile, so there is nothing to opt into and
nothing held back. See
[the container table](../platform/architecture.md#the-containers-concretely) for what each
one is.

### If port 80 is taken

Caddy can't bind, so `up` fails at container start and the `portal` service is left in
`Created` — the failure looks like the stack half-came-up. (On macOS the built-in Apache
is a common squatter; `curl -I http://localhost` names whoever holds it. A squatter bound
to IPv4 only does *not* collide — Docker binds the IPv6 stack and `up` succeeds — so the
symptom appears with dual-stack listeners.) Either free the port, or move the ingress — in
`packages/infra/.env`:

```bash
CADDY_HTTP_PORT=8000
DATARAUM_PORTAL_ORIGIN=http://dataraum.localhost:8000
```

**Both lines, same port.** `DATARAUM_PORTAL_ORIGIN` is what workspace links are built from
(`workspace-url.ts` uses its `host`, port included) and what better-auth matches request
origins against; if it disagrees with the published port, links point at a port nothing
listens on. Every URL below then carries `:8000`.

## Sign in

Caddy routes by hostname, so the entry point is a **domain, not a port**:

| URL | What |
|---|---|
| `http://dataraum.localhost` | the **portal** — sign in, see your workspaces, create new ones |
| `http://ws1.dataraum.localhost` | the default workspace's cockpit |

Start at the portal. Dev stacks seed a credential user from `DATARAUM_DEV_USER_EMAIL` /
`DATARAUM_DEV_USER_PASSWORD` (`dev@dataraum.dev` / `dataraum-dev` by default; unset both to
disable). Sign in, then follow **Open** into the workspace.

There is no sign-up *screen*, but better-auth's endpoint is mounted and public: an
unauthenticated `POST /api/auth/sign-up/email` creates an account. Combined with the create
policy — any signed-in user of the installation may provision a workspace — anyone who can
reach the portal can create one. That is accepted while installations carry only test
users; it needs closing before an install carries real ones or is exposed beyond localhost.

Two things worth knowing:

- **`http://localhost:3000` is not the entry point.** The cockpit container still publishes
  it for debugging, but the session cookie is scoped to the parent domain, so a request to
  `localhost` never carries one. A browser there is redirected to the portal; a non-HTML
  caller (curl, a script) gets `401`. Use the subdomain.
- **`*.localhost` resolves to loopback on its own**, at any depth — no `/etc/hosts` edit.
  Browsers implement this themselves, and on macOS the system resolver does too, which is
  why plain `curl` and Safari also work (Safari relies on the resolver, not its own rule).
  Some Linux resolvers don't; there, add the hosts or use
  `curl --resolve ws1.dataraum.localhost:80:127.0.0.1 …`.

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
engine + cockpit pair, and the two halves seed it between them: the **engine** worker
creates the `ws_<id>` schema, its promoted-read schema, and the two metadata roles from its
`DATARAUM_WORKSPACE_ID` (it never reads `cockpit_db`), while the **cockpit** seeds the
`cockpit_db` side — the registry row named "Default Workspace", and the dev user's
membership. It exists so a fresh install has something to log into and something for the
provisioner to clone. Treat it as scaffolding.

**Every other workspace is provisioned**, which mints the roles and catalog schema, starts an
engine + cockpit pair, and registers the Caddy route. Compose does *not* grow a service per
workspace. Two equivalent front doors:

- The portal → **New workspace**: name,
  [vertical](../concepts/learnable-surface.md#verticals-reusable-starting-points),
  subdomain.
  This is the path to prefer — the portal container already holds the admin connections
  and the docker socket the lifecycle needs.
- The CLI, for scripted or headless setups. It runs on the **host**, so it needs the
  provisioner env pointed at the published ports — the full block is documented at the top
  of `packages/cockpit/scripts/provision-workspace.ts`; without it you get a config throw
  naming the missing field.

  ```bash
  cd packages/cockpit
  # ... provisioner env (see the script header) ...
  bun run workspace:create -- --id "$(uuidgen | tr A-Z a-z)" --name "Controlling" \
      --subdomain ws3 --vertical finance --member dev@dataraum.dev
  bun run workspace:archive -- --id <workspace-id>     # the undo
  ```

  Both ops are idempotent **on an id**: re-running with the same `--id` resumes a create
  that died midway. That is why the example passes one — without `--id` a re-run mints a
  fresh UUID and then collides with the first attempt's still-live subdomain.

## Everyday operations

```bash
make logs                    # tail all services  (or: docker compose … logs -f)
make down                    # stop the stack
```

- **Iterating on the cockpit UI?** Skip the cockpit container and run the dev server
  outside docker for hot reload — bring up only the backend deps and `bun --bun run dev`.
  Note that bare host dev runs the **workspace** role only: there is no portal and no
  sign-in screen, and the membership gate redirects a signed-out request to the portal
  origin — which defaults to `http://localhost:3000`, i.e. itself. Use the composed stack
  for anything that involves logging in. See
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
