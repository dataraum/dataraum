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

## Bring it up

Run from the workspace root:

```bash
# One-time: seed the env file and set the LLM key
cp packages/infra/.env.example packages/infra/.env
echo "ANTHROPIC_API_KEY=sk-ant-..." >> packages/infra/.env

# Build + start Postgres, the object store, Temporal, the engine worker, and the cockpit
docker compose -f packages/infra/docker-compose.yml --env-file packages/infra/.env up -d --wait
```

or, equivalently, `make up` from the root.

The first run builds both app images and pulls the substrate images, so it takes a few
minutes; subsequent runs reuse the layers. `--wait` blocks until every service is healthy
(and the one-shot `cockpit-migrate` has applied the `cockpit_db` migrations).

## Verify

The engine is a **Temporal activity worker** with no HTTP surface — its health is the
worker heartbeat, not a port. Check it through Temporal:

```bash
docker compose -f packages/infra/docker-compose.yml run --rm --no-deps \
  --entrypoint temporal temporal-admin-tools \
  worker list --namespace default --address temporal:7233
# → Status: Running   (on the engine-<workspace-id> task queue)
```

Then open the cockpit — the only surface you interact with:

```
open http://localhost:3000
```

The Temporal web UI is at <http://localhost:8080> if you want to watch workflows run.

## Everyday operations

```bash
make logs                    # tail all services  (or: docker compose … logs -f)
make down                    # stop the stack
```

- **Iterating on the cockpit UI?** Skip the cockpit container and run the dev server
  outside docker for hot reload — bring up only the backend deps and `bun --bun run dev`.
  See [`packages/cockpit/README.md`](https://github.com/dataraum/dataraum/blob/main/packages/cockpit/README.md).
- **Changed engine or cockpit code and want it in the container?** A plain `up` reuses the
  cached image. Rebuild and recreate the affected service:

  ```bash
  docker compose -f packages/infra/docker-compose.yml up -d --build --force-recreate cockpit
  ```

  This matters for the cockpit's orchestration worker in particular: its workflow bundle is
  baked into the image at build time, so without `--build --force-recreate` the old bundle
  keeps running and your change silently never lands.

## Multiple workspaces

A [workspace](../platform/architecture.md#the-per-workspace-model) is the unit of isolation
— its own engine container, Temporal queue, Postgres schema, catalog, and object-store
prefix. The default stack runs one. To bring up a second engine worker side by side (for a
two-workspace smoke), enable the profile:

```bash
docker compose -f packages/infra/docker-compose.yml --profile multi-workspace up -d --wait
```

The cockpit is a single app that serves all workspaces, routing each request to the right
one via the registry.

## Next

- [Deployment](../operations/deployment.md) — run pinned release images on a host, no build.
- [Platform architecture](../platform/architecture.md) — the two-tier split, the substrate,
  and how a run flows.
