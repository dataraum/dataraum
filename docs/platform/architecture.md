# Platform architecture

DataRaum is a small set of cooperating containers. This page describes how they fit
together: the two workers, the substrate they share, the seam between them, and the
per-workspace model that makes the whole thing multi-tenant.

The decisions behind this shape are recorded as [ADRs](../adr/README.md); this page links
to the relevant one wherever a choice matters.

## The two-tier split

There are exactly two pieces of application code, and the boundary between them is sharp.

```mermaid
flowchart TB
    User(["user"])

    subgraph CP["Cockpit container — TypeScript / TanStack Start"]
        direction TB
        UI["chat surfaces + pages"]
        Agent["agentic LLM tier<br/>(TanStack AI → Anthropic)"]
        Client["Temporal client"]
        AWk["co-located activity-only worker<br/>(cockpit-&lt;workspace&gt; queue)"]
    end

    subgraph EWk["Engine container(s) — Python"]
        direction TB
        Worker["Temporal worker — workflows + activities<br/>(engine-&lt;workspace&gt; queue)"]
        Phases["20 pipeline phases<br/>typing · stats · semantic · …"]
    end

    User --> UI --> Agent
    Agent --> Client
    Client -- "workflow starts (orchestration + single-shot)" --> Worker
    Worker -- "run recording + teach agent, by name" --> AWk
```

- **The cockpit** is the **agentic, interactive tier**. It hosts the chat, the LLM agent
  loop, and all user-facing rendering. It triggers the journey by starting the engine's
  orchestration workflows, and hosts a co-located **activity-only** Temporal worker for
  the control-plane activities (run recording, the grounding-teach agent) those workflows
  call back into. ([ADR-0004](../adr/0004-agent-tier-boundary.md): agentic LLM lives in
  the cockpit; [ADR-0020](../adr/0020-workflows-python-cockpit-activity-only.md): no
  workflow code in the cockpit.)
- **The engine** is the **durable analysis tier**. It is a pure **Temporal worker** —
  it has no HTTP server and no API. It hosts every workflow and runs the
  deterministic, long-running analysis, writing results to Postgres. ([ADR-0002](../adr/0002-engine-no-http-transport.md): engine is a pure activity worker, no HTTP/MCP transport.)

There is **no shared process state** between them. The cockpit and engine talk only
through the substrate: Postgres for metadata, Temporal for work.

## The shared substrate

Three backing services sit under both tiers.

```mermaid
flowchart LR
    subgraph SUB["Substrate"]
        PG[("Postgres<br/>one instance, many databases/schemas")]
        OBJ[("Object store — S3<br/>(SeaweedFS in dev)")]
        TMP["Temporal server"]
    end
```

**Postgres** — one instance hosts several logically separate stores
([ADR-0003](../adr/0003-postgres-schema-ownership.md)):

| Database / schema | Owner | Holds |
|---|---|---|
| `ws_<id>` schema | engine (SQLAlchemy) | per-workspace metadata: tables, columns, semantics, relationships, readiness, lifecycle artifacts |
| `ws_<id>_read` schema | engine | the promoted read views the cockpit is allowed to see |
| `cockpit_db` | cockpit (Drizzle) | the cockpit's own state: workspace registry, chat history, UI state |
| `<catalog>` database | engine | the ONE installation-wide DuckLake catalog database; each workspace's catalog is its own `ws_<id>` schema inside it (`METADATA_SCHEMA` on ATTACH) |
| `temporal` / `temporal_visibility` | Temporal | durable workflow state |

**Object store (S3)** — the data lake lives here, not on a local disk. The engine writes
typed/staged data as parquet under the workspace's lake prefix; file uploads land under an
`uploads/` prefix in the same bucket. In dev this is a single-node **SeaweedFS** S3
gateway; in production it is a real object store with real IAM. (The per-workspace lake
prefix and catalog layout are in
[ADR-0012](../adr/0012-per-workspace-tenancy.md).)

**Temporal** — the durable orchestration backbone. Both workers poll it; it guarantees
long-running analysis survives restarts and is retried correctly.
([ADR-0001](../adr/0001-temporal-orchestration-python.md).)

## The engine↔cockpit seam

Because there is no HTTP between them, the seam is two channels, and only two:

### 1. Work flows through Temporal

The cockpit triggers analysis by starting **engine workflows** by name on the workspace's
task queue. The engine worker bundles three analysis workflows
(`add_source`, `begin_session`, `operating_model`), a per-table child workflow, and the
two orchestration workflows that sequence them (the grounding loop and the session
cascade). The cockpit never runs analysis or workflow code itself; its activity-only
worker serves the run-recording writes and the grounding-teach agent the orchestration
workflows schedule by name. The full orchestration model is in
[ADR-0001](../adr/0001-temporal-orchestration-python.md) and
[ADR-0020](../adr/0020-workflows-python-cockpit-activity-only.md).

### 2. Metadata flows through promoted Postgres views

The cockpit reads engine results straight from Postgres via Drizzle — but **not** from the
engine's raw tables. Engine metadata is **run-versioned**: every phase appends
run-stamped rows, and a terminal *promote* step atomically flips a per-stage head pointer.
Reading "the current state" therefore means joining through that head — a join the engine
materializes once, as a set of generated `current_<table>` **views** in the `ws_<id>_read`
schema. ([ADR-0008](../adr/0008-promoted-read-views.md), [ADR-0010](../adr/0010-failure-contract-idempotent-writers.md).)

The cockpit connects with the workspace's dedicated reader role (`ws_<id>_reader`), whose
`search_path` is pinned to the read schema and which has `SELECT` there **only**. The role
resolves the schema — the cockpit carries no workspace literal — and the raw run-stamped
tables (and every other workspace) are not even visible to it: a stale or wrong-run read
is *unwritable*, not merely discouraged. Control-plane writes use a separate `ws_<id>_writer`
role scoped to exactly the sanctioned control tables. The cockpit mirrors the view schema
with `bun run db:pull:metadata`, which introspects the views into typed Drizzle models.

For interactive data reads (the SQL grid, probes), the cockpit attaches the workspace's
DuckLake catalog **read-only** and reads parquet from S3 directly via DuckDB.

!!! info "Cross-package contracts"
    With no codegen, two contracts are maintained by hand on both sides: the **Temporal
    workflow signatures** (the cockpit mirrors the engine's `worker/contracts.py` in
    TypeScript) and the **concept overlay payload** the cockpit writes for the engine to
    ground against ([ADR-0007](../adr/0007-frame-frozen-artifact-contract.md)). Changing
    either is a coordinated edit across both packages.

## The per-workspace model

A **workspace** is the unit of isolation. Everything that belongs to one workspace is
namespaced and never shared with another:

```mermaid
flowchart TB
    subgraph WS["workspace &lt;id&gt;"]
        direction LR
        C["1 engine container"]
        CC["1 cockpit container"]
        SD["1 subdomain<br/>&lt;sub&gt;.&lt;domain&gt;"]
        Q["2 Temporal task queues<br/>engine-&lt;id&gt; · cockpit-&lt;id&gt;"]
        SCH["1 Postgres schema<br/>ws_&lt;id&gt;"]
        CAT["1 DuckLake catalog schema<br/>in the shared catalog DB"]
        LAKE["1 S3 prefix<br/>s3://bucket/&lt;id&gt;/"]
    end
```

Each workspace runs its **own engine container** and its **own cockpit container**
(DD/51740673). The engine bootstraps that workspace's connection manager and DuckLake
anchor at startup and polls exactly its own queue (`engine-<id>`); the cockpit boots with
a single workspace identity (`DATARAUM_WORKSPACE_ID`) and never resolves the workspace
per request — its activity worker polls `cockpit-<id>`, and every `cockpit_db` query is
scoped to the boot workspace. Adding a workspace is, mechanically, a registry row plus
one engine + one cockpit service plus a Caddy route. See
[Deployment](../operations/deployment.md) for how this maps to running containers and to
the cloud.

## Ingress and identity

Caddy is the installation's one ingress: each workspace's cockpit is served on its own
subdomain (`ws1.<domain>`), and the bare parent domain serves the **portal** — the same
cockpit image in a second role (`DATARAUM_PORTAL_MODE=1`) that owns login and membership
routing. Caddy routes are `@id`-tagged and managed through its admin API
(`src/portal/caddy.ts`), which is the seam the provisioner (DAT-820) uses to make a
workspace reachable.

Identity is **better-auth**, self-hosted in `cockpit_db` (its `user` table *is* the
`users` table; `memberships` FKs onto it). The portal issues the session cookie on the
parent domain; every workspace cockpit verifies it locally against the shared session
rows and enforces **membership of its own workspace** in global request middleware — an
authenticated non-member is bounced back to the portal. See
[ADR-0022](../adr/0022-portal-auth-better-auth.md).

## The containers, concretely

A default local stack is:

| Container | What it is |
|---|---|
| `postgres` | the one Postgres instance (all databases above) |
| `seaweedfs` (+ `seaweedfs-init`) | the S3 object store + one-shot bucket creation |
| `temporal` (+ `temporal-admin-tools`, `temporal-create-namespace`, `temporal-ui`) | the Temporal server, schema setup, namespace registration, and web UI |
| `engine-worker` | the engine analysis worker for workspace 1 (one per workspace) |
| `cockpit` (+ `cockpit-migrate`) | workspace 1's web app + its co-located activity-only worker; migrations run once first |
| `caddy` | the ingress: per-workspace subdomains + the portal's parent domain (`caddy/caddy.json`; admin API = the provisioner seam) |
| `portal` | the cockpit image in portal mode — login + membership routing on the parent domain |

That is the complete installation, and it defines exactly **one** workspace pair — the
bootstrap workspace. Compose grows no further per workspace: the provisioner clones this
pair's recorded container config for every workspace created through the portal.

The engine has **no healthcheck port** — its health is the Temporal worker heartbeat, not
an HTTP probe. See [Running the stack](../getting-started/running-the-stack.md) to bring
this up and [Deployment](../operations/deployment.md) for the full topology and the
per-workspace template.

## Next

- [Decision records](../adr/README.md) — the settled architecture decisions and the *why*
  behind each one.
- [How it works](../concepts/the-journey.md) — the journey and the model behind it.
