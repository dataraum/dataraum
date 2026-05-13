# Platform — Reference Artifacts

Design docs live in Confluence (Dataraum Design space):

- **Control Plane: Sessions, Metadata, Governance** — control plane interfaces, session model, governance, config-as-data
- **Data Plane: Executors, DuckLake & the RPC Contract** — executor lifecycle, gRPC contract, DuckLake, resource limits
- **Observability: OpenTelemetry-First** — OTel SDK, local LGTM stack, SLIs/SLOs
- **Web UI as Conversation-Layer Client** — frontend container, Caddy front door, query governance gate, agentic chat
  - Child: **Web UI: Tech Stack** — React + Vite + TanStack Router + shadcn + Vercel AI SDK; Python BFF inside the control plane

This directory contains reference artifacts the Confluence docs refer to.

## Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | Reference single-host deployment topology. Not wired to real images yet — the image tags `dataraum/control-plane`, `dataraum/executor`, `dataraum/frontend` are placeholders. Used as a concrete shape for design conversation and as the target for the decompose phase. |

## Status

**Design-stage.** Nothing here is built. The compose file is intentional shape (services, volumes, networks, limits, healthchecks) without the binaries to back it. Decompose will produce the Jira phases to make it run.
