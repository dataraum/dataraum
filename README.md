# DataRaum

[![License](https://img.shields.io/github/license/dataraum/dataraum)](LICENSE)

A rich metadata context engine for AI-driven data analytics.

Traditional semantic layers tell BI tools "what things are called." DataRaum tells AI "what the data means, how it behaves, how it relates, and what you can compute from it."

## Monorepo layout

```
packages/
├── engine/     # Python — pipeline, detectors, Starlette kernel shell
├── cockpit/    # TypeScript — TanStack Start web UI
└── infra/      # docker-compose orchestration
```

Each package has its own README. Start there if you're working in a specific package.

## Status — transitioning to v1

DataRaum is mid-pivot. v0.2.x exposed a 12-tool MCP server over HTTP. **That transport is gone.** v1 is a 3-verb kernel + cockpit:

- **engine** — Python pipeline + Starlette kernel shell exposing three verbs over HTTP: `/measure` (SSE), `/query` (Arrow), `/probe` (read-only SQL), plus `/health`.
- **cockpit** — TanStack Start app that hosts the chat surface, renders the agentic widgets, and reads the engine's metadata schema directly via Drizzle (introspected per workspace).

Today the substrate boots and you can poke `/health`. The 3 kernel verbs are 501 stubs and get filled in phase-by-phase per the DAT-339 pivot. **No end-user surface yet** — if you need v0.2.x MCP behavior, pin `dataraum==0.2.2`.

## Quick start

```bash
# Set the LLM key
cp packages/infra/.env.example packages/infra/.env
echo "ANTHROPIC_API_KEY=sk-ant-..." >> packages/infra/.env

# Bring up Postgres + control plane + cockpit
docker compose -f packages/infra/docker-compose.yml up -d --wait

# Verify the substrate
curl -fsS http://localhost:8000/health

# Open the cockpit
open http://localhost:3000
```

For UI iteration, run the cockpit dev server outside docker for hot reload — see `packages/cockpit/README.md`.

## Develop

- **Engine (Python):** `cd packages/engine && uv sync --group dev && uv run pytest --testmon tests/unit -q`. See `packages/engine/README.md` and `packages/engine/CLAUDE.md`.
- **Cockpit (TypeScript):** `cd packages/cockpit && pnpm install && pnpm dev`. See `packages/cockpit/README.md` and `packages/cockpit/CLAUDE.md`.
- **Pull the engine metadata schema (cockpit):** `cd packages/cockpit && DATARAUM_WORKSPACE_ID=<id> METADATA_DATABASE_URL=<url> pnpm db:pull:metadata`. Re-run after the engine adds/changes SQLAlchemy models.

## Documentation

User-facing docs live in `packages/engine/docs/` and are published via Zensical.

- [Architecture](packages/engine/docs/architecture.md)
- [Pipeline](packages/engine/docs/pipeline.md)
- [Entropy](packages/engine/docs/entropy.md)
- [Data Model](packages/engine/docs/data-model.md)
- [Configuration](packages/engine/docs/configuration.md)

## License

MIT — see [LICENSE](LICENSE).
