# DataRaum cockpit — TanStack Start production image.
#
# Multi-stage:
#   1. deps   — install bun deps (cached when package.json/bun.lock unchanged)
#   2. build  — vite build → Nitro emits a self-binding bundle at
#               .output/server/index.mjs. The cockpit consumes engine
#               metadata via Drizzle introspection (src/db/metadata/) and
#               the engine kernel verbs over HTTP — no OpenAPI / codegen
#               (retired in the DAT-339 pivot, Phase 0c).
#   3. runner — minimal bun image serving .output/server/index.mjs
#
# Build args:
#   VITE_ENGINE_API_URL — baked into the client bundle for cross-origin
#     fetches to the engine. Defaults to http://localhost:8000 for the
#     standard docker-compose host-port mapping.
#
# Runtime env:
#   PORT — server bind port (default 3000)
#
# Build via the workspace compose file:
#   docker compose -f packages/infra/docker-compose.yml build cockpit

# ---------- 1. deps ----------
FROM oven/bun:1 AS deps
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

# ---------- 2. build ----------
FROM oven/bun:1 AS build
WORKDIR /app

ARG VITE_ENGINE_API_URL=http://localhost:8000
ENV VITE_ENGINE_API_URL=$VITE_ENGINE_API_URL

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Cockpit consumes the engine's metadata schema via Drizzle introspection
# (`bun run db:pull:metadata` writes src/db/metadata/), and the engine
# kernel verbs over HTTP. No OpenAPI / codegen anymore (DAT-339 pivot 0c).
RUN bun run build

# ---------- 3. runner ----------
FROM oven/bun:1-slim AS runner
WORKDIR /app
ENV NODE_ENV=production \
    PORT=3000 \
    HOSTNAME=0.0.0.0

# oven/bun:1-slim ships without wget; add it for the healthcheck.
RUN apt-get update && apt-get install -y wget \
    && rm -rf /var/lib/apt/lists/*

# Nitro produces a self-contained server bundle at .output/server/index.mjs.
COPY --from=build /app/.output ./.output

# …except DuckDB's native binding: `@duckdb/node-bindings-*` is externalized
# from the bundle (vite.config.ts — the `.node` binary can't be bundled), so the
# matching platform package must be present in node_modules at runtime. These
# packages are pure binary (no transitive deps); `bun` resolves the runtime
# `require('@duckdb/node-bindings-<plat>/duckdb.node')` from /app/node_modules.
# The deps stage installs the binding for THIS image's platform/libc (alpine =
# musl), which matches the runner — so the binary loads.
COPY --from=deps /app/node_modules/@duckdb ./node_modules/@duckdb

EXPOSE 3000

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- --spider http://localhost:3000/ || exit 1

CMD ["bun", "run", ".output/server/index.mjs"]
