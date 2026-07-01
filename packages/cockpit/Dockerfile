# DataRaum cockpit — TanStack Start production image.
#
# Multi-stage:
#   1.  deps      — install bun deps (cached when package.json/bun.lock unchanged)
#   1b. deps-prod — production-only deps for the runner (worker runtime tree)
#   2.  build     — vite build → Nitro emits a self-binding bundle at
#                   .output/server/index.mjs. The cockpit reads engine metadata
#                   via Drizzle (src/db/metadata/) and drives work as Temporal
#                   workflows — the engine has NO HTTP surface (DAT-339 pivot).
#   3.  runner    — minimal bun image serving .output/server/index.mjs
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

# ---------- 1b. deps-prod ----------
# Production-only node_modules for the runner. The orchestration worker (DAT-529)
# is co-located in the server process, and @temporalio/* is externalized from the
# bundle (native Rust core-bridge — can't be bundled), so the runner must carry
# the worker's runtime dependency tree (worker + core-bridge + common + proto +
# nexus-rpc + grpc + protobufjs + …). That tree is too tangled to hand-pick, so
# install the whole prod set here (`--production` drops devDependencies). This
# also provides @duckdb, so the runner needs no per-package COPY.
FROM oven/bun:1 AS deps-prod
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

# ---------- 2. build ----------
FROM oven/bun:1 AS build
WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# The engine's metadata schema arrives via Drizzle introspection
# (`bun run db:pull:metadata` writes src/db/metadata/, checked in).
RUN bun run build

# ---------- 3. runner ----------
FROM oven/bun:1-slim AS runner
WORKDIR /app
ENV NODE_ENV=production \
    PORT=3000 \
    HOSTNAME=0.0.0.0

# oven/bun:1-slim ships without wget; add it for the healthcheck.
# libgssapi-krb5-2: runtime dependency of the community mssql DuckDB extension
# (probe backend) — without it LOAD mssql fails with a missing
# libgssapi_krb5.so.2, both at the pre-bake below and at runtime.
RUN apt-get update && apt-get install -y wget libgssapi-krb5-2 \
    && rm -rf /var/lib/apt/lists/*

# Nitro produces a self-contained server bundle at .output/server/index.mjs.
COPY --from=build /app/.output ./.output

# The prod-only node_modules: carries every dependency externalized from the
# bundle and resolved at runtime — @duckdb/node-bindings-* (native `.node`)
# and the @temporalio worker tree (native
# core-bridge + its transitive runtime deps). The workflow code is pre-bundled at
# build time (scripts/build-workflow-bundle.ts) into the server bundle, so the
# webpack/swc workflow bundler is never invoked at runtime even though it ships
# here as a worker dependency.
COPY --from=deps-prod /app/node_modules ./node_modules

# Pre-install the DuckDB extensions at image build time (mirror of the engine's
# worker.Dockerfile): ducklake + httpfs for the READ_ONLY lake reader / S3
# secret, plus the connect-probe backends (postgres/mysql/sqlite + community
# mssql). Runtime sets DUCKLAKE_SKIP_INSTALL=1 (read via config.ts by lake.ts /
# s3-secret.ts / probe.ts) so neither cold start nor a probe ever hits
# extensions.duckdb.org — required behind egress proxy filters and in
# air-gapped deploys. `SET extension_directory` lands the cache at a known
# image path; the matching env var keeps the runtime LOAD aligned with it.
RUN bun -e "const { DuckDBInstance } = await import('@duckdb/node-api'); \
    const inst = await DuckDBInstance.create(':memory:'); \
    const conn = await inst.connect(); \
    await conn.run(\"SET extension_directory = '/opt/dataraum/duckdb-extensions'\"); \
    for (const ext of ['ducklake', 'httpfs', 'postgres', 'mysql', 'sqlite']) { \
      await conn.run('INSTALL ' + ext); \
      await conn.run('LOAD ' + ext); \
    } \
    await conn.run('INSTALL mssql FROM community'); \
    await conn.run('LOAD mssql');"

ENV DUCKDB_EXTENSION_DIRECTORY=/opt/dataraum/duckdb-extensions \
    DUCKLAKE_SKIP_INSTALL=1

EXPOSE 3000

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- --spider http://localhost:3000/ || exit 1

CMD ["bun", "run", ".output/server/index.mjs"]
