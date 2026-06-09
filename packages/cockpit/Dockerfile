# DataRaum cockpit — TanStack Start production image.
#
# Multi-stage:
#   1. deps   — install bun deps (cached when package.json/bun.lock unchanged)
#   2. build  — vite build → Nitro emits a self-binding bundle at
#               .output/server/index.mjs. The cockpit reads engine metadata
#               via Drizzle (src/db/metadata/) and drives work as Temporal
#               workflows — the engine has NO HTTP surface (DAT-339 pivot).
#   3. runner — minimal bun image serving .output/server/index.mjs
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

# …except DuckDB's native binding: `@duckdb/node-bindings-*` is externalized
# from the bundle (vite.config.ts — the `.node` binary can't be bundled), so the
# matching platform package must be present in node_modules at runtime. These
# packages are pure binary (no transitive deps); `bun` resolves the runtime
# `require('@duckdb/node-bindings-<plat>/duckdb.node')` from /app/node_modules.
# The deps stage installs the binding for THIS image's platform/libc (both
# stages are Debian-based oven/bun images), so the binary loads in the runner.
COPY --from=deps /app/node_modules/@duckdb ./node_modules/@duckdb

# Likewise @polyglot-sql/sdk (DAT-485): externalized from the bundle (vite.config)
# because its WASM parser readFileSync's a sibling `polyglot_sql.wasm` that the
# bundler doesn't carry. Copy the package (JS + the .wasm) so the runtime import
# resolves from node_modules with its WASM intact.
COPY --from=deps /app/node_modules/@polyglot-sql ./node_modules/@polyglot-sql

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
