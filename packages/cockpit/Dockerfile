# DataRaum cockpit — TanStack Start production image.
#
# Multi-stage:
#   1. deps   — install pnpm deps (cached when package.json/lock unchanged)
#   2. build  — vite build → Nitro emits a self-binding bundle at
#               .output/server/index.mjs. The cockpit consumes engine
#               metadata via Drizzle introspection (src/db/metadata/) and
#               the engine kernel verbs over HTTP — no OpenAPI / codegen
#               (retired in the DAT-339 pivot, Phase 0c).
#   3. runner — minimal node image serving .output/server/index.mjs
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
FROM node:26-alpine AS deps
WORKDIR /app
RUN corepack enable && corepack prepare pnpm@latest --activate

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile

# ---------- 2. build ----------
FROM node:26-alpine AS build
WORKDIR /app
RUN corepack enable && corepack prepare pnpm@latest --activate

ARG VITE_ENGINE_API_URL=http://localhost:8000
ENV VITE_ENGINE_API_URL=$VITE_ENGINE_API_URL

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Cockpit consumes the engine's metadata schema via Drizzle introspection
# (`pnpm db:pull:metadata` writes src/db/metadata/), and the engine kernel
# verbs over HTTP. No OpenAPI / codegen anymore (DAT-339 pivot Phase 0c).
RUN pnpm build

# ---------- 3. runner ----------
FROM node:26-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production \
    PORT=3000 \
    HOSTNAME=0.0.0.0

# Nitro produces a self-contained server bundle at .output/server/index.mjs
# — runtime needs nothing else from the build stage.
COPY --from=build /app/.output ./.output

EXPOSE 3000

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- --spider http://localhost:3000/ || exit 1

CMD ["node", ".output/server/index.mjs"]
