#!/usr/bin/env bash
# Self-contained drizzle metadata regen — no running stack, no engine boot.
#
# Chain: SQLAlchemy models → offline DDL dump (packages/engine/schema.sql)
#        → ephemeral scratch Postgres → drizzle-kit pull → normalize → biome.
#
# The scratch schema is created fresh on every run, so stale-volume drift
# (create_all is additive) can never leak into the pulled mirror. Requires
# docker + uv + bun on PATH; nothing else.
#
# Env (all optional):
#   DATARAUM_WORKSPACE_ID   workspace id baked into pgSchema() — defaults to
#                           the bootstrap id, matching infra/.env.example.
set -euo pipefail

cd "$(dirname "$0")/.." # packages/cockpit

WORKSPACE_ID="${DATARAUM_WORKSPACE_ID:-00000000-0000-0000-0000-000000000001}"
SCHEMA_NAME="ws_${WORKSPACE_ID//-/_}"
PG_IMAGE="postgres:17" # keep in lockstep with packages/infra/docker-compose.yml
# Unique per run (parallel lanes regen concurrently); port is docker-assigned.
CONTAINER="dataraum-schema-scratch-$$"

# 1. Regenerate the offline DDL dump from the SQLAlchemy models (no DB needed).
uv run --locked --directory ../engine python -m dataraum.storage.dump_ddl schema.sql
echo "→ regenerated packages/engine/schema.sql"

# 2. Ephemeral scratch Postgres.
trap 'docker rm -f "$CONTAINER" >/dev/null 2>&1 || true' EXIT
docker run --rm -d --name "$CONTAINER" \
    -e POSTGRES_PASSWORD=scratch -e POSTGRES_DB=scratch \
    -p "127.0.0.1::5432" "$PG_IMAGE" >/dev/null
PG_PORT=$(docker port "$CONTAINER" 5432/tcp | head -1 | awk -F: '{print $NF}')
# TCP probe on purpose: the entrypoint's init-phase server answers the unix
# socket but runs with listen_addresses='' — only the real server passes this.
for _ in $(seq 1 100); do
    docker exec "$CONTAINER" pg_isready -h 127.0.0.1 -U postgres -q 2>/dev/null && break
    sleep 0.3
done
docker exec "$CONTAINER" pg_isready -h 127.0.0.1 -U postgres -q || {
    echo "scratch Postgres never became ready:" >&2
    docker logs "$CONTAINER" >&2
    exit 1
}

# 3. Materialize the schema from the dump (psql via the container — no host psql).
docker exec "$CONTAINER" psql -U postgres -d scratch -v ON_ERROR_STOP=1 \
    -c "CREATE SCHEMA $SCHEMA_NAME" >/dev/null
(
    echo "SET search_path TO $SCHEMA_NAME;"
    cat ../engine/schema.sql
) | docker exec -i "$CONTAINER" psql -U postgres -d scratch -v ON_ERROR_STOP=1 -f - >/dev/null
echo "→ materialized $SCHEMA_NAME in scratch Postgres"

# 4. Pull + normalize + lint.
export DATARAUM_WORKSPACE_ID="$WORKSPACE_ID"
export METADATA_DATABASE_URL="postgresql://postgres:scratch@localhost:$PG_PORT/scratch"
bun --bun drizzle-kit pull --config drizzle.config.metadata.ts
bun run scripts/normalize-metadata-pull.mjs
bun --bun biome check --write --unsafe src/db/metadata/
echo "→ src/db/metadata/ refreshed"
