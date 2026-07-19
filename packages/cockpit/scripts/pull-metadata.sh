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
# Workspace-neutral by design (DAT-816): at runtime the reader ROLE's
# search_path resolves which ws_<id>_read schema a connection sees, so the
# mirror must carry no workspace literal. The read views are materialized into
# the scratch DB's `public` schema — drizzle then emits plain unqualified
# pgView() exports — and the raw tables into a fixed `engine` schema, which is
# what the introspected view bodies reference in place of ws_<id>.
set -euo pipefail

cd "$(dirname "$0")/.." # packages/cockpit

RAW_SCHEMA="engine"
PG_IMAGE="postgres:19beta1" # keep in lockstep with packages/infra/docker-compose.yml
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

# 3. Materialize the raw schema from the dump (psql via the container — no host
#    psql), then the promoted-read surface (ADR-0008/DAT-453): schema_read.sql
#    is tokenized (__WS__ / __READ__); substitute and apply. The read views land
#    in `public` — what drizzle introspects below, the cockpit's whole metadata
#    surface — so the pull emits unqualified tables (zero ws_ literals, DAT-816).
docker exec "$CONTAINER" psql -U postgres -d scratch -v ON_ERROR_STOP=1 \
    -c "CREATE SCHEMA $RAW_SCHEMA" >/dev/null
(
    echo "SET search_path TO $RAW_SCHEMA;"
    cat ../engine/schema.sql
    sed -e "s/__READ__/public/g" -e "s/__WS__/$RAW_SCHEMA/g" ../engine/schema_read.sql
) | docker exec -i "$CONTAINER" psql -U postgres -d scratch -v ON_ERROR_STOP=1 -f - >/dev/null
echo "→ materialized $RAW_SCHEMA (raw) + public (read views) in scratch Postgres"

# 4. Pull + normalize + lint.
export METADATA_DATABASE_URL="postgresql://postgres:scratch@localhost:$PG_PORT/scratch"
bun --bun drizzle-kit pull --config drizzle.config.metadata.ts
bun run scripts/normalize-metadata-pull.mjs
bun --bun biome check --write --unsafe src/db/metadata/
echo "→ src/db/metadata/ refreshed"
