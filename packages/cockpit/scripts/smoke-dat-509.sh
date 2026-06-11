#!/usr/bin/env bash
# Lane smoke for DAT-509 — self-contained: scratch Postgres + the engine's
# materialized ws/_read schemas (same recipe as pull-metadata.sh), then
# scripts/smoke-dat-509.ts seeds multi-grain rows and asserts the changed
# tools' reads through the REAL views. No running stack, no LLM calls
# (ANTHROPIC_API_KEY is a dummy; the asserted paths never synthesize).
#
# Requires docker + uv + bun. Run from packages/cockpit:
#   bash scripts/smoke-dat-509.sh
set -euo pipefail

cd "$(dirname "$0")/.." # packages/cockpit

WORKSPACE_ID="${DATARAUM_WORKSPACE_ID:-00000000-0000-0000-0000-000000000001}"
SCHEMA_NAME="ws_${WORKSPACE_ID//-/_}"
READ_SCHEMA="${SCHEMA_NAME}_read"
PG_IMAGE="postgres:17"
CONTAINER="dataraum-smoke-509-$$"

trap 'docker rm -f "$CONTAINER" >/dev/null 2>&1 || true' EXIT
docker run --rm -d --name "$CONTAINER" \
    -e POSTGRES_PASSWORD=scratch -e POSTGRES_DB=scratch \
    -p "127.0.0.1::5432" "$PG_IMAGE" >/dev/null
PG_PORT=$(docker port "$CONTAINER" 5432/tcp | head -1 | awk -F: '{print $NF}')
for _ in $(seq 1 100); do
    docker exec "$CONTAINER" pg_isready -h 127.0.0.1 -U postgres -q 2>/dev/null && break
    sleep 0.3
done
docker exec "$CONTAINER" pg_isready -h 127.0.0.1 -U postgres -q

docker exec "$CONTAINER" psql -U postgres -d scratch -v ON_ERROR_STOP=1 \
    -c "CREATE SCHEMA $SCHEMA_NAME" -c "CREATE SCHEMA $READ_SCHEMA" >/dev/null
(
    echo "SET search_path TO $SCHEMA_NAME;"
    cat ../engine/schema.sql
    sed -e "s/__READ__/$READ_SCHEMA/g" -e "s/__WS__/$SCHEMA_NAME/g" ../engine/schema_read.sql
) | docker exec -i "$CONTAINER" psql -U postgres -d scratch -v ON_ERROR_STOP=1 -f - >/dev/null
echo "→ materialized $SCHEMA_NAME + $READ_SCHEMA"

export DATARAUM_WORKSPACE_ID="$WORKSPACE_ID"
export METADATA_DATABASE_URL="postgresql://postgres:scratch@localhost:$PG_PORT/scratch"
# Required-but-unused by the smoke's read paths — dummies, validated only.
export COCKPIT_DATABASE_URL="postgresql://postgres:scratch@localhost:$PG_PORT/scratch"
export DATARAUM_CONFIG_PATH="/tmp/dataraum-config-unused"
export DATARAUM_LAKE_PATH="/tmp/dataraum-lake-unused"
export DUCKLAKE_CATALOG_URL="postgresql://postgres:scratch@localhost:$PG_PORT/scratch"
export ANTHROPIC_API_KEY="smoke-dummy-never-called"
export S3_ENDPOINT="localhost:9999"
export S3_ACCESS_KEY_ID="smoke" S3_SECRET_ACCESS_KEY="smoke" S3_BUCKET="smoke"
export S3_USE_SSL="false" DUCKLAKE_SKIP_INSTALL="1"

bun run scripts/smoke-dat-509.ts
