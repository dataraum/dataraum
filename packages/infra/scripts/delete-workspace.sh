#!/usr/bin/env bash
# Workspace deletion sweep (DAT-505 AC5).
#
# A workspace = unit of isolation across FIVE substrates, so deleting one is a
# fixed sweep across all five, in dependency order (stop producing/consuming
# before dropping state, drop data before the registry row):
#
#   1. STOP the workspace's engine container  (no more writes to its ws_<id> /
#      catalog / s3 prefix while we drop them).
#   2. DROP the engine Postgres schemas        ws_<id> + ws_<id>_read.
#   3. DROP the workspace's DuckLake catalog SCHEMA (ws_<id>) from the
#      installation-wide catalog database (DAT-815 — the database is shared by
#      every workspace and is never dropped here).
#   4. DELETE the workspace's S3 prefix         s3://<bucket>/<ws>/  (lake + uploads).
#   5. DELETE the cockpit_db control-plane rows  (session_runs → sessions →
#      conversations/ui_state → the workspace row) OR, for a soft delete, stamp
#      workspaces.archived_at and stop (steps 2–4 + the registry row stay).
#
# Clean cut, dev posture: no migration tooling, no graceful drain — a dev
# workspace is re-provisioned, not migrated. Destructive: it DROPs schemas and
# DELETEs an S3 prefix. Run it deliberately.
#
# Usage:
#   packages/infra/scripts/delete-workspace.sh <workspace_id> [--soft]
#
# Env (defaults match docker-compose.yml / .env.example):
#   POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB
#   DUCKLAKE_CATALOG_DB   the ONE installation-wide catalog database (DAT-815)
#   COCKPIT_DB            cockpit control-plane database
#   S3_BUCKET             the lake bucket
#   PGHOST / PGPORT       Postgres host/port (default localhost:5432)
#   SEAWEEDFS_MASTER / SEAWEEDFS_FILER  for the S3 prefix delete (weed shell)
#
# --soft : skip the hard cockpit_db row deletes; stamp archived_at instead
#          (steps 2–4 still run — the data is gone, the registry remembers it).
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "usage: $0 <workspace_id> [--soft]" >&2
  exit 2
fi

WS_ID="$1"
shift
SOFT=0
CATALOG_DB="${DUCKLAKE_CATALOG_DB:-dataraum_lake_catalog}"
while [ $# -gt 0 ]; do
  case "$1" in
    --soft) SOFT=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${POSTGRES_USER:-dataraum}"
export PGPASSWORD="${POSTGRES_PASSWORD:-dataraum}"
PRIMARY_DB="${POSTGRES_DB:-dataraum}"
COCKPIT_DB="${COCKPIT_DB:-cockpit_db}"
S3_BUCKET="${S3_BUCKET:-dataraum-lake}"
SEAWEEDFS_MASTER="${SEAWEEDFS_MASTER:-seaweedfs:9333}"
SEAWEEDFS_FILER="${SEAWEEDFS_FILER:-seaweedfs:8888}"

# Schema name mirrors server/workspace.py::schema_name_for (dashes → _). The
# SAME ws_<id> name identifies the workspace in the primary DB (engine
# metadata) and in the catalog DB (DuckLake METADATA_SCHEMA, DAT-815).
SCHEMA="ws_${WS_ID//-/_}"

echo "==> Deleting workspace ${WS_ID} (schema ${SCHEMA}, catalog db ${CATALOG_DB})"

# 1. Stop the workspace's engine container (best-effort; it may not be named
#    deterministically in every deployment — in dev it is engine-worker[-N]).
echo "==> [1/5] stop the engine container (do this in your orchestrator; dev:"
echo "        docker compose -f packages/infra/docker-compose.yml stop engine-worker)"

psql_primary() { psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PRIMARY_DB" "$@"; }

# 2. Drop the engine Postgres schemas (raw + the promoted-read schema).
echo "==> [2/5] drop schemas ${SCHEMA}, ${SCHEMA}_read"
psql_primary -c "DROP SCHEMA IF EXISTS \"${SCHEMA}\" CASCADE;"
psql_primary -c "DROP SCHEMA IF EXISTS \"${SCHEMA}_read\" CASCADE;"

# 3. Drop the workspace's DuckLake catalog schema from the shared catalog DB
#    (DAT-815). No pg_terminate_backend sweep here: the catalog database is
#    shared by EVERY workspace, so terminating its backends would kill sibling
#    workspaces' live catalog pools. The stopped worker (step 1) holds no locks
#    on this schema; a still-running cockpit for THIS workspace normally doesn't
#    either (its reader pool idles outside transactions), but a straggling
#    in-flight read could — so a lock_timeout makes the DROP fail loud after 30s
#    (stop the workspace's cockpit, re-run) instead of hanging on the lock.
echo "==> [3/5] drop DuckLake catalog schema ${SCHEMA} in ${CATALOG_DB}"
psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$CATALOG_DB" \
  -c "SET lock_timeout = '30s'; DROP SCHEMA IF EXISTS \"${SCHEMA}\" CASCADE;"

# 4. Delete the workspace's S3 prefix (lake + uploads) via weed shell. The
#    fs.rm -r removes everything under the workspace's <ws>/ prefix in the bucket.
echo "==> [4/5] delete S3 prefix s3://${S3_BUCKET}/${WS_ID}/"
if command -v weed >/dev/null 2>&1; then
  echo "fs.rm -r /buckets/${S3_BUCKET}/${WS_ID}" \
    | weed shell -master="$SEAWEEDFS_MASTER" -filer="$SEAWEEDFS_FILER" || true
else
  echo "    (weed CLI not on PATH — run inside the seaweedfs container:"
  echo "     docker compose exec seaweedfs sh -c \"echo 'fs.rm -r /buckets/${S3_BUCKET}/${WS_ID}' | weed shell\")"
fi

# 5. Cockpit_db control plane: hard delete rows OR soft archive.
if [ "$SOFT" -eq 1 ]; then
  echo "==> [5/5] SOFT delete — stamp workspaces.archived_at"
  psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$COCKPIT_DB" \
    -c "UPDATE workspaces SET archived_at = now() WHERE id = '${WS_ID}';"
else
  echo "==> [5/5] hard delete cockpit_db rows (session_runs → sessions → conversations → workspace)"
  psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$COCKPIT_DB" <<-SQL
    DELETE FROM session_runs WHERE session_id IN (
      SELECT id FROM sessions WHERE workspace_id = '${WS_ID}'
    );
    DELETE FROM sessions WHERE workspace_id = '${WS_ID}';
    DELETE FROM conversation_messages WHERE conversation_id IN (
      SELECT id FROM conversations WHERE workspace_id = '${WS_ID}'
    );
    DELETE FROM ui_state WHERE conversation_id IN (
      SELECT id FROM conversations WHERE workspace_id = '${WS_ID}'
    );
    DELETE FROM conversations WHERE workspace_id = '${WS_ID}';
    DELETE FROM workspaces WHERE id = '${WS_ID}';
SQL
fi

echo "==> Workspace ${WS_ID} deletion sweep complete."
