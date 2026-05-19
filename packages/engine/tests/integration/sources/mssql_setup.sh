#!/usr/bin/env bash
#
# Restore AdventureWorksLT and create a read-only login for DataRaum's
# MSSQL integration test. Idempotent — re-running is a no-op.
#
# Prerequisite: a running SQL Server 2022+ container. On macOS with
# Apple's `container` CLI, one way to bring it up is:
#
#   container run \
#     -e ACCEPT_EULA=Y \
#     -e MSSQL_SA_PASSWORD=Test1234 \
#     -e MSSQL_PID=Evaluation \
#     -p 1433:1433 \
#     --name sql2025 \
#     --memory 3g \
#     --platform linux/amd64 \
#     -d mcr.microsoft.com/mssql/server:2025-latest
#
# With Docker Desktop, replace `container` with `docker` throughout.
#
# After this script completes, run the integration test with the URL
# printed at the end.
#
# Knobs (override with env vars before invocation):
#
#   CONTAINER         container name                       (default: sql2025)
#   SA_PASSWORD       sa password set at container launch  (default: Test1234)
#   READER_USER       db reader login to create            (default: dataraum_reader)
#   READER_PASSWORD   db reader password                   (default: ReadOnly!2026)

set -euo pipefail

CONTAINER="${CONTAINER:-sql2025}"
SA_PASSWORD="${SA_PASSWORD:-Test1234}"
READER_USER="${READER_USER:-dataraum_reader}"
READER_PASSWORD="${READER_PASSWORD:-ReadOnly!2026}"

DB_NAME="AdventureWorksLT"
BAK_URL="https://github.com/microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksLT2025.bak"
BAK_PATH_IN_CONTAINER="/var/opt/mssql/backup/AdventureWorksLT2025.bak"

# Logical filenames inside AdventureWorksLT2025.bak. Microsoft did not rename
# them when packaging the 2025 backup — they retained the 2022-era names.
# If RESTORE fails with "logical file 'X' does not match a file", re-run
# `RESTORE FILELISTONLY FROM DISK = '...'` and update these.
BAK_LOGICAL_DATA="AdventureWorksLT2022_Data"
BAK_LOGICAL_LOG="AdventureWorksLT2022_Log"

# Choose container CLI: prefer Apple `container`, fall back to `docker`.
if command -v container >/dev/null 2>&1; then
  CONTAINER_CLI="container"
elif command -v docker >/dev/null 2>&1; then
  CONTAINER_CLI="docker"
else
  echo "ERROR: neither 'container' nor 'docker' on PATH." >&2
  exit 1
fi

sa_sqlcmd() {
  "$CONTAINER_CLI" exec "$CONTAINER" /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$SA_PASSWORD" -C -b "$@"
}

reader_sqlcmd() {
  "$CONTAINER_CLI" exec "$CONTAINER" /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U "$READER_USER" -P "$READER_PASSWORD" -C -b "$@"
}

echo "[1/5] Waiting for SQL Server in container '$CONTAINER'..."
for _ in $(seq 1 60); do
  if sa_sqlcmd -Q "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
sa_sqlcmd -Q "SELECT 1" >/dev/null

if sa_sqlcmd -h -1 -W -Q "SET NOCOUNT ON; SELECT name FROM sys.databases WHERE name = '$DB_NAME'" \
     2>/dev/null | grep -q "^${DB_NAME}\$"; then
  echo "[2/5] $DB_NAME already exists — skipping restore."
else
  echo "[2/5] Downloading AdventureWorksLT2025.bak inside container (~1.8 MB)..."
  "$CONTAINER_CLI" exec "$CONTAINER" sh -c \
    "mkdir -p /var/opt/mssql/backup && wget -q -O '$BAK_PATH_IN_CONTAINER' '$BAK_URL'"

  echo "[3/5] Restoring $DB_NAME..."
  sa_sqlcmd -Q "
    RESTORE DATABASE [$DB_NAME] FROM DISK = '$BAK_PATH_IN_CONTAINER'
    WITH
      MOVE '$BAK_LOGICAL_DATA' TO '/var/opt/mssql/data/${DB_NAME}.mdf',
      MOVE '$BAK_LOGICAL_LOG'  TO '/var/opt/mssql/data/${DB_NAME}_log.ldf',
      REPLACE
  "
fi

echo "[4/5] Creating read-only login '$READER_USER'..."
sa_sqlcmd -Q "
  IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '$READER_USER')
    CREATE LOGIN [$READER_USER] WITH PASSWORD = '$READER_PASSWORD', CHECK_POLICY = OFF;
  USE [$DB_NAME];
  IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '$READER_USER')
    CREATE USER [$READER_USER] FOR LOGIN [$READER_USER];
  ALTER ROLE db_datareader ADD MEMBER [$READER_USER];
"

echo "[5/5] Verifying reader access..."
reader_sqlcmd -d "$DB_NAME" -Q \
  "SELECT TOP 1 CustomerID, FirstName, LastName FROM SalesLT.Customer ORDER BY CustomerID" \
  >/dev/null

# Best-effort detection of the container's address. Apple `container` exposes
# it via inspect; Docker uses NetworkSettings.IPAddress. If detection fails
# (because the user runs Docker Desktop with port mapping to 127.0.0.1, for
# instance), print a placeholder for the user to fill in.
CONTAINER_HOST=""
if [[ "$CONTAINER_CLI" == "container" ]]; then
  CONTAINER_HOST=$("$CONTAINER_CLI" ls 2>/dev/null \
    | awk -v c="$CONTAINER" '$1 == c { for (i=1; i<=NF; i++) if ($i ~ /^[0-9]+\./) { split($i, a, "/"); print a[1]; exit } }')
fi
CONTAINER_HOST="${CONTAINER_HOST:-<container-ip-or-127.0.0.1>}"

cat <<EOF

Done. $DB_NAME is ready on container '$CONTAINER' with read-only login '$READER_USER'.

Export and run the integration test:

  export DATARAUM_MSSQL_TEST_URL='mssql://${READER_USER}:${READER_PASSWORD}@${CONTAINER_HOST}:1433/${DB_NAME}?TrustServerCertificate=yes'
  uv run pytest tests/integration/sources/test_db_recipe_mssql.py -v
EOF
