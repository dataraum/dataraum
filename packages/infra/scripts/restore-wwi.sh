#!/bin/bash
# Restore Wide World Importers into the `mssql` sample-source service (DAT-589,
# epic DAT-574). One-shot, mirroring the create-bucket.sh / create-namespace.sh
# idiom: wait until SQL Server accepts logins (the port opens BEFORE logins are
# ready), then RESTORE. Idempotent — skips if the DB already exists, so it is a
# no-op on every `up` after the first.
#
# The .bak is placed in the shared backup volume by the `mssql-wwi-fetch`
# one-shot; the RESTORE runs server-side (the `mssql` service reads the file from
# its own /var/opt/mssql/backup). Logical file names + the in-memory filegroup
# (`WWI_InMemory_Data_1`) were confirmed against the v1.0 backup.
set -eu

SQLCMD=/opt/mssql-tools18/bin/sqlcmd
SERVER=${MSSQL_HOST:-mssql}
SA_PASS=${MSSQL_SA_PASSWORD:?MSSQL_SA_PASSWORD required}
BAK=${WWI_BAK_PATH:-/var/opt/mssql/backup/WideWorldImporters-Full.bak}
DB=WideWorldImporters
MAX_ATTEMPTS=${MSSQL_INIT_MAX_ATTEMPTS:-60}
SLEEP_SECONDS=${MSSQL_INIT_SLEEP_SECONDS:-5}

# -C trusts the container's self-signed cert; -b makes sqlcmd exit non-zero on a
# SQL error so `set -e` catches a failed restore.
q() { "$SQLCMD" -S "$SERVER" -U sa -P "$SA_PASS" -C -b "$@"; }

echo "Waiting for SQL Server ($SERVER) to accept logins…"
attempt=1
until q -Q "SELECT 1" -o /dev/null 2>/dev/null; do
  if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
    echo "SQL Server did not accept logins after $MAX_ATTEMPTS attempts"; exit 1
  fi
  echo "  not ready yet (attempt $attempt/$MAX_ATTEMPTS)…"
  attempt=$((attempt + 1)); sleep "$SLEEP_SECONDS"
done
echo "SQL Server is accepting logins"

exists=$(q -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name='$DB'" | tr -d '[:space:]')
if [ "$exists" = "1" ]; then
  echo "$DB already restored — nothing to do"
  exit 0
fi

if [ ! -s "$BAK" ]; then
  echo "Backup not found at $BAK (the mssql-wwi-fetch one-shot should place it)"
  exit 1
fi

echo "Restoring $DB from $BAK …"
q -Q "RESTORE DATABASE [$DB] FROM DISK='$BAK' WITH \
  MOVE 'WWI_Primary'          TO '/var/opt/mssql/data/WideWorldImporters.mdf', \
  MOVE 'WWI_UserData'         TO '/var/opt/mssql/data/WideWorldImporters_UserData.ndf', \
  MOVE 'WWI_Log'              TO '/var/opt/mssql/data/WideWorldImporters.ldf', \
  MOVE 'WWI_InMemory_Data_1'  TO '/var/opt/mssql/data/WideWorldImporters_InMemory_Data_1', \
  REPLACE, RECOVERY"

echo "Restore complete:"
q -h -1 -Q "SET NOCOUNT ON; SELECT '  Sales.Orders rows = ' + CAST(COUNT(*) AS varchar) FROM $DB.Sales.Orders"
