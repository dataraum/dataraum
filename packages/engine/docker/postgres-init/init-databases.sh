#!/usr/bin/env bash
# Postgres first-boot init: create the DuckLake catalog database + the
# cockpit_db database alongside the primary platform database. The primary
# db (`$POSTGRES_DB`) is created by the official postgres image; this
# script adds `$DUCKLAKE_CATALOG_DB` and `$COCKPIT_DB`.
set -euo pipefail

: "${DUCKLAKE_CATALOG_DB:?DUCKLAKE_CATALOG_DB must be set on the postgres service}"
: "${COCKPIT_DB:?COCKPIT_DB must be set on the postgres service}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE "$DUCKLAKE_CATALOG_DB";
    CREATE DATABASE "$COCKPIT_DB";
EOSQL
