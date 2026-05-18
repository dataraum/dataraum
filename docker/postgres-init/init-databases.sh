#!/usr/bin/env bash
# Postgres first-boot init: create the DuckLake catalog database alongside
# the primary platform database. The primary db (`$POSTGRES_DB`) is created
# by the official postgres image; this script adds `$DUCKLAKE_CATALOG_DB`.
set -euo pipefail

: "${DUCKLAKE_CATALOG_DB:?DUCKLAKE_CATALOG_DB must be set on the postgres service}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE "$DUCKLAKE_CATALOG_DB";
EOSQL
