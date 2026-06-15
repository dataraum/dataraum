#!/usr/bin/env bash
# Postgres first-boot init: create the per-workspace DuckLake catalog databases
# + the cockpit_db database alongside the primary platform database. The primary
# db (`$POSTGRES_DB`) is created by the official postgres image; this script adds
# the catalog DBs and `$COCKPIT_DB`.
#
# Per-workspace catalogs (DAT-505): each workspace's engine worker ATTACHes its
# OWN DuckLake catalog DB — never a shared one. Workspace 1 uses
# `$DUCKLAKE_CATALOG_DB`; the dev `multi-workspace` compose profile's workspace 2
# uses `${DUCKLAKE_CATALOG_DB}_2`. Both are created here so the worker's ATTACH
# finds its catalog whether or not the second workspace is brought up (creating
# an unused DB is harmless; an ATTACH to a missing one fails loud at boot). A new
# dev workspace = add its `_<n>` catalog to the list below + a compose service.
set -euo pipefail

: "${DUCKLAKE_CATALOG_DB:?DUCKLAKE_CATALOG_DB must be set on the postgres service}"
: "${COCKPIT_DB:?COCKPIT_DB must be set on the postgres service}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE "$DUCKLAKE_CATALOG_DB";
    CREATE DATABASE "${DUCKLAKE_CATALOG_DB}_2";
    CREATE DATABASE "$COCKPIT_DB";
EOSQL
