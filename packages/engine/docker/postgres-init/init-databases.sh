#!/usr/bin/env bash
# Postgres first-boot init: create the installation-wide DuckLake catalog
# database + the cockpit_db database alongside the primary platform database.
# The primary db (`$POSTGRES_DB`) is created by the official postgres image;
# this script adds `$DUCKLAKE_CATALOG_DB` and `$COCKPIT_DB`.
#
# ONE catalog database per installation (DAT-815): every workspace's DuckLake
# catalog lives in `$DUCKLAKE_CATALOG_DB` as its own Postgres schema, selected
# via METADATA_SCHEMA on the ATTACH (`ws_<id>`, derived from the workspace id
# at boot). The ATTACH creates the schema itself (spike DAT-814), so nothing is
# allocated per workspace here — a new workspace needs no Postgres first-boot
# step at all. Teardown drops the workspace's schema
# (packages/infra/scripts/delete-workspace.sh), never this database.
set -euo pipefail

: "${DUCKLAKE_CATALOG_DB:?DUCKLAKE_CATALOG_DB must be set on the postgres service}"
: "${COCKPIT_DB:?COCKPIT_DB must be set on the postgres service}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE "$DUCKLAKE_CATALOG_DB";
    CREATE DATABASE "$COCKPIT_DB";
EOSQL
