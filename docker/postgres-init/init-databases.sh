#!/usr/bin/env bash
# Postgres first-boot init: create the DuckLake catalog database alongside
# the primary platform database. The primary db (`dataraum`) is created by
# the official postgres image via $POSTGRES_DB. This script adds the second.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE dataraum_lake_catalog;
EOSQL
