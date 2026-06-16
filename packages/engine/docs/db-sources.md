# Database Sources

DataRaum can analyze data sitting in a relational database via DuckDB's database extensions. Today: **Microsoft SQL Server** (other backends arrive in a follow-up release).

You pick *what* to extract in the cockpit — connect to the database, choose the tables/columns you want, and DataRaum synthesizes a **recipe** (a set of named `SELECT` queries) attached to the source. Credentials never go into the recipe; they live in the engine's environment (`.env` / container env) and are resolved at extraction time. When the session runs, DataRaum reads the recipe, runs the SELECTs against your database, and materializes the results as raw tables — the rest of the pipeline doesn't know or care that the source was a database.

## How it works

```
  cockpit (browser)                    engine-worker container env
┌──────────────────────────┐         ┌─────────────────────────────┐
│ connect → pick tables     │         │ DATARAUM_ERP_URL=mssql://…   │
│  → select tool synthesizes│         └─────────────────────────────┘
│    a db_recipe source:    │                      │
│      backend: mssql       │                      ▼
│      tables: {invoices…}  │   ┌──────────────────────────────────┐
│      recipe_hash          ├──▶│  addSourceWorkflow → import phase  │
└──────────────────────────┘   │                                    │
                               │  → resolve DATARAUM_ERP_URL        │
                               │  → INSTALL/LOAD mssql extension    │
                               │  → ATTACH READ_ONLY                │
                               │  → CREATE TABLE lake.raw.raw_… AS  │
                               │       <your SELECT>                │
                               │  → DETACH                          │
                               └──────────────────────────────────┘
```

The source **name** does double duty: source identity and the credential-lookup key (`DATARAUM_{NAME}_URL`). The recipe lives on the source row in the cockpit DB (`source_type='db_recipe'`, a `backend` column, and `connection_config.tables`) — there is no recipe file on disk and no sources bind-mount (file sources moved to the object store, DAT-388/389).

## Registering a database source (cockpit)

The recipe is produced entirely through the cockpit — there is no MCP tool and no yaml file (both retired: MCP in DAT-487, the yaml recipe parser in DAT-430).

1. **Connect.** Point the cockpit at the database (read-only probe — it inspects the catalog, creates nothing).
2. **Pick the tables/columns** you want to bring in.
3. The **`select`** tool synthesizes the recipe and persists one `db_recipe` source: the `backend` (`mssql`), the named `SELECT`s in `connection_config.tables`, and a `recipe_hash` (sha256 over the canonical `{backend, tables}`) so the engine can tell a re-pointed recipe from an unchanged one.
4. **Run the session** — `addSourceWorkflow` → the import phase resolves the credential URL, ATTACHes the database READ_ONLY, and materializes each recipe table into `lake.raw`.

### Recipe SQL rules

- **Recipe SQL is parsed by DuckDB first**, then forwarded to your database. Use portable SQL: `LIMIT 10` not `TOP 10`, `||` not `+` for string concat. Standard `SELECT … FROM schema.Table WHERE x = 1 GROUP BY …` works.
- **Schema-qualify** table references (`FROM dbo.Invoices`, or `FROM sales.Orders` for a non-default schema). After ATTACH the engine issues `USE src.<default_schema>` so a qualified name resolves without an alias prefix.
- **Identifier quoting:** if a column or table has a space in its name (e.g. AdventureWorksLT's `dbo.BuildVersion."Database Version"`), quote it with **double quotes**, not square brackets — DuckDB parses it first.

| Backend | Default schema |
|---|---|
| mssql | `dbo` |
| postgres | `public` |
| mysql | `main` |
| sqlite | `main` |

## Credentials

DataRaum resolves a connection URL from the environment via the `CredentialChain` (`core/credentials.py`) — the **`DATARAUM_{NAME}_URL`** environment variable, where `{NAME}` is the source name uppercased. So a source named `erp` → `DATARAUM_ERP_URL`.

Credentials are **never** persisted to the cockpit DB or the workspace DB, never appear in tool responses, and never go into the recipe. If the var is missing the import phase fails loud (`No credentials found for database source 'erp'. Set DATARAUM_ERP_URL …`).

### Connection URL

```
DATARAUM_ERP_URL=mssql://dataraum_reader:ReadOnly!2026@host:1433/AdventureWorksLT?TrustServerCertificate=yes
```

Three things to know:

- **`TrustServerCertificate=yes` is required for typical installs.** SQL Server 2022+ enables TLS by default with a self-signed cert. Without this flag, the handshake fails as a generic "Failed to connect." Only set it when you've verified the host out-of-band (or for dev/test).
- **The URL above is one of three accepted shapes.** Equivalent: `Server=host;Database=AdventureWorksLT;UID=dataraum_reader;PWD=…;TrustServerCertificate=yes;` and the ODBC-style `Driver={ODBC Driver 18 for SQL Server};Server=host,1433;…`. They all resolve to the same TDS connection underneath.
- **The DuckDB community `mssql` extension is auto-installed on first use** (from the community repo). No manual setup; it pins to the engine's DuckDB version. (See the air-gapped note under deployment if your host blocks egress.)

A read-only login is recommended. DataRaum already ATTACHes with `READ_ONLY` (writes are blocked at the extension layer), but a `db_datareader` user makes the no-write guarantee belt-and-braces:

```sql
USE master;
CREATE LOGIN dataraum_reader WITH PASSWORD = 'ReadOnly!2026', CHECK_POLICY = OFF;
GO
USE YourDatabase;
CREATE USER dataraum_reader FOR LOGIN dataraum_reader;
ALTER ROLE db_datareader ADD MEMBER dataraum_reader;
GO
```

## Deploying with docker-compose (client test machine)

A typical client trial: DataRaum runs from `packages/infra/docker-compose.yml` on a test box, pointed at the client's existing SQL Server. Three things to get right — credential injection, network reachability, and (sometimes) extension egress.

### 1. Inject the credential into the engine worker

The engine worker only sees env vars that compose passes into it. Add the source's URL to the shared worker-env anchor (`x-engine-worker-env`) so every workspace worker resolves it, sourcing the value from `.env`:

```yaml
# packages/infra/docker-compose.yml  →  x-engine-worker-env: &engine-worker-env
  ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY:-}
  DATARAUM_ERP_URL: ${DATARAUM_ERP_URL:-}        # ← add: one line per db source
```

```bash
# packages/infra/.env  (gitignored — the single source of secrets)
DATARAUM_ERP_URL=mssql://dataraum_reader:ReadOnly!2026@sql.client.internal:1433/Sales?TrustServerCertificate=yes
```

The `${DATARAUM_ERP_URL:-}` interpolation reads `.env`; the var name's `ERP` must match the uppercased source name you register in the cockpit. Add one line per database source. (This mirrors how `ANTHROPIC_API_KEY` and the S3 creds are already passed — explicit `environment:` entries, never the host shell.)

### 2. Make the SQL Server reachable from the container

`host` in the URL is resolved **from inside the engine-worker container**, not from the host shell:

- **SQL Server on another machine / the client LAN** — use its hostname or IP as the client network sees it (`sql.client.internal`, `10.0.0.12`). The Docker bridge network forwards outbound, so a routable address just works.
- **SQL Server on the same machine as compose** — use `host.docker.internal:1433` (Docker Desktop, and Linux with `extra_hosts: ["host.docker.internal:host-gateway"]`), not `localhost` — `localhost` inside the container is the container itself.
- **SQL Server in a sibling compose service** — use the service name on the shared compose network.

Quick reachability check from the worker:

```bash
docker compose -f packages/infra/docker-compose.yml exec engine-worker \
  python -c "import socket; socket.create_connection(('sql.client.internal', 1433), 5); print('reachable')"
```

### 3. Egress-locked hosts: pre-bake the extension

The community `mssql` extension installs from `extensions.duckdb.org` on first use. On an air-gapped or egress-filtered client box, set `DUCKLAKE_SKIP_INSTALL=true` and `DUCKDB_EXTENSION_DIRECTORY=/path/to/baked` so `LOAD` resolves a pre-baked copy instead of reaching the network (the worker image already bakes the core extensions; `mssql` needs to be added to that bake for fully-offline installs). Otherwise the import phase fails loud at `DuckDB extension 'mssql' failed to install/load`.

After editing the env, recreate the worker so it picks up the new variable:

```bash
docker compose -f packages/infra/docker-compose.yml up -d --force-recreate engine-worker
```

## Loud failure on every step

Anything that can fail is surfaced verbatim through the run's `phases_failed` structure:

| Failure | Phase | Example message |
|---|---|---|
| Credentials missing | `import` | `No credentials found for database source 'erp'. Set DATARAUM_ERP_URL in the environment.` |
| Extension install/load fails | `import` | `DuckDB extension 'mssql' failed to install/load: <verbatim>` |
| Connection / TLS fails | `import` | `ATTACH failed for mssql source: Connection failed to host:1433: …` |
| SELECT errors | `import` | `Recipe table 'invoices' SELECT failed: Invalid column 'invoice_dat'` |
| Zero rows | `look` warning | (Surfaced as a warning on the source, not a pipeline failure.) |

## Other backends

`backend: postgres`, `backend: mysql`, `backend: sqlite` are recognized by the extraction layer (`sources/backends.py`) but not yet wired through the cockpit connect flow. They'll be enabled in a follow-up release.
