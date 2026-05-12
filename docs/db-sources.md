# Database Sources

DataRaum can analyze data sitting in a relational database via DuckDB's database extensions. Today: **Microsoft SQL Server** (other backends arrive in a follow-up release).

You declare *what* to extract in a yaml **recipe**. The recipe is per-user state — it lives under `~/.dataraum/recipes/` alongside your DataRaum workspace, not in a project repo. Credentials live in `.env` (or in the container env), never in the yaml. At session time, DataRaum reads the recipe, runs the SELECTs against your database, and materializes the results as raw tables — the rest of the pipeline doesn't know or care that the source was a database.

## How it works

```
~/.dataraum/recipes/erp.yaml    .env / container env
┌─────────────────────────┐     ┌─────────────────────────┐
│ backend: mssql          │     │ DATARAUM_ERP_URL=mssql..│
│ tables:                 │     └─────────────────────────┘
│   invoices:             │                 │
│     sql: ...            │                 ▼
│   customers:            │   ┌─────────────────────────────────┐
│     sql: ...            ├──▶│  add_source(path="erp",         │
└─────────────────────────┘   │              name="erp")        │
                              │                                 │
                              │  → reads DATARAUM_ERP_URL       │
                              │  → ATTACH READ_ONLY             │
                              │  → CREATE TABLE raw_invoices AS │
                              │       <your SQL>                │
                              │  → DETACH                       │
                              └─────────────────────────────────┘
```

The recipe name (`erp`) does triple duty: source identity, credential lookup key (`DATARAUM_ERP_URL`), and the prefix for raw tables (`erp__invoices`, `erp__customers`).

## Where recipes live

By convention, recipes live under `~/.dataraum/recipes/` (override the home directory with `DATARAUM_HOME`). DataRaum resolves the `path` argument in this order:

| You pass | DataRaum looks for | Notes |
|---|---|---|
| `/abs/path/erp.yaml` | The absolute path | Full flexibility — any location |
| `./local/erp.yaml` | The relative path from cwd | If it exists, used directly |
| `erp.yaml` | First `./erp.yaml`, then `~/.dataraum/recipes/erp.yaml` | Filename — recipes home is the fallback |
| `erp` | `~/.dataraum/recipes/erp.yaml`, then `…/erp.yml` | Bare name — `.yaml` and `.yml` tried in order |

Only recipe-shaped names (`.yaml`/`.yml` or no extension) get the recipes-home fallback. File paths like `data.csv` are taken at face value — DataRaum doesn't hunt for them in `~/.dataraum/recipes/`.

## Recipe yaml

```yaml
# ~/.dataraum/recipes/erp.yaml
backend: mssql              # mssql (today); postgres/mysql/sqlite follow
tables:
  invoices:
    sql: |
      SELECT invoice_id, customer_id, invoice_date,
             total_amount, currency, status
      FROM dbo.Invoices
      WHERE invoice_date >= '2024-01-01'
  customers:
    sql: |
      SELECT customer_id, name, region, segment
      FROM dbo.Customers
```

Rules:

- **No credentials in the yaml.** `connection:`, `credentials:`, `auth:`, `password:`, `secret:` at the top level are rejected at parse time. The recipe stays a safe artifact even if it accidentally leaks (the credential lookup happens entirely through env vars at runtime).
- **At least one entry under `tables:`**, each with a non-empty `sql:` field.
- **Table names unique** (case-insensitive).
- **Recipe SQL is parsed by DuckDB first**, then forwarded to your database. Use portable SQL where possible: `LIMIT 10` not `TOP 10`, `||` not `+` for string concat. Standard `SELECT ... FROM schema.Table WHERE x = 1 GROUP BY ...` works.

## Credentials

DataRaum resolves a connection URL from the environment via the existing `CredentialChain`:

1. `DATARAUM_{NAME}_URL` environment variable (set in `.env` or the container's env)
2. Failing that, `~/.dataraum/credentials.yaml`:

   ```yaml
   sources:
     erp: "mssql://reader:pwd@erp.internal:1433/Finance?TrustServerCertificate=yes"
   ```

`{NAME}` is the source name passed to `add_source` — uppercased. So `add_source(name="erp", ...)` → `DATARAUM_ERP_URL`. Credentials are **never** persisted in workspace.db, never appear in MCP responses, and never go into the recipe yaml.

## Setting up MS SQL Server

### 1. Container

Microsoft ships an image at `mcr.microsoft.com/mssql/server:2025-latest`. With Apple's `container` CLI:

```bash
container run \
  -e ACCEPT_EULA=Y \
  -e MSSQL_SA_PASSWORD=Test1234 \
  -e MSSQL_PID=Evaluation \
  -p 1433:1433 \
  --name sql2025 \
  --memory 3g \
  --platform linux/amd64 \
  -d mcr.microsoft.com/mssql/server:2025-latest
```

With Docker Desktop, the equivalent `docker run` works — replace `container` with `docker`.

### 2. Restore a sample DB + read-only user

The repo ships a single idempotent script that downloads Microsoft's [AdventureWorksLT2025](https://github.com/Microsoft/sql-server-samples/tree/master/samples/databases/adventure-works) (~1.8 MB), restores it into your container, and creates a `dataraum_reader` login with `db_datareader` rights:

```bash
tests/integration/sources/mssql_setup.sh
```

Output ends with the `DATARAUM_MSSQL_TEST_URL` to export for the integration test. Cold run: <5 s. Re-runs are no-ops.

If you'd rather use your own database, the script's body shows the two SQL statements involved:

```sql
USE master;
CREATE LOGIN dataraum_reader
  WITH PASSWORD = 'ReadOnly!2026', CHECK_POLICY = OFF;
GO

USE YourDatabase;
CREATE USER dataraum_reader FOR LOGIN dataraum_reader;
ALTER ROLE db_datareader ADD MEMBER dataraum_reader;
GO
```

DataRaum already ATTACHes with `READ_ONLY` — writes are blocked at the extension layer — but `db_datareader` makes the no-write guarantee belt-and-braces.

### 3. Connection URL

```
DATARAUM_ERP_URL=mssql://dataraum_reader:ReadOnly!2026@host:1433/AdventureWorksLT?TrustServerCertificate=yes
```

Three things to know:

- **`TrustServerCertificate=yes` is required for typical installs.** SQL Server 2022+ enables TLS by default with a self-signed cert. Without this flag, the handshake fails silently as "Failed to connect." Only set it when you've verified the host out-of-band (or for dev/test containers).
- **The URL form above is one of three accepted shapes.** Equivalent: `Server=host;Database=AdventureWorksLT;UID=dataraum_reader;PWD=...;TrustServerCertificate=yes;` and the ODBC-style `Driver={ODBC Driver 18 for SQL Server};Server=host,1433;...`. Use whichever your team prefers — they all resolve to the same TDS connection underneath.
- **The DuckDB community `mssql` extension is auto-installed on first use.** No manual setup. It pins to your installed DuckDB version.

### 4. Register the source

Save a recipe pointing at the tables you want. A starter against AdventureWorksLT — drop it at `~/.dataraum/recipes/aw.yaml`:

```yaml
# ~/.dataraum/recipes/aw.yaml
backend: mssql
tables:
  customers:
    sql: |
      SELECT CustomerID, FirstName, LastName, CompanyName
      FROM SalesLT.Customer
  products:
    sql: |
      SELECT ProductID, Name, ListPrice, SellStartDate
      FROM SalesLT.Product
```

Then:

```python
# Via MCP tool from Claude — bare name resolves against the recipes home
add_source(path="aw", name="aw")
begin_session(source="aw", intent="...")  # session is bound to this one source
measure  # runs the pipeline — extracts via the recipe and analyzes
```

Or pass an absolute path if you keep the recipe elsewhere:

```python
add_source(path="/path/to/my/aw.yaml", name="aw")
```

> **Identifier quoting.** Recipe SQL is parsed by DuckDB before forwarding to MSSQL. If a column or table has a space in its name (e.g. AdventureWorksLT's `dbo.BuildVersion."Database Version"`), quote it with **double quotes**, not square brackets.

## How recipe SQL is executed

After ATTACH, DataRaum issues `USE attached_alias.<default_schema>` so your SQL can reference tables by schema-qualified name (`FROM dbo.Invoices`) without an alias prefix. Per backend:

| Backend | Default schema |
|---|---|
| mssql | `dbo` |
| postgres | `public` |
| mysql | `main` |
| sqlite | `main` |

If your database puts tables in a non-default schema (e.g., `sales.Orders`), qualify with that schema in the recipe — `FROM sales.Orders`. DuckDB resolves it correctly.

## Loud failure on every step

Anything that can fail is surfaced verbatim through DataRaum's `phases_failed` structure on `measure`:

| Failure | Phase | Example message |
|---|---|---|
| Recipe yaml malformed | `import` | `Recipe sources/erp.yaml invalid: Table 'invoices' has empty or missing 'sql:' field.` |
| Credentials missing | `import` | `No credentials found for database source 'erp'. Set DATARAUM_ERP_URL in .env or add an entry to ~/.dataraum/credentials.yaml.` |
| Extension install/load fails | `import` | `DuckDB extension 'mssql' failed to install/load: <verbatim>` |
| Connection / TLS fails | `import` | `ATTACH failed for mssql source: Connection failed to host:1433: ...` |
| SELECT errors | `import` | `Recipe table 'invoices' SELECT failed: Invalid column 'invoice_dat'` |
| Zero rows | `look` warning | (Surfaced as a warning on the source, not a pipeline failure.) |

## Other backends

`backend: postgres`, `backend: mysql`, `backend: sqlite` are accepted by the parser but not yet wired through the pipeline. They'll be enabled in a follow-up release.
