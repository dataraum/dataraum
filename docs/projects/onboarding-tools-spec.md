# Onboarding Tools: MCP Schemas & Credential Resolution

*Technical spec for source management tools and the credential chain.*

---

## Design Principles

1. **Intent through the LLM, secrets never.** Claude sees backend types, hostnames, table names — never passwords, tokens, or keys.
2. **Credential references, not inline secrets.** Source configs store a `credential_ref` that the server resolves at connection time through a chain of providers.
3. **Validate early, analyze later.** Connection checks happen at registration, not when the user kicks off a long pipeline run.
4. **DuckDB does the heavy lifting.** No custom drivers. Every backend is a DuckDB extension, `ATTACH`, or scanner function.

---

## MCP Tool Schemas

### `discover_workspace`

Scan the current workspace for data files. Called proactively on session start or when the user asks "what data do I have?"

```jsonc
// Input
{
  "name": "discover_workspace",
  "description": "Scan the workspace for data files and return what's available.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "path": {
        "type": "string",
        "description": "Root directory to scan. Defaults to workspace root."
      },
      "recursive": {
        "type": "boolean",
        "default": true,
        "description": "Scan subdirectories."
      }
    }
  }
}

// Output
{
  "files": [
    {
      "path": "data/bookings.csv",
      "format": "csv",
      "size_bytes": 1240000,
      "modified": "2025-02-20T14:30:00Z",
      "preview": {
        "row_count_estimate": 12400,
        "columns": ["booking_id", "customer", "amount", "date", "status"]
      }
    },
    {
      "path": "exports/*.parquet",
      "format": "parquet",
      "matched_files": 3,
      "total_size_bytes": 48000000
    }
  ],
  "existing_sources": ["bookings"]  // already registered, skip in suggestions
}
```

**Notes:** The preview for CSV/Parquet is cheap — DuckDB can read the first row or Parquet metadata without loading the full file. For glob patterns, report the match count and aggregate size. `existing_sources` lets Claude avoid suggesting files that are already configured.

---

### `add_source`

Register a data source. Handles both local files and database backends.

```jsonc
// Input
{
  "name": "add_source",
  "description": "Register a new data source. For databases, validates the connection if credentials are available.",
  "inputSchema": {
    "type": "object",
    "required": ["name"],
    "properties": {
      "name": {
        "type": "string",
        "description": "Human-readable source name. Used as credential_ref if not specified separately.",
        "pattern": "^[a-z][a-z0-9_]{1,48}$"
      },

      // --- File sources ---
      "path": {
        "type": "string",
        "description": "File path or glob pattern. Mutually exclusive with 'backend'."
      },

      // --- Database / remote sources ---
      "backend": {
        "type": "string",
        "enum": ["postgres", "mysql", "sqlite", "s3", "motherduck"],
        "description": "DuckDB extension to use. Mutually exclusive with 'path'."
      },
      "connection": {
        "type": "object",
        "description": "Non-secret connection parameters.",
        "properties": {
          "host":     { "type": "string" },
          "port":     { "type": "integer" },
          "database": { "type": "string" },
          "schema":   { "type": "string", "default": "public" },
          "bucket":   { "type": "string", "description": "S3/MinIO bucket name." },
          "prefix":   { "type": "string", "description": "S3 key prefix / path filter." },
          "region":   { "type": "string" },
          "endpoint": { "type": "string", "description": "Custom S3 endpoint for MinIO etc." }
        }
      },
      "tables": {
        "type": "array",
        "items": { "type": "string" },
        "description": "Optional table filter. If omitted, all tables are discovered."
      },
      "credential_ref": {
        "type": "string",
        "description": "Key to look up in the credential chain. Defaults to source name."
      }
    },
    "oneOf": [
      { "required": ["path"] },
      { "required": ["backend"] }
    ]
  }
}

// Output — success (file)
{
  "source": {
    "name": "bookings",
    "type": "file",
    "path": "data/bookings.csv",
    "status": "configured",
    "preview": {
      "row_count_estimate": 12400,
      "columns": ["booking_id", "customer", "amount", "date", "status"]
    }
  }
}

// Output — success (database, credentials found)
{
  "source": {
    "name": "accounting",
    "type": "postgres",
    "status": "validated",
    "credential_source": "credentials_file",   // transparency: where creds came from
    "schema_discovered": {
      "tables": [
        {
          "name": "journal_entries",
          "columns": ["id", "date", "account", "debit", "credit", "memo"],
          "row_count_estimate": 84000
        },
        {
          "name": "chart_of_accounts",
          "columns": ["account_id", "name", "type", "parent_id"],
          "row_count_estimate": 320
        }
      ],
      "tables_excluded": 45   // how many were filtered out or not selected
    }
  }
}

// Output — needs credentials
{
  "source": {
    "name": "accounting",
    "type": "postgres",
    "status": "needs_credentials"
  },
  "credential_instructions": {
    "ref": "accounting",
    "required_keys": ["user", "password"],
    "optional_keys": ["sslmode"],
    "file_template": "[sources.accounting]\nuser = \"\"\npassword = \"\"",
    "file_path": "~/.dataraum/credentials.toml",
    "env_alternative": {
      "user": "DATARAUM_ACCOUNTING_USER",
      "password": "DATARAUM_ACCOUNTING_PASSWORD"
    }
  }
}
```

**Why `credential_source` in the response?** When credentials resolve successfully, the user (via Claude) sees *where* they came from — `env`, `credentials_file`, `keychain` — without seeing the values. This helps debugging ("oh, it's using the env var, not my credential file").

---

### `list_sources`

```jsonc
// Input
{
  "name": "list_sources",
  "description": "Return all configured sources with their current status.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "status_filter": {
        "type": "string",
        "enum": ["configured", "validated", "analyzed", "stale", "error", "needs_credentials"],
        "description": "Optional filter by status."
      }
    }
  }
}

// Output
{
  "sources": [
    {
      "name": "bookings",
      "type": "file",
      "path": "data/bookings.csv",
      "status": "analyzed",
      "last_analyzed": "2025-02-20T15:00:00Z",
      "stale": false,
      "row_count": 12400,
      "entropy_summary": { "overall": 0.42, "worst_dimension": "semantic" }
    },
    {
      "name": "accounting",
      "type": "postgres",
      "status": "validated",
      "tables": ["journal_entries", "chart_of_accounts"],
      "stale": null  // unknown for databases
    }
  ]
}
```

---

### `remove_source`

```jsonc
// Input
{
  "name": "remove_source",
  "description": "Archive a source. Does not delete analysis history.",
  "inputSchema": {
    "type": "object",
    "required": ["name"],
    "properties": {
      "name": { "type": "string" },
      "purge_results": {
        "type": "boolean",
        "default": false,
        "description": "Also delete stored analysis results. Default: keep history."
      }
    }
  }
}

// Output
{
  "removed": "accounting",
  "analysis_preserved": true,
  "credential_hint": "You may also want to remove the [sources.accounting] entry from ~/.dataraum/credentials.toml"
}
```

---

### `validate_source`

Standalone validation for when users update credentials and want to check without re-analyzing.

```jsonc
// Input
{
  "name": "validate_source",
  "description": "Test the connection for an existing source.",
  "inputSchema": {
    "type": "object",
    "required": ["name"],
    "properties": {
      "name": { "type": "string" },
      "refresh_schema": {
        "type": "boolean",
        "default": false,
        "description": "Re-discover tables and update the source config."
      }
    }
  }
}

// Output — success
{
  "name": "accounting",
  "status": "validated",
  "credential_source": "credentials_file",
  "latency_ms": 45,
  "schema_changed": false
}

// Output — failure
{
  "name": "accounting",
  "status": "error",
  "error": "connection_refused",
  "message": "Could not connect to accounting.internal:5432. Is the host reachable?",
  "credential_source": "env"   // still useful: tells user which creds were attempted
}
```

---

## Credential Resolution Chain

The server resolves credentials at connection time by walking a chain of providers. First match wins. **No credentials are ever returned in MCP tool responses or logged.**

```
┌─────────────────────────────────────────────────────────┐
│                  Credential Resolution                   │
│                                                          │
│  1. Environment variables                                │
│     DATARAUM_{SOURCE_NAME}_{KEY}                         │
│     e.g. DATARAUM_ACCOUNTING_PASSWORD                    │
│                                                          │
│  2. Credentials file                                     │
│     ~/.dataraum/credentials.toml                         │
│     [sources.<credential_ref>]                           │
│     password = "..."                                     │
│                                                          │
│  3. OS keychain (future)                                 │
│     service: "dataraum/<credential_ref>"                 │
│     via `keyring` library                                │
│                                                          │
│  4. Fail → return needs_credentials with instructions    │
└─────────────────────────────────────────────────────────┘
```

### Implementation sketch

```python
from dataclasses import dataclass
from pathlib import Path
import os
import tomllib


@dataclass
class ResolvedCredentials:
    """Credentials resolved for a source. Never serialized to MCP responses."""
    values: dict[str, str]
    source: str  # "env" | "credentials_file" | "keychain"


class CredentialChain:
    """Resolve credentials through a chain of providers.
    
    The chain is ordered by precedence. Environment variables
    override the credential file, which overrides the keychain.
    This lets users override per-session via env vars without
    editing persistent config.
    """

    def __init__(self, credentials_dir: Path | None = None):
        self._credentials_dir = credentials_dir or Path.home() / ".dataraum"
        self._providers: list[CredentialProvider] = [
            EnvProvider(),
            FileProvider(self._credentials_dir / "credentials.toml"),
            # KeychainProvider(),  # future
        ]

    def resolve(
        self, credential_ref: str, required_keys: list[str]
    ) -> ResolvedCredentials | None:
        """Walk the chain. Return first complete match or None."""
        for provider in self._providers:
            result = provider.resolve(credential_ref, required_keys)
            if result is not None:
                return result
        return None

    def instructions_for(
        self, credential_ref: str, required_keys: list[str], optional_keys: list[str] | None = None
    ) -> dict:
        """Generate setup instructions when no credentials are found.
        
        Returns a dict suitable for including in MCP tool responses.
        Claude uses this to guide the user without ever seeing secret values.
        """
        optional_keys = optional_keys or []
        env_map = {
            key: f"DATARAUM_{credential_ref.upper()}_{key.upper()}"
            for key in required_keys + optional_keys
        }
        toml_lines = [f"[sources.{credential_ref}]"]
        for key in required_keys:
            toml_lines.append(f'{key} = ""')
        for key in optional_keys:
            toml_lines.append(f'# {key} = ""')

        return {
            "ref": credential_ref,
            "required_keys": required_keys,
            "optional_keys": optional_keys,
            "file_template": "\n".join(toml_lines),
            "file_path": str(self._credentials_dir / "credentials.toml"),
            "env_alternative": env_map,
        }


class EnvProvider:
    """Resolve credentials from environment variables.
    
    Convention: DATARAUM_{SOURCE}_{KEY}
    Example:    DATARAUM_ACCOUNTING_PASSWORD
    """

    def resolve(
        self, credential_ref: str, required_keys: list[str]
    ) -> ResolvedCredentials | None:
        prefix = f"DATARAUM_{credential_ref.upper()}_"
        values = {}
        for key in required_keys:
            env_key = f"{prefix}{key.upper()}"
            val = os.environ.get(env_key)
            if val is None:
                return None  # incomplete — try next provider
            values[key] = val
        return ResolvedCredentials(values=values, source="env")


class FileProvider:
    """Resolve credentials from a TOML file.
    
    File format:
        [sources.accounting]
        user = "reader"
        password = "secret"
    """

    def __init__(self, path: Path):
        self._path = path

    def resolve(
        self, credential_ref: str, required_keys: list[str]
    ) -> ResolvedCredentials | None:
        if not self._path.exists():
            return None

        with open(self._path, "rb") as f:
            config = tomllib.load(f)

        section = config.get("sources", {}).get(credential_ref, {})
        values = {}
        for key in required_keys:
            if key not in section:
                return None  # incomplete
            values[key] = str(section[key])
        return ResolvedCredentials(values=values, source="credentials_file")
```

### Per-backend credential requirements

Each backend declares what keys it needs. This drives both validation and instruction generation.

```python
BACKEND_CREDENTIALS: dict[str, dict] = {
    "postgres": {
        "required": ["user", "password"],
        "optional": ["sslmode", "sslrootcert"],
    },
    "mysql": {
        "required": ["user", "password"],
        "optional": ["ssl_ca"],
    },
    "s3": {
        "required": ["aws_access_key_id", "aws_secret_access_key"],
        "optional": ["aws_session_token", "region"],
    },
    "sqlite": {
        "required": [],   # no credentials needed
        "optional": [],
    },
    "motherduck": {
        "required": ["token"],
        "optional": [],
    },
}
```

---

## Source Config Model

Stored in the SQLite metadata DB. No secrets here — just structural config and the credential reference.

```python
@dataclass
class SourceConfig:
    name: str                          # unique identifier, e.g. "accounting"
    source_type: str                   # "file" | "postgres" | "mysql" | ...
    status: str                        # configured | validated | analyzed | stale | error | needs_credentials

    # File sources
    path: str | None = None            # file path or glob pattern

    # Database sources
    connection: dict | None = None     # non-secret params: host, port, database, schema
    tables: list[str] | None = None    # table filter (None = all discovered)
    credential_ref: str | None = None  # lookup key for credential chain (defaults to name)

    # Metadata
    discovered_schema: dict | None = None  # cached table/column info from last validation
    last_validated: str | None = None
    last_analyzed: str | None = None
    created_at: str | None = None
```

The corresponding SQLite table:

```sql
CREATE TABLE IF NOT EXISTS sources (
    name            TEXT PRIMARY KEY,
    source_type     TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'configured',
    path            TEXT,
    connection_json TEXT,           -- JSON: {host, port, database, schema, ...}
    tables_json     TEXT,           -- JSON: ["table1", "table2"]
    credential_ref  TEXT,
    schema_json     TEXT,           -- JSON: cached discovered schema
    last_validated  TEXT,
    last_analyzed   TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    archived_at     TEXT            -- soft delete
);
```

---

## Connection Lifecycle

```
User: "connect to my postgres database"
          │
          ▼
   Claude asks for: host, database, tables of interest
          │
          ▼
   add_source(name="accounting", backend="postgres",
              connection={host: "...", database: "..."})
          │
          ▼
   ┌─ Server ──────────────────────────────────┐
   │  1. Store SourceConfig in SQLite           │
   │  2. Look up credential_ref in chain        │
   │     ├── Found? → Connect via DuckDB        │
   │     │   ├── OK → status: validated          │
   │     │   │       return discovered schema    │
   │     │   └── Fail → status: error            │
   │     │           return error details        │
   │     └── Not found? → status: needs_creds   │
   │                      return instructions    │
   └───────────────────────────────────────────┘
          │
          ▼
   Claude tells user what happened:
   ├── "Connected! Found 3 tables. Want to analyze?"
   ├── "Connection failed: host unreachable. Check the hostname."
   └── "I need credentials. Add them to ~/.dataraum/credentials.toml:
        [sources.accounting]
        user = \"\"
        password = \"\"
        Then say 'connect accounting' and I'll retry."
          │
          ▼
   User adds credentials, says "connect accounting"
          │
          ▼
   validate_source(name="accounting")
          │
          ▼
   "Connected. Ready to analyze."
          │
          ▼
   analyze(source="accounting")
```

---

## Open Decisions

### Validate on add vs. defer

**Recommendation: validate on add.** The latency of a connection check is sub-second for databases. The user gets immediate feedback, and Claude can present the discovered schema in the same conversational turn. The only exception: S3 bucket listing can be slow for large prefixes — consider a timeout and async fallback.

### Table auto-discovery vs. user picks

**Recommendation: discover all, present curated.** Return the full table list but have Claude categorize and suggest. Example: "I found 47 tables. The ones that look like core business data are X, Y, Z based on their names and column profiles. Want to start there?" The `tables` filter on `add_source` lets the user narrow it down without re-registering.

### Credential file permissions

The `~/.dataraum/credentials.toml` file should be created with `0600` permissions (owner-only read/write). The server should warn if the file has broader permissions, similar to how SSH warns about key file permissions.
