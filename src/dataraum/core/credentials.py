"""Credential resolution chain for data source connections.

Resolves connection URLs by source name through an ordered chain of providers.
First match wins. Secrets are never serialized to MCP responses or logged.

Resolution order:
1. Environment variable: DATARAUM_{SOURCE_NAME}_URL
2. Credentials file: ~/.dataraum/credentials.yaml → sources.{name}
3. Not found → return None (caller returns setup instructions)
"""

from __future__ import annotations

import logging
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import yaml

_log = logging.getLogger(__name__)

# URL templates for each supported backend, used in setup instructions.
BACKEND_URL_TEMPLATES: dict[str, str] = {
    "postgres": "postgres://user:password@host:5432/database",
    "mysql": "mysql://user:password@host:3306/database",
    "sqlite": "/path/to/database.db",
}

_CREDENTIALS_FILE = "credentials.yaml"
_DESIRED_DIR_MODE = 0o700
_DESIRED_FILE_MODE = 0o600


class CredentialProvider(Protocol):
    """Protocol for credential providers."""

    def resolve(self, source_name: str) -> ResolvedCredential | None:
        """Resolve a connection URL for the given source name."""
        ...


@dataclass(frozen=True)
class ResolvedCredential:
    """A resolved connection URL. Never serialized to MCP responses."""

    url: str
    source: str  # "env" | "credentials_file"


class EnvProvider:
    """Resolve connection URL from DATARAUM_{SOURCE_NAME}_URL environment variable."""

    def resolve(self, source_name: str) -> ResolvedCredential | None:
        env_key = f"DATARAUM_{source_name.upper()}_URL"
        url = os.environ.get(env_key)
        if url:
            return ResolvedCredential(url=url, source="env")
        return None


class FileProvider:
    """Resolve connection URL from a YAML credentials file.

    File format:
        sources:
          accounting: "postgres://reader:secret@localhost:5432/accounting"
          erp: "mysql://user:pass@erp.internal/production"
    """

    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def resolve(self, source_name: str) -> ResolvedCredential | None:
        if not self._path.exists():
            return None

        self._check_permissions()

        with open(self._path) as f:
            config = yaml.safe_load(f)

        if not isinstance(config, dict):
            return None

        sources = config.get("sources", {})
        if not isinstance(sources, dict):
            return None

        url = sources.get(source_name)
        if url and isinstance(url, str):
            return ResolvedCredential(url=url, source="credentials_file")
        return None

    def _check_permissions(self) -> None:
        """Warn if credentials file has overly permissive permissions."""
        try:
            file_stat = self._path.stat()
            mode = stat.S_IMODE(file_stat.st_mode)
            if mode & (stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH):
                _log.warning(
                    "Credentials file %s has permissions %o. "
                    "Recommended: %o (owner-only read/write).",
                    self._path,
                    mode,
                    _DESIRED_FILE_MODE,
                )
        except OSError:
            pass


class CredentialChain:
    """Resolve connection URLs through an ordered chain of providers.

    Environment variables override the credentials file, allowing
    per-session overrides without editing persistent config.
    """

    def __init__(self, credentials_dir: Path | None = None) -> None:
        self._credentials_dir = credentials_dir or Path.home() / ".dataraum"
        self._providers: list[CredentialProvider] = [
            EnvProvider(),
            FileProvider(self._credentials_dir / _CREDENTIALS_FILE),
        ]

    @property
    def credentials_dir(self) -> Path:
        return self._credentials_dir

    @property
    def credentials_file(self) -> Path:
        return self._credentials_dir / _CREDENTIALS_FILE

    def resolve(self, source_name: str) -> ResolvedCredential | None:
        """Walk the provider chain. Return first match or None."""
        for provider in self._providers:
            result = provider.resolve(source_name)
            if result is not None:
                return result
        return None

    def save(self, source_name: str, url: str) -> Path:
        """Save a connection URL to the credentials file.

        Creates the config directory and file if they don't exist.
        Sets secure permissions (0700 for dir, 0600 for file).

        Returns:
            Path to the credentials file.
        """
        self._ensure_config_dir()

        cred_file = self.credentials_file
        config: dict[str, Any] = {}

        if cred_file.exists():
            with open(cred_file) as f:
                loaded = yaml.safe_load(f)
                if isinstance(loaded, dict):
                    config = loaded

        sources = config.setdefault("sources", {})
        sources[source_name] = url

        with open(cred_file, "w") as f:
            yaml.dump(config, f, default_flow_style=False)

        # Ensure 0600 permissions after write
        cred_file.chmod(_DESIRED_FILE_MODE)
        return cred_file

    def instructions_for(self, source_name: str, backend: str) -> dict[str, Any]:
        """Generate setup instructions when no credentials are found.

        Returns a dict suitable for including in MCP tool responses.
        Claude uses this to guide the user without ever seeing secret values.
        """
        url_template = BACKEND_URL_TEMPLATES.get(
            backend, f"{backend}://user:password@host/database"
        )

        env_var = f"DATARAUM_{source_name.upper()}_URL"

        yaml_template = f'sources:\n  {source_name}: "{url_template}"'

        return {
            "ref": source_name,
            "url_template": url_template,
            "file_template": yaml_template,
            "file_path": str(self.credentials_file),
            "env_alternative": env_var,
        }

    def _ensure_config_dir(self) -> None:
        """Create ~/.dataraum/ with 0700 permissions if it doesn't exist."""
        self._credentials_dir.mkdir(parents=True, exist_ok=True)
        try:
            self._credentials_dir.chmod(_DESIRED_DIR_MODE)
        except OSError:
            _log.debug("Could not set permissions on %s", self._credentials_dir)
