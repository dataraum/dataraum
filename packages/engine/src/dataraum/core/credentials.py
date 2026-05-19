"""Credential resolution for database source connections.

Resolves connection URLs by source name from the
``DATARAUM_{SOURCE_NAME}_URL`` environment variable. Secrets are never
serialized to MCP responses or logged.

Resolution order:

1. Environment variable: ``DATARAUM_{SOURCE_NAME}_URL``
2. Not found → return ``None`` (caller fails loud)
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Protocol

_log = logging.getLogger(__name__)


class CredentialProvider(Protocol):
    """Protocol for credential providers."""

    def resolve(self, source_name: str) -> ResolvedCredential | None:
        """Resolve a connection URL for the given source name."""
        ...


@dataclass(frozen=True)
class ResolvedCredential:
    """A resolved connection URL. Never serialized to MCP responses."""

    url: str
    source: str  # "env"


class EnvProvider:
    """Resolve connection URL from ``DATARAUM_{SOURCE_NAME}_URL`` environment variable."""

    def resolve(self, source_name: str) -> ResolvedCredential | None:
        env_key = f"DATARAUM_{source_name.upper()}_URL"
        url = os.environ.get(env_key)
        if url:
            return ResolvedCredential(url=url, source="env")
        return None


class CredentialChain:
    """Resolve connection URLs through an ordered chain of providers.

    Currently env-only. The chain shape is retained so additional providers
    (e.g. a future secrets-manager backend) can be added without touching
    callers.
    """

    def __init__(self) -> None:
        self._providers: list[CredentialProvider] = [EnvProvider()]

    def resolve(self, source_name: str) -> ResolvedCredential | None:
        """Walk the provider chain. Return first match or None."""
        for provider in self._providers:
            result = provider.resolve(source_name)
            if result is not None:
                return result
        return None
