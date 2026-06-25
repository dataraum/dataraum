"""Offline prompt dumping — read the *actual* rendered prompt, not the template.

The graph SQL prompt is a vertical-agnostic template whose grounding content is
injected at render time (schema, value-sets, field-mappings, prior context). The
only way to verify what the agent actually saw — and where a wrong grounding came
from — is to read the rendered system+user prompt, with every injected concept in
place. This module writes that to ``settings.prompt_dump_dir`` when set, and is a
no-op otherwise. Best-effort: a dump failure NEVER breaks generation.

One file per (label, key, prompt_hash): the same render re-dumps to the same path
(idempotent across retries), distinct renders are distinct files. Read them with
``cat`` / ``grep`` during a smoke or eval round.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

from dataraum.core.logging import get_logger
from dataraum.core.settings import get_settings

logger = get_logger(__name__)

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _slug(value: str) -> str:
    """Filesystem-safe, bounded slug for a label/key segment."""
    return _SAFE.sub("-", value).strip("-")[:80] or "unknown"


def _dump_path(label: str, key: str, prompt_hash: str) -> Path | None:
    """The dump file for a (label, key, hash), or None if dumping is off."""
    dump_dir = get_settings().prompt_dump_dir
    if dump_dir is None:
        return None
    target = dump_dir / _slug(label)
    target.mkdir(parents=True, exist_ok=True)
    return target / f"{_slug(key)}.{prompt_hash}.txt"


def dump_response(
    *,
    label: str,
    key: str,
    prompt_hash: str,
    body: str,
) -> None:
    """Append the agent's OUTPUT to the matching prompt dump (verification half).

    The prompt alone shows what the agent SAW; this shows what it PRODUCED — the
    generated SQL, the per-concept grounding, and the confidence — so a failed or
    weak metric (which never persists a snippet) is still inspectable. No-op
    unless dumping is on; best-effort.
    """
    try:
        path = _dump_path(label, key, prompt_hash)
        if path is None:
            return
        with path.open("a", encoding="utf-8") as f:
            f.write(f"\n===== RESPONSE =====\n{body}\n")
    except Exception as e:  # pragma: no cover - dumping is best-effort
        logger.debug("prompt_response_dump_failed", label=label, key=key, error=str(e))


def dump_prompt(
    *,
    label: str,
    key: str,
    prompt_hash: str,
    system: str | None,
    user: str,
    model: str | None = None,
) -> None:
    """Write a rendered prompt to the dump dir, if configured.

    Args:
        label: the LLM feature (e.g. ``"graph_sql_generation"``) — top dir.
        key: what the prompt is FOR (e.g. the ``graph_id``) — names the file.
        prompt_hash: the user-prompt hash — disambiguates re-renders.
        system: the rendered system prompt.
        user: the rendered user prompt (carries the injected concepts).
        model: the resolved model id, recorded in the header.
    """
    try:
        path = _dump_path(label, key, prompt_hash)
        if path is None:
            return
        header = (
            f"# label={label}\n# key={key}\n# prompt_hash={prompt_hash}\n"
            f"# model={model or '?'}\n# dumped_at={datetime.now(UTC).isoformat()}\n"
        )
        path.write_text(
            f"{header}\n===== SYSTEM =====\n{system or ''}\n\n===== USER =====\n{user}\n",
            encoding="utf-8",
        )
    except Exception as e:  # pragma: no cover - dumping is best-effort
        logger.debug("prompt_dump_failed", label=label, key=key, error=str(e))


__all__ = ["dump_prompt", "dump_response"]
