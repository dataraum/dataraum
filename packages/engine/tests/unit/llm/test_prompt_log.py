"""Unit tests for offline prompt dumping (DAT-631 verification aid)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dataraum.llm import prompt_log


@dataclass
class _Settings:
    prompt_dump_dir: Path | None


def test_noop_when_dir_unset(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(prompt_log, "get_settings", lambda: _Settings(prompt_dump_dir=None))
    # Must not raise and must write nothing.
    prompt_log.dump_prompt(
        label="graph_sql_generation", key="gross_profit", prompt_hash="abc", system="s", user="u"
    )
    assert not any(tmp_path.iterdir())


def test_writes_rendered_prompt_when_dir_set(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(prompt_log, "get_settings", lambda: _Settings(prompt_dump_dir=tmp_path))
    prompt_log.dump_prompt(
        label="graph_sql_generation",
        key="gross_profit",
        prompt_hash="abc123",
        system="SYS BODY",
        user="USER BODY with COGS",
        model="claude-x",
    )
    written = list((tmp_path / "graph_sql_generation").glob("gross_profit.abc123.txt"))
    assert len(written) == 1
    text = written[0].read_text()
    assert "SYS BODY" in text
    assert "USER BODY with COGS" in text
    assert "model=claude-x" in text


def test_response_appends_to_prompt_file(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(prompt_log, "get_settings", lambda: _Settings(prompt_dump_dir=tmp_path))
    prompt_log.dump_prompt(
        label="graph_sql_generation", key="cogs", prompt_hash="h9", system="S", user="U"
    )
    prompt_log.dump_response(
        label="graph_sql_generation", key="cogs", prompt_hash="h9", body="GENERATED SQL HERE"
    )
    text = (tmp_path / "graph_sql_generation" / "cogs.h9.txt").read_text()
    # Same file carries both halves: what the agent saw AND what it produced.
    assert "===== USER =====" in text
    assert "===== RESPONSE =====" in text
    assert "GENERATED SQL HERE" in text


def test_response_noop_when_dir_unset(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(prompt_log, "get_settings", lambda: _Settings(prompt_dump_dir=None))
    prompt_log.dump_response(label="x", key="y", prompt_hash="h", body="b")
    assert not any(tmp_path.iterdir())


def test_unsafe_key_is_slugged(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(prompt_log, "get_settings", lambda: _Settings(prompt_dump_dir=tmp_path))
    prompt_log.dump_prompt(label="x/y", key="a b/c", prompt_hash="h", system=None, user="u")
    # Path traversal / spaces collapsed to a safe slug; one file written.
    files = list(tmp_path.rglob("*.txt"))
    assert len(files) == 1
    assert files[0].parent.name == "x-y"
