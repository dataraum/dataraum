"""Tests for ``dataraum.server.workspace.bootstrap_workspace``.

Backed by an in-memory SQLite engine so the SQLAlchemy roundtrip is
exercised without a Postgres dependency. Workspace shape is dialect-
agnostic (String PK, DateTime, no Postgres-specific types) so the
SQLite proxy is a fair stand-in for the production Postgres path.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.core.config import (
    _get_config_root,
    reset_active_workspace_for_tests,
    reset_config_root,
    set_config_root,
)
from dataraum.server.workspace import bootstrap_workspace
from dataraum.storage import Workspace


@pytest.fixture
def baked_in_config(tmp_path: Path) -> Path:
    """A minimal baked-in config tree the bootstrap copies from."""
    src = tmp_path / "baked_in_config"
    (src / "phases").mkdir(parents=True)
    (src / "phases" / "import.yaml").write_text("junk_columns: []\n")
    (src / "pipeline.yaml").write_text("phases: {}\npipeline: {}\n")
    (src / "verticals" / "finance").mkdir(parents=True)
    (src / "verticals" / "finance" / "ontology.yaml").write_text("concepts: []\n")
    return src


@pytest.fixture
def home_dir(tmp_path: Path) -> Path:
    home = tmp_path / "datahome"
    home.mkdir()
    return home


@pytest.fixture
def session_factory() -> Iterator[Callable[[], AbstractContextManager[Session]]]:
    """In-memory SQLite session factory with the Workspace table created.

    StaticPool keeps the single connection alive across session_scope
    calls — the in-memory database evaporates otherwise (per the
    feedback memory: Python 3.12+ ResourceWarning on QueuePool GC).

    Imports every phase + storage db_models module so SQLAlchemy can
    resolve the string forward references on ``Source.tables`` /
    ``Table.entity_detections`` / etc when the first query runs.
    """
    # Mapper-registration imports (mirror of init_database) — needed so
    # SQLAlchemy can configure the mapper graph even though we only
    # materialize the Workspace table.
    from dataraum.documentation import db_models as _fixes  # noqa: F401
    from dataraum.investigation import db_models as _investigation  # noqa: F401
    from dataraum.pipeline import db_models as _pipeline  # noqa: F401
    from dataraum.pipeline.registry import import_all_phase_models
    from dataraum.query import db_models as _query  # noqa: F401
    from dataraum.query import snippet_models as _snippets  # noqa: F401
    from dataraum.storage import models as _storage_models  # noqa: F401

    import_all_phase_models()

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Workspace.__table__.create(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    @contextmanager
    def factory() -> Iterator[Session]:
        sess = SessionLocal()
        try:
            yield sess
            sess.commit()
        except Exception:
            sess.rollback()
            raise
        finally:
            sess.close()

    yield factory
    engine.dispose()


@pytest.fixture(autouse=True)
def _isolate_active_workspace() -> Iterator[None]:
    """Reset the module-level active-workspace pointer between tests."""
    yield
    reset_active_workspace_for_tests()
    reset_config_root()


@pytest.fixture
def pointed_at_baked_in(
    baked_in_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    """Make ``_get_config_root()`` return the baked-in fixture tree.

    Uses ``set_config_root`` (the top-priority override) rather than
    ``DATARAUM_CONFIG_PATH`` so the test is robust against the env var
    being unset/inherited from the harness.
    """
    set_config_root(baked_in_config)
    yield baked_in_config


def test_bootstrap_creates_default_workspace_on_empty_db(
    session_factory: Callable[[], AbstractContextManager[Session]],
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))

    ws = bootstrap_workspace(session_factory)

    assert ws.name == "default"
    assert ws.workspace_id  # uuid present
    expected_config_dir = home_dir / "workspaces" / ws.workspace_id / "config"
    assert Path(ws.config_dir) == expected_config_dir
    assert expected_config_dir.is_dir()


def test_bootstrap_copies_baked_in_config_on_first_boot(
    session_factory: Callable[[], AbstractContextManager[Session]],
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))

    ws = bootstrap_workspace(session_factory)

    overlay = Path(ws.config_dir)
    assert (overlay / "pipeline.yaml").read_text() == "phases: {}\npipeline: {}\n"
    assert (overlay / "phases" / "import.yaml").read_text() == "junk_columns: []\n"
    assert (overlay / "verticals" / "finance" / "ontology.yaml").exists()


def test_bootstrap_activates_workspace_as_config_root(
    session_factory: Callable[[], AbstractContextManager[Session]],
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))

    ws = bootstrap_workspace(session_factory)

    # The set_config_root() override would still win; drop it so we can
    # observe the active-workspace step.
    reset_config_root()
    assert _get_config_root() == Path(ws.config_dir)


def test_bootstrap_creates_adhoc_vertical_scaffold(
    session_factory: Callable[[], AbstractContextManager[Session]],
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))

    ws = bootstrap_workspace(session_factory)

    adhoc = Path(ws.config_dir) / "verticals" / "_adhoc"
    assert adhoc.is_dir()
    assert (adhoc / "ontology.yaml").exists()
    assert (adhoc / "cycles.yaml").exists()
    assert (adhoc / "validations").is_dir()
    assert (adhoc / "metrics").is_dir()


def test_bootstrap_reuses_existing_workspace_and_does_not_overwrite(
    session_factory: Callable[[], AbstractContextManager[Session]],
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Subsequent boots must not stomp teach edits already on disk."""
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))

    first = bootstrap_workspace(session_factory)
    overlay = Path(first.config_dir)
    teach_edit = overlay / "phases" / "import.yaml"
    teach_edit.write_text("junk_columns:\n  - id\n# edited by teach\n")
    reset_active_workspace_for_tests()

    second = bootstrap_workspace(session_factory)

    assert second.workspace_id == first.workspace_id
    assert (
        teach_edit.read_text()
        == "junk_columns:\n  - id\n# edited by teach\n"
    ), "second boot overwrote teach edits"


def test_bootstrap_picks_lowest_created_at_when_multiple_workspaces(
    session_factory: Callable[[], AbstractContextManager[Session]],
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))

    older_id = str(uuid4())
    older_dir = home_dir / "workspaces" / older_id / "config"
    older_dir.mkdir(parents=True)
    (older_dir / "marker.yaml").write_text("older: true\n")

    newer_id = str(uuid4())
    newer_dir = home_dir / "workspaces" / newer_id / "config"
    newer_dir.mkdir(parents=True)

    from datetime import UTC, datetime, timedelta

    older = Workspace(
        workspace_id=older_id,
        name="older",
        config_dir=str(older_dir),
        created_at=datetime.now(UTC) - timedelta(hours=1),
    )
    newer = Workspace(
        workspace_id=newer_id,
        name="newer",
        config_dir=str(newer_dir),
        created_at=datetime.now(UTC),
    )
    with session_factory() as session:
        session.add(older)
        session.add(newer)

    picked = bootstrap_workspace(session_factory)

    assert picked.workspace_id == older_id
    assert picked.name == "older"


def test_bootstrap_raises_when_datatraum_home_unset(
    session_factory: Callable[[], AbstractContextManager[Session]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATARAUM_HOME", raising=False)

    with pytest.raises(RuntimeError, match="DATARAUM_HOME is not set"):
        bootstrap_workspace(session_factory)


def test_bootstrap_adhoc_scaffold_is_idempotent(
    session_factory: Callable[[], AbstractContextManager[Session]],
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))

    bootstrap_workspace(session_factory)
    reset_active_workspace_for_tests()

    # second call should not raise even though _adhoc already exists
    bootstrap_workspace(session_factory)
