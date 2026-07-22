"""SQLAlchemy models for validation.

Two homes with different lifecycles:

* :class:`Validation` — the workspace's typed *validation vocabulary* (DAT-735):
  declaration-versioned, keyed ``(vertical, validation_id)``, written by the seed
  (shipped YAML) and by agentic induction. The DAT-789 ``Convention`` typed-home
  pattern applied to validation specs, so the check LOGIC gets a typed home
  instead of living as free ``sql_hints`` text.
* :class:`ValidationResultRecord` — one run-versioned grounded SQL per check
  (ADR-0017), the pure SQL store whose verdict is recomputed on demand.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Float,
    Index,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.analysis.validation.models import ValidationSeverity
from dataraum.storage import Base

# Closed-vocabulary CHECK values (DAT-802 enum-standard sweep): the severity
# vocabulary derives from its single-home enum so the CHECK and the enum can
# never drift (the DAT-784 pattern). Sorted for a deterministic offline DDL dump.
_VALIDATION_SEVERITY_VALUES: tuple[str, ...] = tuple(sorted(v.value for v in ValidationSeverity))


class Validation(Base):
    """The workspace's typed validation vocabulary — one home (DAT-735).

    A validation is a named data-quality/business rule with a TYPED check
    definition — ``check_type`` + ``tolerance`` (the ADR-0017 verdict param,
    ``deviation <= tolerance``) — plus advisory ``guidance`` prose for the
    SQL-binding agent (the former free-text ``sql_hints``, which is NO LONGER the
    check's definition). Config→DB, the same cut :class:`~dataraum.analysis.
    semantic.db_models.Convention` took (DAT-789): the shipped vertical YAML is the
    *seed* (source='seed'), normalized into typed rows at connect; agentic
    induction (:mod:`~dataraum.analysis.validation.induction`) proposes more rows
    over the served graph (source='generated'). The validation phase reads these
    rows (never the YAML directory walk), so a *framed* vertical whose validations
    exist only as rows is served identically to a builtin.

    **Identity contract — NOT run-versioned (the DAT-728 pattern).** A validation
    is a stable node keyed by ``(vertical, validation_id)``; ``row_id`` is a
    workspace-stable surrogate minted once, NOT a per-run uuid. Re-induction
    supersedes rather than collides: the ``uq_validation_active`` partial-unique
    index keeps at most one *active* row per ``(vertical, validation_id)`` so a
    head-free read is unambiguous. Workspace identity IS the ``ws_<id>`` schema (no
    ``workspace_id`` column); the read surface scopes to the workspace's bound
    ``active_vertical`` (``_VERTICAL_SCOPED`` in ``storage/read_views.py``).

    The teach overlay (frame-2 ``validation`` config_overlay rows, DAT-441) is a
    SEPARATE layer ``⊕``'d over these rows at read time — it is NOT a ``source``
    here, because no writer lands teach rows in THIS table yet (the DAT-802
    live-writer discipline: admit only sources a writer produces).
    """

    __tablename__ = "validations"
    __table_args__ = (
        # At most one ACTIVE row per (vertical, validation_id); superseded history
        # rows are exempt. The deterministic single-active-row guarantee the
        # head-free reads and the seed's ON CONFLICT DO NOTHING rely on — the same
        # shape as Convention.uq_convention_active.
        Index(
            "uq_validation_active",
            "vertical",
            "validation_id",
            unique=True,
            postgresql_where=text("superseded_at IS NULL"),
            sqlite_where=text("superseded_at IS NULL"),
        ),
        # Lifecycle-source vocabulary (DAT-802, the two-layer standard): every
        # admitted value has a LIVE writer — 'seed'
        # (``validation_store.ensure_validations_seeded``, engine) and 'generated'
        # (``validation_store.persist_generated_validations``, the agentic-induction
        # writer). NOT 'frame'/'teach': the cockpit teach path writes config_overlay
        # rows (the ⊕ layer), not this table — a CHECK admitting a value no writer
        # produces is the exact DAT-802 defect. Widening is one line + a re-dump in
        # the PR that adds the writer.
        CheckConstraint("source IS NULL OR source IN ('generated', 'seed')", name="source"),
        # Severity vocabulary (DAT-802, the two-layer standard): derived from
        # :class:`ValidationSeverity`, the single home, so the CHECK and enum can
        # never drift. NOT NULL — every validation declares a severity (the seed and
        # induction both always supply one).
        CheckConstraint(
            "severity IN (" + ", ".join(f"'{v}'" for v in _VALIDATION_SEVERITY_VALUES) + ")",
            name="severity",
        ),
    )

    row_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    vertical: Mapped[str] = mapped_column(String, nullable=False)
    # The validation's stable identifier within `vertical` (the YAML `validation_id`).
    validation_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    # Open vocabulary (induction/teach may extend), validated at the Pydantic
    # contract layer, not the DB — mirrors ValidationSpec keeping these free.
    category: Mapped[str] = mapped_column(String, nullable=False)
    # Closed vocab: see ck_validations_severity (ValidationSeverity).
    severity: Mapped[str] = mapped_column(String, nullable=False)
    check_type: Mapped[str] = mapped_column(String, nullable=False)

    # The TYPED check definition (DAT-735). ``tolerance`` is the ADR-0017 verdict
    # param (``deviation <= tolerance``); NULL ⇒ the evaluator's DEFAULT_TOLERANCE.
    tolerance: Mapped[float | None] = mapped_column(Float)
    # Advisory SQL-binding hint prose — the former ``sql_hints``. Served to the
    # binding agent, NEVER the check's definition (that is check_type + tolerance).
    guidance: Mapped[str | None] = mapped_column(Text)
    expected_outcome: Mapped[str | None] = mapped_column(Text)

    # cycle types this validation applies to; empty/NULL = universal.
    relevant_cycles: Mapped[list[str] | None] = mapped_column(JSON)
    tags: Mapped[list[str] | None] = mapped_column(JSON)
    version: Mapped[str] = mapped_column(String, nullable=False, default="1.0")

    # Lifecycle: workspace-persistent with supersession (NULL superseded_at = active).
    # Closed vocab: see ck_validations_source — 'seed' | 'generated' are the live writers.
    source: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime)


class ValidationResultRecord(Base):
    """A single validation's grounded SQL for a run — a pure SQL store (ADR-0017).

    Run-versioned (DAT-438): one row per ``(session, validation, run)``. A
    re-run supersedes by writing rows under its fresh ``run_id``; readers scope
    to the promoted ``operating_model`` head (or, in-run, to this run's id),
    never across runs.

    The pass/fail VERDICT is **not** stored — a stored verdict goes stale the
    moment data is re-imported, the SQL does not (DAT-617). Neither are the
    declared judgement params (``severity``/``tolerance``): those live in the
    vertical config, read via the spec reader at every consumer. This record is
    just the durable run-versioned ``sql_used`` (+ the columns it touched); the
    verdict is recomputed on demand by re-running it (``validation/evaluate.py``).
    """

    __tablename__ = "validation_results"
    __table_args__ = (UniqueConstraint("validation_id", "run_id", name="uq_validation_result_run"),)

    result_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    validation_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    table_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    # The "table.column" names the generated SQL actually touched (LLM-declared
    # at bind time, DAT-432) — the entropy detector bands these columns when
    # the recomputed verdict is a failure.
    columns_used: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)

    # The grounded SQL (the durable artifact) + when it was bound for this run.
    sql_used: Mapped[str | None] = mapped_column(Text, nullable=True)
    executed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


__all__ = [
    "Validation",
    "ValidationResultRecord",
]
