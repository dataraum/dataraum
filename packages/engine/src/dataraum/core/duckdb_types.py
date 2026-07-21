"""Type-family predicates over a column's ``resolved_type`` (DAT-835).

``Column.resolved_type`` is a **DuckDB type name**, not a :class:`DataType` member.
On the inference path it happens to coincide with one (``resolve_types`` writes
``spec.data_type.value``), but on the strongly-typed path ``typing_phase`` trusts
the source and stores DuckDB's verbatim ``DESCRIBE`` name — ``DECIMAL(18,2)``,
``FLOAT``, ``TIMESTAMP_NS``, ``TIMESTAMP WITH TIME ZONE``. ``DataType`` has no
width or precision variants, so none of those are enum members and none of them
match a hardcoded literal list.

Before this module, eight consumers each answered "is this numeric / temporal"
their own way. Five compared against ``["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]``
with exact string equality, which on a parquet source silently drops FLOAT
(float32 — the ordinary parquet float), every parameterized DECIMAL (every money
column in a real warehouse), TINYINT and SMALLINT. Two compared temporal columns
against ``["DATE", "TIMESTAMP", "TIMESTAMPTZ"]``, which drops ``TIMESTAMP_NS``
(what pandas writes by default) and never matches a timezone-aware column at all —
DuckDB spells that ``TIMESTAMP WITH TIME ZONE``, so the literal ``"TIMESTAMPTZ"``
was dead. Nothing failed; the columns were simply absent from statistics, quality,
derived-column discovery and temporal profiling.

The CSV corpora could not surface any of it: they load VARCHAR-first, resolve
through ``DataType``, and produce exactly the four bare names the lists expected.

So the predicate lives here once, and every consumer asks the same question.

Three questions, deliberately distinct — they are not interchangeable:

* :func:`is_numeric` — can this column be summed / averaged / profiled numerically?
* :func:`is_time_point` — is this a point on a time axis (min/max/gap/cadence are
  meaningful)? A duration is NOT: ``INTERVAL`` has no position in time, and
  ``TIME`` has no date, so neither bounds a data window.
* :func:`is_datetime_like` — is this temporal in ANY sense, durations and
  times-of-day included? The right question for "does the type agree with a
  ``timestamp`` semantic role", the wrong one for "profile this axis".
"""

from __future__ import annotations

from typing import Final

#: Summable numerics, by DuckDB family name. Unsigned variants are included:
#: a ``UBIGINT`` is a numeric by any reading, and omitting it is the same defect
#: this module exists to close. ``REAL``/``NUMERIC`` are DuckDB aliases that
#: ``DESCRIBE`` folds to ``FLOAT``/``DECIMAL``, kept so a name that reaches us
#: from anywhere else still resolves.
NUMERIC_TYPES: Final[frozenset[str]] = frozenset(
    {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "UHUGEINT",
        "FLOAT",
        "REAL",
        "DOUBLE",
        "DECIMAL",
        "NUMERIC",
    }
)

#: Point-in-time types — the ones that bound a data window. Every spelling is
#: listed literally because DuckDB timestamp names carry NO parameters (it emits
#: ``TIMESTAMP_NS``, never ``TIMESTAMP(9)``), so a literal set is complete and can
#: also be pushed into SQL as an ``IN`` list. ``TIMESTAMP WITH TIME ZONE`` is what
#: ``DESCRIBE`` returns for a ``TIMESTAMPTZ`` column; both spellings are here
#: because ``DataType.TIMESTAMPTZ`` produces the short one.
TIME_POINT_TYPES: Final[frozenset[str]] = frozenset(
    {
        "DATE",
        "DATETIME",
        "TIMESTAMP",
        "TIMESTAMP_NS",
        "TIMESTAMP_MS",
        "TIMESTAMP_S",
        "TIMESTAMPTZ",
        "TIMESTAMP WITH TIME ZONE",
    }
)

#: Durations and times-of-day: temporal, but with no position on a time axis.
_DURATION_TYPES: Final[frozenset[str]] = frozenset(
    {"TIME", "TIMETZ", "TIME WITH TIME ZONE", "INTERVAL"}
)

#: Anything temporal at all — the union. Used where the question is "is the type
#: temporal in kind", not "can I profile a window over it".
DATETIME_LIKE_TYPES: Final[frozenset[str]] = TIME_POINT_TYPES | _DURATION_TYPES


def family(resolved_type: str | None) -> str:
    """The bare type family of a DuckDB type name, upper-cased.

    Strips precision/scale parameters and surrounding whitespace, so
    ``"decimal(18,2)"`` and ``" DECIMAL "`` both yield ``"DECIMAL"``. A ``None``
    (an unresolved column) yields ``""``, which is in no family — absence is
    never silently a match.

    Composite types keep their head (``STRUCT(a INTEGER)`` → ``STRUCT``), which
    is correct: a struct is not a numeric no matter what it contains.
    """
    if resolved_type is None:
        return ""
    return resolved_type.split("(")[0].strip().upper()


def is_numeric(resolved_type: str | None) -> bool:
    """Can this column be summed, averaged, or numerically profiled?"""
    return family(resolved_type) in NUMERIC_TYPES


def is_time_point(resolved_type: str | None) -> bool:
    """Is this a position on a time axis — so min/max/gaps/cadence mean something?

    ``INTERVAL`` and ``TIME`` are deliberately excluded: a duration has no
    position and a time-of-day has no date, so neither bounds a data window.
    Use :func:`is_datetime_like` when the question is merely "is it temporal".
    """
    return family(resolved_type) in TIME_POINT_TYPES


def is_datetime_like(resolved_type: str | None) -> bool:
    """Is this temporal in any sense, durations and times-of-day included?"""
    return family(resolved_type) in DATETIME_LIKE_TYPES
