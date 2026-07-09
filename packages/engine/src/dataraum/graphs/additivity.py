"""Additivity verdict for a grounded metric (DAT-716).

Deterministic classification of whether a metric's value *reconciles* under
aggregation across an axis class — the drill's grounding for two decisions:
offer a time grain, and sum vs dash a categorical breakdown. No measurement,
no LLM: a pure function over signals the pipeline already computes.

Two independent rules (Kimball / Malloy aggregate locality):

* **function symmetry** — a property of the aggregate alone, on *every* axis:
  ``SUM``/``COUNT(*)``/``COUNT(col)`` reconcile under ``SUM``; ``AVG`` and
  ``COUNT(DISTINCT)`` never do; ``MIN``/``MAX`` are non-summable this cut
  (symmetric under their own function — a later refinement).
* **time semi-additivity** — even ``SUM`` is non-additive *across time* when its
  column is a stock (``temporal_behavior='point_in_time'``); a ``COUNT`` is
  non-additive across time on a **periodic-snapshot** fact (a time column sits in
  the fact's grain, e.g. a trial balance keyed by ``(account, period)``).

The per-extract atoms compose through the metric DAG: a division (ratio) is
non-additive on every axis whatever its operands; a sum/difference of additive
extracts stays additive; any non-additive operand poisons the whole metric.

The ``select_expr`` parse is the verified seam (DAT-713 ``json_serialize_sql`` on
one small single-relation expression — never the composed metric SQL): it holds
only the measure aggregates, so every ``COLUMN_REF`` under an aggregate node is a
base column of that measure, with no filter-column contamination.
"""

from __future__ import annotations

import ast
import json
from dataclasses import dataclass
from typing import Any

import duckdb

# --- reasons a breakdown does not reconcile (surfaced to the drill) -----------
STOCK = "stock"  # SUM of a point-in-time balance — not additive across time
AVERAGE = "average"  # AVG — an average of averages is meaningless
DISTINCT_COUNT = "distinct_count"  # COUNT(DISTINCT) — slices overlap
MIN_MAX = "min_max"  # MIN/MAX — non-summable this cut
SNAPSHOT_COUNT = "snapshot_count"  # COUNT over a periodic-snapshot fact, across time
RATIO = "ratio"  # a formula/extract division or product of measures
UNKNOWN_AGGREGATE = "unknown_aggregate"  # an aggregate outside the doctrine — conservative
UNKNOWN_TEMPORAL = "unknown_temporal"  # an aggregated column with no resolved stock/flow verdict

POINT_IN_TIME = "point_in_time"  # temporal_behavior value marking a stock column
FLOW = "additive"  # temporal_behavior value marking a flow column


@dataclass(frozen=True)
class AggregateCall:
    """One aggregate application inside an extract's ``select_expr``.

    ``function`` is normalized to the doctrine's vocabulary
    (``sum``/``count``/``count_star``/``count_distinct``/``avg``/``min``/``max``
    or the raw lowercase name for anything else). ``columns`` are the base
    columns aggregated (empty for ``COUNT(*)``).
    """

    function: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class AxisClass:
    """Whether a value reconciles under SUM across the two axis classes.

    ``categorical_additive`` — a breakdown by a (grain-safe) categorical
    dimension sums to the unsliced total. ``time_additive`` — a breakdown by a
    time grain sums to the total (the semi-additive question). A reason names
    *why* an axis does not reconcile, for the drill to phrase plainly.
    """

    categorical_additive: bool
    time_additive: bool
    categorical_reason: str | None = None
    time_reason: str | None = None
    #: True only for a pure constant/scalar operand — it scales a measure under
    #: multiplication without changing the measure's additivity (never emitted
    #: as a metric verdict; an internal roll-up marker).
    is_constant: bool = False


ADDITIVE = AxisClass(categorical_additive=True, time_additive=True)
CONSTANT = AxisClass(categorical_additive=True, time_additive=True, is_constant=True)


# =============================================================================
# 1. Parse: select_expr -> aggregate calls
# =============================================================================


def parse_aggregate_calls(select_expr: str, con: duckdb.DuckDBPyConnection) -> list[AggregateCall]:
    """Recover every aggregate call in an extract's ``select_expr``.

    Parses ``SELECT <select_expr>`` with DuckDB's own serializer (a catalog-free
    parse — the columns/relation need not exist) and walks the AST for
    ``FUNCTION`` nodes whose name is a DuckDB aggregate, collecting the
    ``COLUMN_REF`` base columns beneath each. Arithmetic operators and
    ``COALESCE`` are non-aggregate nodes and are recursed *through*, not counted.

    Raises:
        ValueError: the expression does not parse (a malformed catalogue extract
            — surfaced loud rather than silently classified).
    """
    doc = _serialize(select_expr, con)
    agg_names = _aggregate_function_names(con)
    calls: list[AggregateCall] = []
    _collect_aggregates(doc["statements"][0]["node"]["select_list"], agg_names, calls)
    return calls


def select_expr_is_ratio(select_expr: str, con: duckdb.DuckDBPyConnection) -> bool:
    """Whether the extract combines its measures by division or a product of measures.

    A ratio computed inline in ONE ``select_expr`` (``SUM(num) / SUM(den)``) does
    not reconcile under SUM, exactly like a formula-level division — but the flat
    aggregate-call list can't see it. Detected structurally on the AST: a ``/``
    whose denominator subtree contains an aggregate (dividing by a measure —
    scaling by a constant stays additive), or a ``*`` whose BOTH sides contain an
    aggregate (a product of measures). Sums/differences, constant scaling, and
    ``COALESCE`` guards are not ratios.
    """
    doc = _serialize(select_expr, con)
    agg_names = _aggregate_function_names(con)
    return _has_ratio(doc["statements"][0]["node"]["select_list"], agg_names)


def _serialize(select_expr: str, con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Parse ``SELECT <select_expr>`` to DuckDB's JSON AST (a catalog-free parse)."""
    try:
        raw = con.execute("SELECT json_serialize_sql(?)", [f"SELECT {select_expr}"]).fetchone()
    except duckdb.Error as exc:  # pragma: no cover - defensive
        raise ValueError(f"unparseable select_expr {select_expr!r}: {exc}") from exc
    if raw is None:  # pragma: no cover - json_serialize_sql always returns a row
        raise ValueError(f"select_expr {select_expr!r} did not serialize")
    doc: dict[str, Any] = json.loads(raw[0])
    if doc.get("error"):
        raise ValueError(f"unparseable select_expr {select_expr!r}: {doc.get('error_message')}")
    return doc


def _has_ratio(node: Any, agg_names: frozenset[str]) -> bool:
    """A division-by-measure or product-of-measures anywhere in the AST."""
    if isinstance(node, dict):
        if node.get("class") == "FUNCTION":
            fn = node.get("function_name")
            kids = node.get("children") or []
            if fn == "/" and len(kids) == 2 and _has_aggregate(kids[1], agg_names):
                return True
            if (
                fn == "*"
                and len(kids) == 2
                and _has_aggregate(kids[0], agg_names)
                and _has_aggregate(kids[1], agg_names)
            ):
                return True
        return any(_has_ratio(v, agg_names) for v in node.values())
    if isinstance(node, list):
        return any(_has_ratio(item, agg_names) for item in node)
    return False


def _has_aggregate(node: Any, agg_names: frozenset[str]) -> bool:
    """Whether an aggregate FUNCTION appears anywhere in a subtree."""
    if isinstance(node, dict):
        if node.get("class") == "FUNCTION" and node.get("function_name") in agg_names:
            return True
        return any(_has_aggregate(v, agg_names) for v in node.values())
    if isinstance(node, list):
        return any(_has_aggregate(item, agg_names) for item in node)
    return False


_AGG_NAMES: frozenset[str] | None = None


def _aggregate_function_names(con: duckdb.DuckDBPyConnection) -> frozenset[str]:
    """DuckDB's aggregate-function names (memoized; connection-independent).

    The authority for "is this FUNCTION node an aggregate?" — it separates
    ``sum``/``count`` from the arithmetic operators (``-``/``*``) that serialize
    as ``FUNCTION`` nodes too. The set is the same for every connection, so one
    global memo is safe; the unlocked check-then-set is benign (the value is
    idempotent and the GIL makes the assignment atomic — and the metrics phase
    only reaches this after its ThreadPoolExecutor stages have joined).
    """
    global _AGG_NAMES
    if _AGG_NAMES is None:
        rows = con.execute(
            "SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_type='aggregate'"
        ).fetchall()
        _AGG_NAMES = frozenset(r[0] for r in rows)
    return _AGG_NAMES


def _collect_aggregates(node: Any, agg_names: frozenset[str], out: list[AggregateCall]) -> None:
    """Recurse the AST, appending an ``AggregateCall`` per aggregate FUNCTION.

    An aggregate's own arguments are scanned for columns but not for further
    aggregates (aggregates do not nest); every other node is recursed through.
    """
    if isinstance(node, dict):
        if node.get("class") == "FUNCTION" and node.get("function_name") in agg_names:
            out.append(
                AggregateCall(
                    function=_normalize_function(node),
                    columns=tuple(_column_refs(node.get("children", []))),
                )
            )
            return
        for value in node.values():
            _collect_aggregates(value, agg_names, out)
    elif isinstance(node, list):
        for item in node:
            _collect_aggregates(item, agg_names, out)


def _normalize_function(node: dict[str, Any]) -> str:
    """Map a FUNCTION node to the doctrine vocabulary.

    ``count`` + ``distinct`` → ``count_distinct``; everything else its lowercase name.
    """
    name = str(node.get("function_name", "")).lower()
    if name == "count" and node.get("distinct"):
        return "count_distinct"
    return name


def _column_refs(node: Any) -> list[str]:
    """Every ``COLUMN_REF`` base column beneath a node.

    The last name in a ``COLUMN_REF`` is the column, stripping any ``table.`` qualifier.
    """
    cols: list[str] = []

    def rec(n: Any) -> None:
        if isinstance(n, dict):
            if n.get("class") == "COLUMN_REF":
                names = n.get("column_names") or []
                if names:
                    cols.append(names[-1])
            else:
                for value in n.values():
                    rec(value)
        elif isinstance(n, list):
            for item in n:
                rec(item)

    rec(node)
    return cols


# =============================================================================
# 2. Classify: an extract's aggregate calls -> AxisClass
# =============================================================================


def classify_extract(
    calls: list[AggregateCall],
    temporal_by_column: dict[str, str | None],
    fact_is_snapshot: bool | None,
    *,
    is_ratio: bool = False,
) -> AxisClass:
    """The additivity of one extract — the most-restrictive of its aggregate calls.

    ``temporal_by_column`` maps each base column to its ``temporal_behavior``
    (``'additive'`` / ``'point_in_time'``); a column ABSENT from the map (or
    mapping to ``None``) has no resolved verdict and is treated conservatively.
    ``fact_is_snapshot`` is True for a periodic-snapshot fact (a time column in
    the grain), False for a confirmed event fact, ``None`` when the grain is
    unknown — both snapshot and unknown deny ``COUNT`` the time axis.
    ``is_ratio`` marks an extract whose measures are combined by division/product
    inline in one ``select_expr`` (a ratio) — non-additive on every axis. An
    extract with no aggregate (a bare passthrough) is treated as additive.
    """
    if is_ratio:
        return AxisClass(False, False, RATIO, RATIO)
    # A bare COUNT(*) alongside a column-bearing aggregate is a NULL-presence guard
    # (``CASE WHEN COUNT(*)=0 THEN NULL ELSE <measure> END``), not the measure — the
    # value is the other aggregate. Drop it so the guard can't strip time or taint
    # the reason (a stock balance reads ``stock``, not ``snapshot_count``). A
    # COUNT(*) ALONE is a genuine count measure and is kept. This targets the one
    # guard idiom the grounding prompt emits (COUNT(*)); a COUNT(1)/COUNT(col) guard
    # would not be recognized, but the prompt never produces those.
    measures = [c for c in calls if not (c.function == "count_star" and not c.columns)]
    result = ADDITIVE
    for call in measures or calls:
        result = most_restrictive(
            result, _classify_call(call, temporal_by_column, fact_is_snapshot)
        )
    return result


def _classify_call(
    call: AggregateCall, temporal_by_column: dict[str, str | None], fact_is_snapshot: bool | None
) -> AxisClass:
    fn = call.function
    if fn == "sum":
        behaviors = [temporal_by_column.get(c) for c in call.columns]
        if any(b == POINT_IN_TIME for b in behaviors):
            # A summed balance reconciles across categories but not across time.
            return AxisClass(True, False, None, STOCK)
        if call.columns and all(b == FLOW for b in behaviors):
            return AxisClass(True, True)
        # A column with no resolved flow/stock verdict (no concept row / NULL) —
        # can't confirm it sums across time; refuse it, never assume flow.
        return AxisClass(True, False, None, UNKNOWN_TEMPORAL)
    if fn in ("count_star", "count"):
        # Counting is additive across categorical; across time only on a CONFIRMED
        # event fact — a snapshot (or an unknown grain) recounts the same population.
        if fact_is_snapshot is False:
            return AxisClass(True, True)
        return AxisClass(
            True, False, None, SNAPSHOT_COUNT if fact_is_snapshot else UNKNOWN_TEMPORAL
        )
    if fn == "count_distinct":
        return AxisClass(False, False, DISTINCT_COUNT, DISTINCT_COUNT)
    if fn == "avg":
        return AxisClass(False, False, AVERAGE, AVERAGE)
    if fn in ("min", "max"):
        return AxisClass(False, False, MIN_MAX, MIN_MAX)
    # An aggregate outside the doctrine — do not guess it reconciles.
    return AxisClass(False, False, UNKNOWN_AGGREGATE, UNKNOWN_AGGREGATE)


def most_restrictive(a: AxisClass, b: AxisClass) -> AxisClass:
    """Combine two classes conservatively.

    An axis is additive only if both are; a reason is carried from whichever side
    made the axis non-additive.
    """
    return AxisClass(
        categorical_additive=a.categorical_additive and b.categorical_additive,
        time_additive=a.time_additive and b.time_additive,
        categorical_reason=a.categorical_reason
        or (b.categorical_reason if not b.categorical_additive else None),
        time_reason=a.time_reason or (b.time_reason if not b.time_additive else None),
    )


# =============================================================================
# 3. Roll up: the metric DAG -> one verdict
# =============================================================================


@dataclass(frozen=True)
class MetricVerdict:
    """A metric's additivity, as the drill reads it."""

    categorical_additive: bool
    time_additive: bool
    categorical_reason: str | None = None
    time_reason: str | None = None


def roll_up_metric(graph: Any, extract_class_by_step: dict[str, AxisClass]) -> MetricVerdict:
    """Compose the per-extract classes through the metric DAG to one verdict.

    ``graph`` is a ``TransformationGraph``; ``extract_class_by_step`` gives the
    ``AxisClass`` of each EXTRACT leaf (from :func:`classify_extract`). Walks the
    output step: a FORMULA combines its dependencies by operator (a division or a
    product of two measures ⇒ ratio, non-additive on every axis; add/subtract or
    scale-by-constant ⇒ combine the operands); a CONSTANT is a scalar identity.
    """
    output = graph.get_output_step()
    if output is None:
        # No output marker — fall to the most-restrictive over whatever we classified.
        result = ADDITIVE
        for cls in extract_class_by_step.values():
            result = most_restrictive(result, cls)
        return _to_verdict(result)
    return _to_verdict(_step_class(output.step_id, graph, extract_class_by_step, frozenset()))


def _step_class(
    step_id: str,
    graph: Any,
    extract_class_by_step: dict[str, AxisClass],
    seen: frozenset[str],
) -> AxisClass:
    from dataraum.graphs.models import StepType

    if step_id in seen:
        # A FORMULA→FORMULA cycle. Guarded upstream by agent.assemble's dependency
        # ordering before a metric reaches `executed`, but never recurse unbounded
        # on that assumption — refuse.
        return AxisClass(False, False, UNKNOWN_AGGREGATE, UNKNOWN_AGGREGATE)
    step = graph.steps.get(step_id)
    if step is None:
        # A referenced-but-absent dependency — conservative.
        return AxisClass(False, False, UNKNOWN_AGGREGATE, UNKNOWN_AGGREGATE)
    if step.step_type == StepType.EXTRACT:
        # An extract absent from the map never grounded (the resolver skips a
        # source-less/ungrounded leaf) — refuse conservatively, never assume additive.
        return extract_class_by_step.get(
            step_id, AxisClass(False, False, UNKNOWN_AGGREGATE, UNKNOWN_AGGREGATE)
        )
    if step.step_type == StepType.CONSTANT:
        return CONSTANT
    # FORMULA: classify its expression over the dependency step ids.
    try:
        tree = ast.parse(step.expression or "", mode="eval")
    except SyntaxError:
        return AxisClass(False, False, RATIO, RATIO)
    return _expr_class(tree.body, graph, extract_class_by_step, seen | {step_id})


def _expr_class(
    node: ast.expr, graph: Any, extract_class_by_step: dict[str, AxisClass], seen: frozenset[str]
) -> AxisClass:
    if isinstance(node, ast.Name):
        return _step_class(node.id, graph, extract_class_by_step, seen)
    if isinstance(node, ast.Constant):
        return CONSTANT
    if isinstance(node, ast.UnaryOp):
        return _expr_class(node.operand, graph, extract_class_by_step, seen)
    if isinstance(node, ast.BinOp):
        left = _expr_class(node.left, graph, extract_class_by_step, seen)
        right = _expr_class(node.right, graph, extract_class_by_step, seen)
        if isinstance(node.op, ast.Div):
            # A ratio never reconciles under SUM, whatever its operands.
            return AxisClass(False, False, RATIO, RATIO)
        if isinstance(node.op, ast.Mult):
            # Scaling a measure by a constant preserves its additivity; a product
            # of two measures does not.
            if left.is_constant:
                return right
            if right.is_constant:
                return left
            return AxisClass(False, False, RATIO, RATIO)
        # Add / Sub: additive combination — reconciles only where both operands do.
        combined = most_restrictive(left, right)
        return AxisClass(
            combined.categorical_additive,
            combined.time_additive,
            combined.categorical_reason,
            combined.time_reason,
            is_constant=left.is_constant and right.is_constant,
        )
    # Any other node shape — conservative.
    return AxisClass(False, False, RATIO, RATIO)


def _to_verdict(cls: AxisClass) -> MetricVerdict:
    return MetricVerdict(
        categorical_additive=cls.categorical_additive,
        time_additive=cls.time_additive,
        categorical_reason=cls.categorical_reason,
        time_reason=cls.time_reason,
    )
