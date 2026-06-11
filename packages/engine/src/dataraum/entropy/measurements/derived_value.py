"""Derived-formula adjudication — the second witness for derived_value (ADR-0009).

Is a column governed by a within-table arithmetic formula? For each canonical
formula identity in play the claim space is {``holds``, ``fails``} — "this
formula governs the column's values" — and up to two witnesses opine:

* **formula discovery** — the DATA witness (exists today): the correlation
  phase's deterministic formula search read every row, and its match rate IS its
  belief that the formula holds. For a formula the discovery did not find but
  the LLM hypothesized, the SAME row statistic (computed by the detector's
  loader over the typed table) grades the hypothesis; when it cannot be graded
  (non-numeric target, unknown source columns) the witness abstains.
* **LLM hypothesis** — the NEW witness: the formula the column *should* obey
  from its name + concept context (``derived_formula_hypothesis``, produced in
  ``semantic_per_column``). It leans ``holds`` on its own canonical formula,
  scaled by its confidence, and ABSTAINS on every other formula: not having
  thought of the discovered formula is no evidence against it (the type_claim
  lesson — absence is not dissent). It abstains everywhere when no hypothesis.

The divergence case is the whole point: the LLM confidently expects
``subtotal + tax`` but the data grades that formula broken while the discovery
found ``subtotal * tax_rate`` → conflict ``C`` rises on the hypothesis slot →
``investigate``. The collinear case stays quiet: a hypothesis that ALSO holds in
the data agrees with its grading. Formulas are compared as canonical structures
(sqlglot-parsed, commutative operands sorted), never raw strings.

Pure module: no DB, no LLM, no config, no tunable numbers — witness leans are
``0.5 + 0.5·confidence`` / the measured match rate itself. Reliabilities are
documented placeholder priors (artifact: dataraum-config/entropy/
reliabilities.yaml), calibrated later by the eval rig — not tuned to a metric.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import SqlglotError

from dataraum.entropy.pooling import PoolResult, Witness, pool

# The canonical claim space. Order fixes the tuple layout passed to the pool.
CLAIM_SPACE: tuple[str, str] = ("holds", "fails")

# A witness within this of uniform is ABSTAINING — dropped before pooling
# (abstention is ignorance, not disagreement; same convention as the siblings).
_OPINION_EPS = 1e-6

# The discovery's formula language: binary arithmetic over two columns. The
# canonical operation names match DerivedColumn.derivation_type; commutative
# operations sort their operands (the discovery's own dedup convention).
OPERATION_SYMBOL: dict[str, str] = {
    "sum": "+",
    "difference": "-",
    "product": "*",
    "ratio": "/",
}
_COMMUTATIVE = frozenset({"sum", "product"})
_NODE_OPERATION: dict[type[exp.Expr], str] = {
    exp.Add: "sum",
    exp.Sub: "difference",
    exp.Mul: "product",
    exp.Div: "ratio",
}

# Neutral uncalibrated FALLBACK — used only when no reliabilities are threaded in
# (direct/test callers). The SHIPPED values live in the artifact
# dataraum-config/entropy/reliabilities.yaml (placeholder priors until the eval
# rig runs for this measurement) and are passed via ``reliabilities=``. Per
# ADR-0009 the shipped r are estimated-with-provenance, never inline constants.
DEFAULT_RELIABILITIES: dict[str, float] = {
    "formula_discovery": 0.9,
    "llm_hypothesis": 0.6,
}


@dataclass(frozen=True)
class CanonicalFormula:
    """A binary within-table formula in canonical form.

    ``operation`` is the derivation-type name (``sum`` / ``difference`` /
    ``product`` / ``ratio``); ``operands`` are lowercased column names, sorted
    for commutative operations so ``a + b`` and ``b + a`` share one identity.
    """

    operation: str
    operands: tuple[str, str]

    @property
    def identity(self) -> str:
        """The canonical claim identity, e.g. ``"sum(net,tax)"``."""
        return f"{self.operation}({self.operands[0]},{self.operands[1]})"


def _canonical(operation: str, left: str, right: str) -> CanonicalFormula:
    a, b = left.strip().lower(), right.strip().lower()
    if operation in _COMMUTATIVE and b < a:
        a, b = b, a
    return CanonicalFormula(operation=operation, operands=(a, b))


def parse_formula(formula: str | None) -> CanonicalFormula | None:
    """Parse a formula string into its canonical structure (claims are canonical).

    Accepts the discovery's language: exactly one of ``+ - * /`` between two
    column references (parenthesised or aliased forms unwrap; a top-level
    ``target = expr`` equation takes the expression side). Anything else —
    functions, literals, more than two operands, unparseable text — returns
    ``None``: the formula cannot be mapped into the claim space, so a witness
    holding it must abstain rather than manufacture an ungrounded claim.
    """
    if not formula or not formula.strip():
        return None
    try:
        node = sqlglot.parse_one(formula, dialect="duckdb")
    except SqlglotError:
        return None
    if node is None:
        return None
    node = _unwrap(node)
    operation = _NODE_OPERATION.get(type(node))
    if operation is None:
        return None
    raw_left = node.args.get("this")
    raw_right = node.args.get("expression")
    if not isinstance(raw_left, exp.Expression) or not isinstance(raw_right, exp.Expression):
        return None
    left = _unwrap(raw_left)
    right = _unwrap(raw_right)
    if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
        return None
    if not left.name or not right.name:
        return None
    return _canonical(operation, left.name, right.name)


def _unwrap(node: exp.Expr) -> exp.Expr:
    """Strip syntax that carries no formula structure (parens, aliases, ``=``)."""
    while True:
        if isinstance(node, exp.Paren | exp.Alias):
            node = node.this
        elif isinstance(node, exp.EQ):
            # "target = a + b": the equation side that is not a bare column is
            # the formula; a bare ``col = col`` carries no derivation.
            left, right = node.this, node.expression
            if isinstance(left, exp.Column) and not isinstance(right, exp.Column):
                node = right
            elif isinstance(right, exp.Column) and not isinstance(left, exp.Column):
                node = left
            else:
                return node
        else:
            return node


def canonicalize_discovered(entry: Mapping[str, Any]) -> CanonicalFormula | None:
    """Canonical form of one discovered ``DerivedColumn`` row.

    Parses the human-readable ``formula`` (always ``"a op b"`` from the
    discovery); falls back to ``derivation_type`` + ``source_column_names`` when
    the formula string does not parse (e.g. source names with spaces).
    """
    canonical = parse_formula(str(entry.get("formula") or ""))
    if canonical is not None:
        return canonical
    operation = str(entry.get("derivation_type") or "")
    sources = [str(s) for s in (entry.get("source_column_names") or []) if str(s).strip()]
    if operation in OPERATION_SYMBOL and len(sources) == 2:
        return _canonical(operation, sources[0], sources[1])
    return None


@dataclass(frozen=True)
class FormulaAdjudication:
    """The pooled holds/fails verdict for one formula claim on one column."""

    table: str
    column: str
    formula: str  # canonical identity (or "raw:<text>" for an unparseable discovery)
    formula_display: str  # the human-readable formula string
    claim_field: str  # "derived_formula:{table}.{column}:{formula}"
    discovered: bool
    hypothesized: bool
    match_rate: float | None  # the row grading the data witness used (None = ungraded)
    witnesses: tuple[Witness, ...]
    result: PoolResult


def _distribution(p_holds: float) -> dict[str, float]:
    """A claim-space distribution from P(holds), clamped to [0, 1]."""
    p = min(1.0, max(0.0, p_holds))
    return {"holds": p, "fails": 1.0 - p}


def _witness(witness_id: str, distribution: Mapping[str, float], reliability: float) -> Witness:
    return Witness(
        witness_id=witness_id,
        distribution=tuple(distribution[label] for label in CLAIM_SPACE),
        reliability=reliability,
    )


def _has_opinion(witness: Witness) -> bool:
    """A witness has an opinion when its distribution is not (≈) uniform."""
    uniform = 1.0 / len(witness.distribution)
    return any(abs(p - uniform) > _OPINION_EPS for p in witness.distribution)


def discovery_distribution(match_rate: float | None) -> dict[str, float]:
    """The data witness's read on one formula — the measured row match rate.

    The discovery (or the loader grading a hypothesis with the same statistic)
    read every row; the match rate IS its belief that the formula holds. An
    ungraded formula (``None``) abstains — no rows were read, no opinion.
    """
    if match_rate is None:
        return _distribution(0.5)
    return _distribution(match_rate)


def llm_hypothesis_distribution(confidence: float | None) -> dict[str, float]:
    """The LLM's name-based lean that ITS hypothesized formula holds.

    ``0.5 + 0.5·confidence`` — at confidence→0 it abstains, at confidence→1 it
    asserts ``holds``. ``None`` (no confidence recorded) abstains rather than
    inventing a default strength. Used only on the hypothesis's own canonical
    slot; on every other formula the witness abstains (absence of a hypothesis
    is not evidence against — the type_claim lesson).
    """
    if confidence is None:
        return _distribution(0.5)
    conf = min(1.0, max(0.0, float(confidence)))
    return _distribution(0.5 + 0.5 * conf)


@dataclass
class _Slot:
    """One canonical formula claim being adjudicated (internal accumulator)."""

    identity: str
    display: str
    discovered_match: float | None = None
    discovered: bool = False
    hypothesized: bool = False
    hypothesis_confidence: float | None = None
    graded_match: float | None = None


def _normalized_raw_identity(formula: str) -> str:
    return "raw:" + " ".join(formula.strip().lower().split())


def measure_derived_value(
    table: str,
    column: str,
    discovered: Sequence[Mapping[str, Any]],
    hypothesis: Mapping[str, Any] | None = None,
    *,
    reliabilities: Mapping[str, float] | None = None,
) -> list[FormulaAdjudication]:
    """Adjudicate every formula claim on one column into ``(C, U)`` + posterior.

    Args:
        table, column: identity for the claim slots.
        discovered: the column's ``DerivedColumn`` rows (``load_correlation``
            shape entries: ``formula``, ``match_rate``, ``derivation_type``,
            optionally ``source_column_names``).
        hypothesis: the LLM's formula hypothesis for this column —
            ``{"formula": str | None, "confidence": float | None,
            "match_rate": float | None}`` where ``match_rate`` is the
            loader-computed row grading of a NOVEL hypothesis (``None`` when it
            matched a discovered formula or could not be graded). ``None`` /
            absent formula → the LLM witness abstains everywhere.
        reliabilities: per-witness reliability overrides; defaults to
            :data:`DEFAULT_RELIABILITIES`.

    Returns:
        One :class:`FormulaAdjudication` per canonical formula in play —
        discovered formulas first (input order), then a novel hypothesis. High
        ``result.conflict`` means the name-expected formula and the data
        disagree (the grounded-divergence case); high ``ignorance`` means no
        qualified witness weighed in. Empty when nothing was discovered and
        nothing was hypothesized.
    """
    rel = reliabilities or DEFAULT_RELIABILITIES
    slots: dict[str, _Slot] = {}

    for entry in discovered:
        raw = str(entry.get("formula") or "")
        match_rate = entry.get("match_rate")
        canonical = canonicalize_discovered(entry)
        identity = canonical.identity if canonical else _normalized_raw_identity(raw)
        slot = slots.setdefault(identity, _Slot(identity=identity, display=raw or identity))
        slot.discovered = True
        rate = None if match_rate is None else float(match_rate)
        if rate is not None and (slot.discovered_match is None or rate > slot.discovered_match):
            slot.discovered_match = rate

    hyp_canonical = None
    if hypothesis is not None:
        hyp_canonical = parse_formula(hypothesis.get("formula"))
        # A self-referential hypothesis (the column among its own operands) is
        # degenerate, not a derivation claim — treat as no hypothesis.
        if hyp_canonical is not None and column.strip().lower() in hyp_canonical.operands:
            hyp_canonical = None
    if hyp_canonical is not None and hypothesis is not None:
        slot = slots.setdefault(
            hyp_canonical.identity,
            _Slot(
                identity=hyp_canonical.identity,
                display=str(hypothesis.get("formula") or hyp_canonical.identity),
            ),
        )
        slot.hypothesized = True
        confidence = hypothesis.get("confidence")
        slot.hypothesis_confidence = None if confidence is None else float(confidence)
        graded = hypothesis.get("match_rate")
        if not slot.discovered and graded is not None:
            slot.graded_match = float(graded)

    adjudications: list[FormulaAdjudication] = []
    for slot in slots.values():
        data_rate = slot.discovered_match if slot.discovered else slot.graded_match
        candidates = (
            _witness(
                "formula_discovery",
                discovery_distribution(data_rate),
                rel.get("formula_discovery", DEFAULT_RELIABILITIES["formula_discovery"]),
            ),
            _witness(
                "llm_hypothesis",
                llm_hypothesis_distribution(slot.hypothesis_confidence)
                if slot.hypothesized
                else _distribution(0.5),
                rel.get("llm_hypothesis", DEFAULT_RELIABILITIES["llm_hypothesis"]),
            ),
        )
        # Only witnesses that take a position are pooled — abstention is
        # ignorance, not a conflicting party (same convention as the siblings).
        witnesses = tuple(w for w in candidates if _has_opinion(w))
        adjudications.append(
            FormulaAdjudication(
                table=table,
                column=column,
                formula=slot.identity,
                formula_display=slot.display,
                claim_field=f"derived_formula:{table}.{column}:{slot.identity}",
                discovered=slot.discovered,
                hypothesized=slot.hypothesized,
                match_rate=data_rate,
                witnesses=witnesses,
                result=pool(witnesses),
            )
        )
    return adjudications
