"""Deterministic class routing for the dimension-identity veto lane (DAT-762).

Stats DECIDE, and this router selects which of their ASSERTED structures are
shown to the names-only veto judge — nothing else ever reaches it. The routing
predicates operate on VALUE evidence only (counts, dtypes, value shapes);
names are deliberately excluded — they are the judge's evidence, so the router
cannot encode name heuristics, and the classes the statistics own outright
(dirty-true hierarchies, weak-true organizational edges — the DAT-757
scorecard's 27/27, which both LLM channels systematically destroy) are
unreachable by construction. Validated against the frozen 45-cell scorecard
fixture (calibration/fixtures/dimension_identity_cells.json in dataraum-eval):
zero leakage into the protected classes, full recall on the veto classes.

The three veto-eligible classes (measured names-judgeable, C2 > C1 and
C2 > C3 — statistics shown to the judge make it rationalize artifacts):

- quasi-identifier: a determinant that identifies rather than groups —
  a temporal determinant (a date is an attribute, never a dimension level
  over other attributes; calendar rollups are the temporal lane, DAT-730),
  an id-shaped determinant over a tiny enum (near-unique codes "determining"
  a 3-value status is row identity), or a digitless name-shaped determinant
  at entity-scale cardinality (surnames cluster by nationality; they do not
  group into it).
- free-text-determinant: prose determines by accident of cardinality, never
  as a groupable level.
- proxy-bijection: an alias (1:1) pair whose sides are different kinds of
  thing — an id is not a date, an id is not a description. Statistically a
  MERGE, semantically an attribute edge (the DAT-761 id<->text residue).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Determinants at or above this distinct share of rows are near-keys — the
# stack's own guard (processor.NEAR_KEY_FRAC) excludes them before assembly,
# and the router refuses them too (defense in depth): a near-key edge is
# spurious by construction, not a judgment call.
NEAR_KEY_FRAC = 0.9
# A tiny enum: a dependent this small is trivially "determined" by anything
# near-unique — grouping onto it from an id-shaped column is row identity.
TINY_ENUM_MAX = 10
# Entity-scale cardinality for name-shaped determinants: below this, plain-word
# columns are legitimate coarse levels (department names); above it they
# enumerate entities (surnames), not levels.
ENTITY_SCALE_MIN = 200

# Shape classification thresholds — shares over the distinct-value sample.
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
_ID_RE = re.compile(r"^[A-Za-z0-9_\-/\.]+$")

QUASI_IDENTIFIER = "quasi-identifier"
FREE_TEXT_DETERMINANT = "free-text-determinant"
PROXY_BIJECTION = "proxy-bijection"


@dataclass(frozen=True)
class ColumnEvidence:
    """Value evidence for one column — everything routing may consult."""

    n_rows: int
    n_distinct: int
    dtype: str
    sample_values: list[str]  # a seeded sample of DISTINCT values, stringified


def classify_shape(ev: ColumnEvidence) -> str:
    """One of temporal | prose | idlike | name | label | code | empty.

    Computed over the distinct-value sample: temporal by dtype or ISO-date
    share; prose needs WHITESPACE and length (a 64-char hash is idlike, not
    prose — the rel-hm postal-code lesson); idlike is separator-free tokens
    carrying digits; name is digitless alpha tokens; label is whitespaced
    short text; code is the residual.
    """
    vals = ev.sample_values
    if not vals:
        return "empty"
    n = len(vals)
    mean_len = sum(len(v) for v in vals) / n
    ws = sum(1 for v in vals if " " in v.strip()) / n
    digit = sum(1 for v in vals if any(c.isdigit() for c in v)) / n
    alpha = sum(1 for v in vals if v.replace("-", "").replace("'", "").isalpha()) / n
    tsish = sum(1 for v in vals if _TS_RE.match(v)) / n
    idish = sum(1 for v in vals if _ID_RE.match(v)) / n
    dtype = ev.dtype.lower()
    if tsish > 0.8 or "date" in dtype or "time" in dtype:
        return "temporal"
    if ws > 0.5 and mean_len > 20:
        return "prose"
    if idish > 0.8 and digit > 0.5:
        return "idlike"
    if alpha > 0.8 and ws < 0.3:
        return "name"
    if ws > 0.5:
        return "label"
    return "code"


def is_entity_label_pair(key: ColumnEvidence, partner: ColumnEvidence) -> bool:
    """Value-evidence half of the entity-key anchor (DAT-762 clean-flat lesson).

    A column whose stats-asserted 1:1 alias partner is a human label
    (name/label shape) at real cardinality is an ENTITY KEY — a dimension's own
    key with its display name (account_id ⇄ account_name), not a transaction
    identifier. The 1:1-ness itself is the alias row's claim (the producer
    checks membership); this predicate checks only shapes and scale. The
    tiny-enum floor on BOTH sides fences degenerate domains — constants are
    trivially "bijective" with anything (the probe's currency/status noise).
    A prose partner never anchors: description ⇄ entry_id is bijective too,
    and entry_id must stay routable (the probe's attack case).
    """
    return (
        classify_shape(partner) in ("name", "label")
        and key.n_distinct > TINY_ENUM_MAX
        and partner.n_distinct > TINY_ENUM_MAX
    )


def route_alias(a: ColumnEvidence, b: ColumnEvidence) -> str | None:
    """Veto class for an asserted ALIAS (1:1 merge) pair, or None (not judged).

    A true code<->label alias pairs same-kind or code/label sides; a proxy
    bijection pairs an identity-ish side with a temporal or prose side — two
    different kinds of thing that merely co-vary.
    """
    sa, sb = classify_shape(a), classify_shape(b)
    if sa == sb:
        return None
    if "temporal" in (sa, sb) or "prose" in (sa, sb):
        return PROXY_BIJECTION
    return None


def route_edge(
    det: ColumnEvidence, dep: ColumnEvidence, *, entity_anchored: bool = False
) -> str | None:
    """Veto class for an asserted DRILLDOWN edge det -> dep, or None.

    None means the statistical verdict stands unjudged — the default. The
    protected classes (dirty-true hierarchies, weak-true org edges: id/code
    determinants with genuine fan-in onto level-scale dependents) satisfy no
    predicate here by construction.

    ``entity_anchored``: the structure carrying this edge contains an entity
    key (a member with a 1:1 name/label alias partner —
    :func:`is_entity_label_pair`; the producer computes membership). Such a
    structure is the entity's own internal hierarchy (account_type →
    parent_account_id → account_id), so its ID-SHAPED determinants skip the
    tiny-enum quasi route — the clean-flat false-veto class the probe
    separated 2/2 vs 9/9. Prose/temporal determinants route regardless: a
    free-text edge inside an anchored chain is still junk.
    """
    if det.n_rows and det.n_distinct >= NEAR_KEY_FRAC * det.n_rows:
        return None  # near-key determinant: the stack's guard territory, not the judge's
    s_det = classify_shape(det)
    if s_det == "prose":
        return FREE_TEXT_DETERMINANT
    if s_det == "temporal":
        return QUASI_IDENTIFIER
    if s_det == "idlike" and dep.n_distinct <= TINY_ENUM_MAX and not entity_anchored:
        return QUASI_IDENTIFIER
    if s_det == "name" and det.n_distinct > ENTITY_SCALE_MIN:
        return QUASI_IDENTIFIER
    return None
