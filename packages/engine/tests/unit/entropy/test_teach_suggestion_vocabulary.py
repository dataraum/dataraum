"""Teach-suggestion vocabulary guard (ADR-0009 pack piece 6).

Every ``teach_suggestion`` any measurement emits must name a teach type the
system can APPLY — either a registered overlay applier
(:func:`dataraum.core.overlay.appliable_teach_types`) or a documented direct
``config_overlay`` read. A suggestion outside that vocabulary is a product
surface telling the user to do something the engine cannot execute (the
DAT-445 ``rebind`` gap: the temporal_behavior detector suggested ``rebind``
before any rebind applier existed).

Generic over all emissions: the test AST-scans every module under
``src/dataraum`` for the ``teach_suggestion`` key (dict-literal entries and
subscript assignments) and statically resolves the suggested ``type``. An
emission whose type CANNOT be statically resolved fails too — the convention
this guard enforces is that suggestions are built as dict literals with a
constant ``"type"`` (directly, or via a local variable), so the vocabulary
seam stays checkable in milliseconds.
"""

from __future__ import annotations

import ast
from pathlib import Path

import dataraum
from dataraum.core.overlay import appliable_teach_types

SRC_ROOT = Path(dataraum.__file__).parent

# Teach types consumed by DIRECT ``config_overlay`` table reads rather than a
# layered-read applier in ``core/overlay.py``. Each entry must cite its
# consumer — a type with no consumer does not belong here.
DIRECT_READ_TEACH_TYPES: frozenset[str] = frozenset(
    {
        # analysis/relationships/utils.load_confirmed_relationship_pairs,
        # read by the join_path_determinism detector (DAT-409).
        "relationship",
        # entropy/detectors/loaders.load_documented_dependencies,
        # read by the dimensional_entropy detector.
        "expected_dependency",
    }
)


def _dict_teach_type(node: ast.Dict) -> str | None:
    """The constant ``"type"`` entry of a teach dict literal, or ``None``."""
    for key, value in zip(node.keys, node.values, strict=True):
        if isinstance(key, ast.Constant) and key.value == "type":
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                return value.value
            return None
    return None


def _suggestion_value_nodes(tree: ast.AST) -> list[ast.expr]:
    """Every expression used as the value of a ``teach_suggestion`` key.

    Covers the two emission shapes: a ``"teach_suggestion": <expr>`` entry in
    a dict literal, and a ``something["teach_suggestion"] = <expr>`` subscript
    assignment.
    """
    values: list[ast.expr] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values, strict=True):
                if isinstance(key, ast.Constant) and key.value == "teach_suggestion":
                    values.append(value)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if (
                    isinstance(target, ast.Subscript)
                    and isinstance(target.slice, ast.Constant)
                    and target.slice.value == "teach_suggestion"
                ):
                    values.append(node.value)
    return values


def _resolve_teach_types(value: ast.expr, tree: ast.AST) -> list[str | None]:
    """Statically resolve the teach type(s) an emission value may carry.

    A dict literal resolves to its ``"type"`` entry. A name resolves to the
    ``"type"`` of every dict literal assigned to that name in the module (the
    branching-assignment pattern the temporal_behavior detector uses).
    ``None`` entries mean "could not resolve" and fail the guard.
    """
    if isinstance(value, ast.Dict):
        return [_dict_teach_type(value)]
    if isinstance(value, ast.Name):
        resolved: list[str | None] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, ast.AnnAssign):
                targets = [node.target]
            else:
                continue
            for target in targets:
                if (
                    isinstance(target, ast.Name)
                    and target.id == value.id
                    and isinstance(node.value, ast.Dict)
                ):
                    resolved.append(_dict_teach_type(node.value))
        return resolved or [None]
    return [None]


def _collect_emissions() -> dict[str, list[str | None]]:
    """Module → statically resolved teach types of its suggestion emissions."""
    emissions: dict[str, list[str | None]] = {}
    for path in sorted(SRC_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        value_nodes = _suggestion_value_nodes(tree)
        if not value_nodes:
            continue
        types: list[str | None] = []
        for value in value_nodes:
            types.extend(_resolve_teach_types(value, tree))
        emissions[str(path.relative_to(SRC_ROOT))] = types
    return emissions


def test_every_emitted_teach_suggestion_names_an_appliable_type() -> None:
    """The seam: suggestion type ∈ (overlay appliers ∪ direct reads), always."""
    vocabulary = appliable_teach_types() | DIRECT_READ_TEACH_TYPES
    emissions = _collect_emissions()

    # Scanner self-check: the temporal_behavior detector emission is the known
    # baseline — if the scanner stops seeing ANY emission, the guard is dead,
    # not green.
    assert emissions, "no teach_suggestion emission found — scanner or emission convention broke"

    problems: list[str] = []
    for module, types in emissions.items():
        for teach_type in types:
            if teach_type is None:
                problems.append(
                    f"{module}: a teach_suggestion's type is not statically resolvable — "
                    "build suggestions as dict literals with a constant 'type' "
                    "(directly or via a local variable) so this guard can check them"
                )
            elif teach_type not in vocabulary:
                problems.append(
                    f"{module}: suggests teach type '{teach_type}' which has no overlay "
                    "applier (core/overlay.py) and no documented direct config_overlay "
                    "read — the product surface cannot execute it (ADR-0009 piece 6)"
                )
    assert not problems, "\n".join(problems)


def test_known_emissions_are_seen_by_the_scanner() -> None:
    """Pin the derived_values emission so scanner regressions surface loudly.

    Canary was temporal_behavior until DAT-657 dropped its teach: stock/flow is
    data-determined (the structural witness wins), a wrong grounding is corrected on
    the grounding path — so the temporal detector emits NO teach_suggestion. The
    derived_values ``validation`` teach is now the stable single-type baseline.
    """
    emissions = _collect_emissions()
    assert "entropy/detectors/computational/temporal_behavior.py" not in emissions, (
        "temporal_behavior must emit NO teach_suggestion (DAT-657 — stock/flow is data-determined)"
    )
    dv = emissions.get("entropy/detectors/computational/derived_values.py")
    assert dv is not None, "derived_values teach_suggestion emission no longer detected"
    assert set(dv) == {"validation"}
