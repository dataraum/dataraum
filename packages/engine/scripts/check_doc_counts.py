"""Assert that numbered claims in user-facing docs match source-of-truth counts.

Source of truth:
  - Phases: subclasses of BasePhase under src/dataraum/pipeline/phases/
  - Detectors: distinct `detector_id = "..."` literals under src/dataraum/entropy/detectors/
                (excluding the abstract "base" default)
  - MCP tools: `name="..."` registrations in src/dataraum/mcp/server.py

We scan a fixed set of doc files for numbered claims like
`18 phases`, `17-phase pipeline`, `10 MCP tools`, `16 detectors`, etc., and
fail if the number disagrees with the truth.

Run locally:  uv run python scripts/check_doc_counts.py
Used in CI by .github/workflows/release.yml (preflight job).
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DOC_FILES = [
    "README.md",
    "docs/index.md",
    "docs/architecture.md",
    "docs/pipeline.md",
    "docs/entropy.md",
    "docs/contributing.md",
    "docs/configuration.md",
    "docs/data-model.md",
]

# Each kind matches noun forms used in prose. Order matters for regex
# alternation (longest first wins inside a group).
KINDS: dict[str, tuple[str, ...]] = {
    "phase": ("phases", "phase"),
    "tool": ("MCP tools", "mcp tools", "tools", "tool"),
    "detector": ("entropy detectors", "detectors", "detector"),
}


@dataclass(frozen=True)
class Truth:
    phase: int
    tool: int
    detector: int


def count_phases() -> int:
    pattern = re.compile(r"class\s+\w+Phase\s*\(\s*BasePhase\s*\)")
    seen: set[str] = set()
    for path in (ROOT / "src/dataraum/pipeline/phases").glob("*.py"):
        for m in pattern.finditer(path.read_text(encoding="utf-8")):
            seen.add(m.group(0))
    return len(seen)


def count_detectors() -> int:
    pattern = re.compile(r'^\s*detector_id\s*=\s*"([a-z_]+)"', re.MULTILINE)
    ids: set[str] = set()
    for path in (ROOT / "src/dataraum/entropy/detectors").rglob("*.py"):
        for m in pattern.finditer(path.read_text(encoding="utf-8")):
            ids.add(m.group(1))
    ids.discard("base")
    return len(ids)


def tool_names() -> set[str]:
    server = (ROOT / "src/dataraum/mcp/server.py").read_text(encoding="utf-8")
    return set(re.findall(r'\bname\s*=\s*"([a-z_]+)"', server))


def count_tools() -> int:
    return len(tool_names())


def truth() -> Truth:
    return Truth(phase=count_phases(), tool=count_tools(), detector=count_detectors())


def _kind_alternation() -> str:
    parts: list[str] = []
    for forms in KINDS.values():
        parts.extend(re.escape(f) for f in forms)
    return "|".join(parts)


def _classify(noun: str) -> str:
    n = noun.lower()
    for kind, forms in KINDS.items():
        if any(n == f.lower() for f in forms):
            return kind
    raise AssertionError(f"unclassified noun: {noun!r}")


@dataclass(frozen=True)
class Claim:
    file: str
    line: int
    text: str
    number: int
    kind: str


def find_claims(path: Path) -> list[Claim]:
    """Find numbered claims about phases/tools/detectors in `path`."""
    alt = _kind_alternation()
    # Match either "<n> <noun>" or "<n>-<singular-noun>"  (e.g., "17-phase").
    pattern = re.compile(
        rf"\b(?P<num>\d{{1,3}})(?:-|\s+)(?P<noun>{alt})\b",
        re.IGNORECASE,
    )
    claims: list[Claim] = []
    text = path.read_text(encoding="utf-8")
    for lineno, line in enumerate(text.splitlines(), start=1):
        for m in pattern.finditer(line):
            num = int(m.group("num"))
            kind = _classify(m.group("noun"))
            claims.append(
                Claim(
                    file=str(path.relative_to(ROOT)),
                    line=lineno,
                    text=line.strip(),
                    number=num,
                    kind=kind,
                )
            )
    return claims


def check_readme_tool_table() -> list[str]:
    """Assert every registered MCP tool appears as a row in the README tool table."""
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    registered = tool_names()
    documented = set(re.findall(r"^\|\s*`([a-z_]+)`\s*\|", readme, re.MULTILINE))
    missing = registered - documented
    extra = documented - registered
    errors: list[str] = []
    if missing:
        errors.append("README.md tool table is missing rows for: " + ", ".join(sorted(missing)))
    if extra:
        errors.append(
            "README.md tool table lists tools not registered in the server: "
            + ", ".join(sorted(extra))
        )
    return errors


def check() -> int:
    t = truth()
    expected = {"phase": t.phase, "tool": t.tool, "detector": t.detector}

    print(f"source of truth: {t.phase} phases, {t.tool} MCP tools, {t.detector} detectors")

    mismatches: list[tuple[Claim, int]] = []
    scanned = 0
    for rel in DOC_FILES:
        path = ROOT / rel
        if not path.exists():
            continue
        scanned += 1
        for claim in find_claims(path):
            want = expected[claim.kind]
            if claim.number != want:
                mismatches.append((claim, want))

    print(f"scanned {scanned} doc files")

    table_errors = check_readme_tool_table()

    if not mismatches and not table_errors:
        print("OK — all numbered claims match and README tool table is complete.")
        return 0

    if mismatches:
        print()
        print(f"{len(mismatches)} stale claim(s):")
        for claim, want in mismatches:
            print(
                f"  {claim.file}:{claim.line}  says {claim.number} {claim.kind}(s), truth is {want}"
            )
            print(f"    >> {claim.text}")

    if table_errors:
        print()
        for err in table_errors:
            print(f"  {err}")

    return 1


if __name__ == "__main__":
    sys.exit(check())
