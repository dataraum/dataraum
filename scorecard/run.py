#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyyaml"]
# ///
"""Scorecard runner — ADR-0019.

Computes the verdict on an epic branch: area regression checks (from
scorecard/scorecard.yaml) plus the epic's KPI measures (from the fenced
```yaml scorecard``` block in epics/<slug>.md), compared against baselines
and targets. Emits scorecard.json + scorecard.md; the PR body is generated
from these — the agent never authors the numbers.

The gate contract (--gate, used by CI after restoring scorecard/ and epics/
from origin/main): exit non-zero on any failed check, any gating KPI that is
below target, regressed, errored, or UNMEASURED, or a touched judge path.
The one sanctioned judge edit is the completing PR deleting exactly its own
epics/<slug>.md (ADR-0019 §1) — the contract is then read from origin/main.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import yaml

# Paths the branch under judgment must not modify. CI restores scorecard/ and
# epics/ from main; .github/ is listed so a workflow edit is at least visible
# and gate-failing here (for pull_request events GitHub runs the PR's workflow
# file — branch protection on the required check is the outer defense).
JUDGE_PATHS = ("scorecard/", "epics/", ".github/")
# Existing-test modifications carry no verdict weight but MUST be visible.
TEST_PATH_PATTERNS = (
    re.compile(r"^packages/engine/tests/"),
    re.compile(r"^packages/cockpit/.*\.test\.[jt]sx?$"),
)
TARGET_RE = re.compile(r"^(>=|<=|==|>|<)\s*(-?\d+(?:\.\d+)?)$")


def sh(cmd: str, cwd: Path) -> tuple[int, str, str]:
    proc = subprocess.run(
        cmd, shell=True, cwd=cwd, capture_output=True, text=True
    )
    return proc.returncode, proc.stdout, proc.stderr


def repo_root() -> Path:
    code, out, _ = sh("git rev-parse --show-toplevel", Path.cwd())
    if code != 0:
        sys.exit("scorecard: not inside a git repository")
    return Path(out.strip())


@dataclass
class CheckResult:
    id: str
    area: str
    status: str  # pass | fail | skip
    duration_s: float = 0.0
    tail: str = ""


@dataclass
class KpiResult:
    id: str
    statement: str
    tier: str
    target: str
    baseline: float | None
    value: float | None = None
    status: str = "pending"  # pass | fail | regressed | pending | error
    detail: str = ""


@dataclass
class Report:
    branch: str
    sha: str
    profile: str
    epic: str | None
    checks: list[CheckResult] = field(default_factory=list)
    kpis: list[KpiResult] = field(default_factory=list)
    tests_modified: list[str] = field(default_factory=list)
    tests_deleted: list[str] = field(default_factory=list)
    tests_added: list[str] = field(default_factory=list)
    judge_touched: list[str] = field(default_factory=list)


def load_registry(root: Path) -> dict:
    path = root / "scorecard" / "scorecard.yaml"
    if not path.exists():
        sys.exit(f"scorecard: {path} not found")
    try:
        registry = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        sys.exit(f"scorecard: malformed {path}: {exc}")
    if not isinstance(registry, dict) or not isinstance(registry.get("areas"), dict):
        sys.exit(f"scorecard: {path} must define an 'areas' mapping")
    return registry


def area_checks(registry: dict, area: str) -> dict:
    spec = registry["areas"].get(area)
    if spec is None:
        sys.exit(f"scorecard: unknown area '{area}' (see scorecard/scorecard.yaml)")
    return spec


def load_epic_contract(root: Path, slug: str) -> dict:
    path = root / "epics" / f"{slug}.md"
    if path.exists():
        text = path.read_text()
    else:
        # The completing PR deletes its own epic file (ADR-0019 §1); the
        # contract that judges it is main's version — same one CI restores.
        code, out, _ = sh(f"git show origin/main:epics/{slug}.md", root)
        if code != 0:
            sys.exit(f"scorecard: epics/{slug}.md not in working tree nor on origin/main")
        text = out
    match = re.search(r"```yaml scorecard\n(.*?)```", text, re.DOTALL)
    if not match:
        sys.exit(f"scorecard: no ```yaml scorecard``` block in epics/{slug}.md")
    try:
        contract = yaml.safe_load(match.group(1))
    except yaml.YAMLError as exc:
        sys.exit(f"scorecard: malformed scorecard block in epics/{slug}.md: {exc}")
    if not isinstance(contract, dict):
        sys.exit(f"scorecard: scorecard block in epics/{slug}.md is not a mapping")
    if contract.get("slug") != slug:
        sys.exit(f"scorecard: slug mismatch — file {slug}, contract {contract.get('slug')}")
    return contract


def run_checks(root: Path, registry: dict, areas: list[str], profile: str) -> list[CheckResult]:
    results: list[CheckResult] = []
    profiles = ["fast"] if profile == "fast" else ["fast", "full"]
    for area in areas:
        spec = area_checks(registry, area)
        for prof in profiles:
            for check in spec.get(prof) or []:
                requires = check.get("requires")
                if requires and not (root / requires).exists():
                    results.append(CheckResult(check["id"], area, "skip", tail=f"missing {requires}"))
                    continue
                start = time.monotonic()
                code, stdout, stderr = sh(check["run"], root)
                results.append(
                    CheckResult(
                        check["id"],
                        area,
                        "pass" if code == 0 else "fail",
                        round(time.monotonic() - start, 1),
                        "" if code == 0 else "\n".join((stdout + stderr).splitlines()[-15:]),
                    )
                )
    return results


def parse_measure_output(stdout: str) -> float:
    # stdout ONLY — tool noise (uv installs, warnings) goes to stderr and must
    # never poison the measurement. This is the epic-template contract.
    last = next(line for line in reversed(stdout.splitlines()) if line.strip())
    try:
        parsed = json.loads(last)
        if isinstance(parsed, dict):
            return float(parsed["value"])
        return float(parsed)
    except (json.JSONDecodeError, KeyError, TypeError):
        return float(last.strip())


def evaluate_kpi(kpi: dict, root: Path) -> KpiResult:
    result = KpiResult(
        id=kpi["id"],
        statement=kpi.get("statement", ""),
        tier=kpi.get("tier", "promotion"),
        target=str(kpi["target"]).strip(),
        baseline=None,
    )
    raw_baseline = kpi.get("baseline")
    if raw_baseline is not None:
        try:
            result.baseline = float(raw_baseline)
        except (TypeError, ValueError):
            result.status, result.detail = "error", f"unparseable baseline {raw_baseline!r}"
            return result
    match = TARGET_RE.match(result.target)
    if not match:
        result.status, result.detail = "error", f"unparseable target {result.target!r}"
        return result
    op, threshold = match.group(1), float(match.group(2))
    measure = kpi.get("measure")
    if not measure:
        result.status, result.detail = "pending", "no measure command"
        return result
    code, stdout, stderr = sh(measure, root)
    if code != 0:
        result.status = "error"
        result.detail = "\n".join((stdout + stderr).splitlines()[-10:])
        return result
    try:
        result.value = parse_measure_output(stdout)
    except (StopIteration, ValueError) as exc:
        result.status, result.detail = "error", f"unparseable measure stdout: {exc}"
        return result

    compare = {
        ">=": lambda v: v >= threshold,
        "<=": lambda v: v <= threshold,
        ">": lambda v: v > threshold,
        "<": lambda v: v < threshold,
        "==": lambda v: v == threshold,
    }[op]
    result.status = "pass" if compare(result.value) else "fail"
    # Regression vs baseline: worse in the direction the target points.
    if result.status == "pass" and result.baseline is not None:
        worse = result.value < result.baseline if op in (">=", ">") else result.value > result.baseline
        if op != "==" and worse:
            result.status = "regressed"
    return result


def collect_diff(root: Path, report: Report, epic_slug: str) -> None:
    code, out, _ = sh("git diff --name-status origin/main...HEAD", root)
    if code != 0:
        return
    own_epic_file = f"epics/{epic_slug}.md" if epic_slug else None
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        status, old, new = parts[0][:1], parts[1], parts[-1]

        for path in dict.fromkeys((old, new)):  # ordered de-dupe
            if not any(path.startswith(j) for j in JUDGE_PATHS):
                continue
            # The one sanctioned judge edit: the completing PR deletes exactly
            # its own epic file (ADR-0019 §1). Everything else is flagged.
            if status == "D" and path == own_epic_file:
                continue
            if path not in report.judge_touched:
                report.judge_touched.append(path)

        old_is_test = any(p.match(old) for p in TEST_PATH_PATTERNS)
        new_is_test = any(p.match(new) for p in TEST_PATH_PATTERNS)
        if status == "R":
            # A test renamed out of the test tree is a deletion in disguise.
            if old_is_test and not new_is_test:
                report.tests_deleted.append(old)
            elif old_is_test or new_is_test:
                report.tests_modified.append(f"{old} → {new}")
        elif status == "C":
            if new_is_test:
                report.tests_added.append(new)
        elif new_is_test:
            if status == "A":
                report.tests_added.append(new)
            elif status == "D":
                report.tests_deleted.append(new)
            else:
                report.tests_modified.append(new)


ICONS = {"pass": "✅", "fail": "❌", "skip": "⏭️", "regressed": "🔻", "pending": "⏳", "error": "💥"}


def to_markdown(report: Report) -> str:
    lines = [
        f"# Scorecard — `{report.branch}` @ `{report.sha[:10]}`",
        "",
        f"Profile: **{report.profile}**"
        + (f" · Epic: **{report.epic}**" if report.epic else "")
        + " · Verdicts computed by `scorecard/run.py` (ADR-0019); agent prose carries no numbers.",
        "",
    ]
    if report.kpis:
        lines += ["## Objective KPIs", "", "| KPI | baseline | value | target | status |", "|---|---|---|---|---|"]
        for k in report.kpis:
            value = "—" if k.value is None else f"{k.value:g}"
            baseline = "—" if k.baseline is None else f"{k.baseline:g}"
            lines.append(f"| {k.id} ({k.tier}) | {baseline} | {value} | `{k.target}` | {ICONS[k.status]} {k.status} |")
        for k in report.kpis:
            if k.detail:
                lines += ["", f"<details><summary>{k.id}: {k.status}</summary>", "", "```", k.detail, "```", "</details>"]
        lines.append("")
    lines += ["## Area regressions", "", "| check | area | status | s |", "|---|---|---|---|"]
    for c in report.checks:
        lines.append(f"| {c.id} | {c.area} | {ICONS[c.status]} {c.status} | {c.duration_s:g} |")
    for c in report.checks:
        if c.tail:
            lines += ["", f"<details><summary>{c.id}: {c.status}</summary>", "", "```", c.tail, "```", "</details>"]
    lines += ["", "## Test diff vs origin/main", ""]
    if report.tests_deleted or report.tests_modified:
        lines.append("**Existing tests touched — review these first (zero evidentiary weight, full visibility):**")
        lines += [f"- 🗑️ deleted: `{p}`" for p in report.tests_deleted]
        lines += [f"- ✏️ modified: `{p}`" for p in report.tests_modified]
    else:
        lines.append("No existing tests modified or deleted.")
    if report.tests_added:
        lines += ["", f"{len(report.tests_added)} test file(s) added (scaffolding, not evidence)."]
    if report.judge_touched:
        lines += ["", "## ⚠️ Judge paths touched on this branch", ""]
        lines += [f"- `{p}`" for p in report.judge_touched]
        lines.append("\nCI recomputes from `origin/main`; these edits do not affect the verdict.")
    return "\n".join(lines) + "\n"


def gate_failures(report: Report) -> list[str]:
    failures = [f"check failed: {c.id}" for c in report.checks if c.status == "fail"]
    for k in report.kpis:
        gating = k.tier == "checkpoint" or report.profile == "full"
        if not gating:
            continue
        if k.status == "pending":
            # An unmeasured gating KPI must never read as "not failing".
            failures.append(f"KPI unmeasured: {k.id}")
        elif k.status in ("fail", "regressed", "error"):
            failures.append(f"KPI {k.status}: {k.id}")
    failures += [f"judge path touched: {p}" for p in report.judge_touched]
    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=["fast", "full"], default="fast")
    parser.add_argument("--epic", help="epic slug (epics/<slug>.md)")
    parser.add_argument("--areas", help="comma-separated override of the epic's areas")
    parser.add_argument("--gate", action="store_true", help="exit non-zero on any gate failure")
    parser.add_argument("--list", action="store_true", help="print the resolved plan, run nothing")
    parser.add_argument("--pr-body", action="store_true", help="print the markdown report to stdout")
    parser.add_argument("--out", default="scorecard/out")
    args = parser.parse_args()

    root = repo_root()
    registry = load_registry(root)
    contract = load_epic_contract(root, args.epic) if args.epic else {}
    areas = (
        [a.strip() for a in args.areas.split(",")]
        if args.areas
        else contract.get("areas", list(registry["areas"]))
    )

    if args.list:
        print(f"profile={args.profile} epic={args.epic or '—'} areas={','.join(areas)}")
        profiles = ["fast"] if args.profile == "fast" else ["fast", "full"]
        for area in areas:
            spec = area_checks(registry, area)
            for prof in profiles:
                for check in spec.get(prof) or []:
                    print(f"  [{area}/{prof}] {check['id']}: {check['run']}")
        for kpi in contract.get("kpis", []):
            print(f"  [kpi/{kpi.get('tier', 'promotion')}] {kpi['id']}: target {kpi['target']}")
        return

    _, branch, _ = sh("git rev-parse --abbrev-ref HEAD", root)
    _, sha, _ = sh("git rev-parse HEAD", root)
    report = Report(branch.strip(), sha.strip(), args.profile, args.epic)
    collect_diff(root, report, args.epic or "")
    report.checks = run_checks(root, registry, areas, args.profile)
    for kpi in contract.get("kpis", []):
        if kpi.get("tier", "promotion") == "promotion" and args.profile == "fast":
            continue  # promotion KPIs are measured at promotion / in CI, not every checkpoint
        report.kpis.append(evaluate_kpi(kpi, root))

    out_dir = root / args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "scorecard.json").write_text(
        json.dumps(report, default=lambda o: o.__dict__, indent=2) + "\n"
    )
    markdown = to_markdown(report)
    (out_dir / "scorecard.md").write_text(markdown)
    if args.pr_body:
        print(markdown)
    else:
        for c in report.checks:
            print(f"{ICONS[c.status]} {c.area}/{c.id}")
        for k in report.kpis:
            print(f"{ICONS[k.status]} kpi/{k.id} = {k.value} (target {k.target})")
        print(f"→ {out_dir / 'scorecard.md'}")

    failures = gate_failures(report)
    if failures:
        print("\ngate: " + "; ".join(failures), file=sys.stderr)
        if args.gate:
            sys.exit(1)


if __name__ == "__main__":
    main()
