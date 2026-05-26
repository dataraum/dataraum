#!/bin/bash
# End-of-turn quality gate for Claude Code
# Exit code 2 = block and show error to Claude (forces it to fix)
# Exit code 0 = success, continue

set -e

# Resolve the project root the hook should scope to. Two-tier lookup:
#
# 1. ``$CLAUDE_PROJECT_DIR`` (preferred) — set by the harness and shifted
#    by EnterWorktree, so a worktree session scopes to its own tree, not
#    the orchestrator's main repo.
# 2. ``git rev-parse --show-toplevel`` (fallback) — returns the current
#    worktree's top, which is what we want whenever the harness env var
#    is empty (some hook contexts) or stale.
#
# If both fail (not in a git worktree at all), exit 2 — silent skip on
# an unset env var has historically let lint/format issues slip through
# to CI; fail loud instead.
if [ -n "$CLAUDE_PROJECT_DIR" ]; then
    project_dir="$CLAUDE_PROJECT_DIR"
elif project_dir=$(git rev-parse --show-toplevel 2>/dev/null); then
    :
else
    echo "❌ end-of-turn-check: cannot resolve project dir (CLAUDE_PROJECT_DIR unset, not in a git worktree)" >&2
    exit 2
fi

# Change-gate the whole quality suite per package. Every check below only
# inspects one package's sources, so if that package has no unpushed
# changes there's nothing any of its checks can newly flag — skip them.
# The engine's pytest is the original motivator (~85s, and --testmon still
# imports/collects the whole suite before it can decide nothing changed),
# but the same logic applies to mypy (~15s), biome and tsc.
#
# Baseline = what CI has already validated: the merge-base with the upstream
# tracking branch (falls back to origin/main, then main). We diff the
# working tree against that, so the guard sees ALL local-ahead work —
# uncommitted AND committed-but-unpushed. Comparing against HEAD alone would
# wrongly skip once a change is committed (e.g. stacking several commits
# locally before a push); the slate only clears when you push and the
# baseline advances. If no baseline resolves (detached/no-remote), fail safe
# by treating every package as changed.
base=$(git -C "$project_dir" rev-parse --verify --quiet '@{upstream}') \
    || base=$(git -C "$project_dir" rev-parse --verify --quiet origin/main) \
    || base=$(git -C "$project_dir" rev-parse --verify --quiet main) \
    || base=""
mb=""
if [ -n "$base" ]; then
    mb=$(git -C "$project_dir" merge-base "$base" HEAD 2>/dev/null) || mb=""
fi

# changed <pathspec>...  → exit 0 if the given paths differ from the
# baseline (or no baseline resolved), exit 1 if they are unchanged.
changed() {
    if [ -z "$mb" ]; then return 0; fi
    if git -C "$project_dir" diff --quiet "$mb" -- "$@"; then return 1; fi
    return 0
}

ran_any=false

# ── Engine (Python) ─────────────────────────────────────────────────────
if changed 'packages/engine/*.py'; then
    ran_any=true
    cd "$project_dir/packages/engine"
    echo "Running engine quality checks ($project_dir/packages/engine)..."

    echo "Checking: ruff..."
    if ! uv run ruff format --check . --quiet 2>/dev/null; then
        echo "❌ Linting failed. Fix lint errors before continuing." >&2
        exit 2
    fi

    echo "Checking: vulture (dead code)..."
    # Filter "unreachable code after 'while'" — known false positive for while-True/break patterns
    vulture_out=$(uv run python -m vulture src/dataraum vulture_whitelist.py --min-confidence 80 2>&1 \
        | grep -v "unreachable code after 'while'" || true)
    if [ -n "$vulture_out" ]; then
        echo "$vulture_out"
        echo "❌ Dead code detected. Remove unreachable/unused code before continuing." >&2
        exit 2
    fi

    echo "Checking: mypy..."
    if ! uv run python -m mypy -i src --no-error-summary 2>/dev/null; then
        echo "❌ Type checking failed. Fix type errors before continuing." >&2
        exit 2
    fi

    # Integration/testcontainer tests stay out of the per-turn loop (they spin
    # up Postgres); CI owns the full matrix. --testmon de-dupes execution so a
    # turn that didn't touch source collects fast and runs nothing.
    echo "Checking: pytest (unit)..."
    if ! uv run python -m pytest --testmon tests/unit --tb=short -q 2>&1; then
        echo "❌ Tests failed. ALL tests must pass before declaring done." >&2
        exit 2
    fi
fi

# ── Cockpit (TypeScript) ────────────────────────────────────────────────
if changed 'packages/cockpit/*.ts' 'packages/cockpit/*.tsx'; then
    ran_any=true
    # Resolve bun: prefer PATH, fall back to the standard install dir (the
    # hook shell may not source the profile that adds ~/.bun/bin).
    if command -v bun >/dev/null 2>&1; then
        BUN=bun
    elif [ -x "$HOME/.bun/bin/bun" ]; then
        BUN="$HOME/.bun/bin/bun"
    else
        echo "❌ cockpit changed but bun not found (PATH or ~/.bun/bin). Install bun or fix PATH." >&2
        exit 2
    fi

    cd "$project_dir/packages/cockpit"
    echo "Running cockpit quality checks ($project_dir/packages/cockpit)..."

    echo "Checking: biome (lint + format)..."
    if ! "$BUN" run check; then
        echo "❌ Biome check failed (lint/format). Fix before continuing." >&2
        exit 2
    fi

    echo "Checking: tsc (types)..."
    if ! "$BUN" x tsc --noEmit; then
        echo "❌ Type checking failed (tsc). Fix type errors before continuing." >&2
        exit 2
    fi

    # vitest has no testmon equivalent; gate it on the presence of test files
    # so it costs nothing (and doesn't error "no tests found") until cockpit
    # grows a suite. CI owns the full run regardless.
    if find src -name '*.test.ts*' -o -name '*.spec.ts*' 2>/dev/null | grep -q .; then
        echo "Checking: vitest..."
        if ! "$BUN" run test; then
            echo "❌ Tests failed. ALL tests must pass before declaring done." >&2
            exit 2
        fi
    else
        echo "No cockpit test files yet — skipping vitest."
    fi
fi

if [ "$ran_any" = false ]; then
    echo "No unpushed engine/cockpit source changes — skipping quality checks (CI covers the full suite)."
    exit 0
fi

echo "✅ All quality checks passed."
exit 0
