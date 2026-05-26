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

# Single change-gate for the whole quality suite. ruff, vulture, mypy and
# pytest all only inspect engine Python, so if there are no unpushed engine
# .py changes there's nothing any of them can newly flag — skip the lot.
# pytest is the motivator (~85s, and --testmon still imports/collects the
# whole suite before it can decide nothing changed), but the same logic
# applies to all four; mypy in particular is ~15s every turn otherwise.
#
# Baseline = what CI has already validated: the merge-base with the upstream
# tracking branch (falls back to origin/main, then main). We diff the
# working tree against that, so the guard sees ALL local-ahead work —
# uncommitted AND committed-but-unpushed. Comparing against HEAD alone would
# wrongly skip once a change is committed (e.g. stacking several commits
# locally before a push); the slate only clears when you push and the
# baseline advances. If no baseline resolves (detached/no-remote), fail safe
# by running the checks.
base=$(git -C "$project_dir" rev-parse --verify --quiet '@{upstream}') \
    || base=$(git -C "$project_dir" rev-parse --verify --quiet origin/main) \
    || base=$(git -C "$project_dir" rev-parse --verify --quiet main) \
    || base=""
mb=""
if [ -n "$base" ]; then
    mb=$(git -C "$project_dir" merge-base "$base" HEAD 2>/dev/null) || mb=""
fi
if [ -n "$mb" ] && git -C "$project_dir" diff --quiet "$mb" -- 'packages/engine/*.py'; then
    echo "No unpushed engine .py changes — skipping quality checks (CI covers the full suite)."
    exit 0
fi

cd "$project_dir/packages/engine"

echo "Running quality checks ($project_dir/packages/engine)..."

# Run ruff linter
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

echo "✅ All quality checks passed."
exit 0
