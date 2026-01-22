#!/bin/bash
# End-of-turn quality gate for Claude Code
# Exit code 2 = block and show error to Claude (forces it to fix)
# Exit code 0 = success, continue

set -e

cd "$CLAUDE_PROJECT_DIR/packages/dataraum-api"

echo "Running quality checks..."

# Run ruff linter
echo "Checking: ruff..."
if ! uv run python -m ruff check . --quiet 2>/dev/null; then
    echo "❌ Linting failed. Fix lint errors before continuing." >&2
    exit 2
fi

echo "Checking: mypy..."
if ! uv run python -m mypy src --no-error-summary 2>/dev/null; then
    echo "❌ Type checking failed. Fix type errors before continuing." >&2
    exit 2
fi

echo "Checking: pytest..."
if ! uv run python -m pytest tests --tb=short -q 2>&1; then
    echo "❌ Tests failed. ALL tests must pass before declaring done." >&2
    exit 2
fi

echo "✅ All quality checks passed."
exit 0
