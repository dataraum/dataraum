#!/bin/bash
# End-of-turn quality gate for Claude Code
# Exit code 2 = block and show error to Claude (forces it to fix)
# Exit code 0 = success, continue

set -e

cd "$CLAUDE_PROJECT_DIR"

echo "Running quality checks..."

# Run ruff linter
echo "Checking: ruff..."
if ! ruff check . --quiet 2>/dev/null; then
    echo "❌ Linting failed. Fix lint errors before continuing." >&2
    exit 2
fi

# Run type checking (if mypy or pyright is available)
if command -v mypy &> /dev/null && [ -f "pyproject.toml" ]; then
    echo "Checking: mypy..."
    if ! mypy src/ --ignore-missing-imports --no-error-summary 2>/dev/null; then
        echo "❌ Type checking failed. Fix type errors before continuing." >&2
        exit 2
    fi
fi

# Run tests
echo "Checking: pytest..."
if ! pytest --tb=short -q 2>&1; then
    echo "❌ Tests failed. ALL tests must pass before declaring done." >&2
    exit 2
fi

echo "✅ All quality checks passed."
exit 0
