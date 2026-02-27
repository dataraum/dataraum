# DataRaum Context Engine

## Core Philosophy

This project prioritizes **correctness over speed**. We would rather have working code slowly than broken code quickly.

## Critical Rules

### Never Claim "Done" Until:
1. ALL tests pass (not just the file you changed)
2. You have verified actual output matches expected behavior
3. Type checking passes
4. Linting passes

If any of these fail, the task is NOT complete. Fix the issues before declaring success.

### The "Three Strikes" Rule
If you've attempted the same fix 3 times without success:
1. STOP making changes
2. Explain what you've tried and what you observed
3. Form a hypothesis about the ROOT CAUSE (not just symptoms)
4. Ask for guidance or propose a fundamentally different approach

Do not continue making random changes hoping something works.

### E2E Test Failures Mean Production Bugs
When an e2e test fails, the default assumption is: **the production code has a bug, not the test.**

1. Read the assertion, understand what behavior it expects.
2. Fix the **source code** (not the test).
3. If you genuinely believe the test expectation is wrong, STOP and explain your reasoning to the user before touching the test.

**Never do these to make e2e tests pass:**
- Weaken thresholds (e.g., changing `>= 0.5` to `>= 0.3`) — these are calibrated
- Add `pytest.mark.skip` or `pytest.mark.xfail`
- Delete or comment out assertions
- Change expected values to match broken output

You may edit `tests/e2e/` files when the user explicitly asks you to (e.g., adding new tests, restructuring fixtures, updating test infrastructure). The rule is about *motive*: never edit a test to make a failure disappear.

## Problem-Solving Standards

### Before Writing Any Code
- Understand the actual requirement, not what you assume it to be
- If the requirement is ambiguous, ask for clarification
- Consider edge cases upfront, not as an afterthought

### When Something Doesn't Work
1. **Read the actual error message** — quote it in your response
2. **Form a hypothesis** about WHY this error occurred
3. **Verify your hypothesis** before attempting a fix
4. **Make ONE targeted change** to test your hypothesis
5. **Observe the result** — did it confirm or refute your hypothesis?

Do NOT:
- Make multiple simultaneous changes
- Modify tests to make them pass (unless the test itself is wrong)
- Assume simple explanations for persistent problems
- Skip the hypothesis step

### Test Failures Are Information
- The test is probably right and your code is wrong
- Understand WHAT the test expects and WHY
- Only modify the test if you can articulate why the test's expectation is incorrect

## Work Decomposition Protocol

### Task Sizing
Before starting any work item, classify it:

| Size | Definition | Approach |
|------|-----------|----------|
| **S** | 1-3 files, <50 lines changed | Direct implementation, no plan needed |
| **M** | 3-8 files, <200 lines changed | Use Plan Mode, single session |
| **L** | 8+ files or 200+ lines | Linear document linked to issue, phased execution |
| **XL** | Spans multiple modules or repos | Plan approval required, integration branch, phased PRs |

### For M/L/XL tasks: mandatory plan structure
Plans live as Linear documents (linked to the relevant Linear issue) and must include:
1. **Scope**: What changes. What explicitly does NOT change.
2. **Files affected**: List every file. Mark read-only/do-not-touch files.
3. **Dependency order**: Which steps block which (e.g., A1a → A1b).
4. **Per-step acceptance criteria**: Verifiable outcomes, not vague descriptions.
5. **Test plan**: Which test files cover this step, what new tests are needed.
6. **Rollback**: How to safely abort mid-phase (revert strategy, branch state).

### Phasing Rules
- Each phase MUST leave ALL tests green — no half-done states
- Commit after each verified phase
- Never start phase N+1 until phase N passes all checks
- For L/XL: use integration branch, PR per phase into it

### Boundaries — Scope Creep Prevention
Every plan must declare:
```
DO change: [list of files]
DO NOT change: [list of files that must remain untouched]
```
If you find yourself wanting to edit a file not in the "DO change" list, STOP and discuss first. Unplanned edits to adjacent code are the #1 source of refactoring errors.

## After Writing Code — Verification Sequence

1. **Read back what you wrote** — does it match the requirement?
2. **Run the specific test file** for the module you changed
3. **Check imports** — no unused imports, no missing imports
4. **Check callers** — does the function signature match all call sites? (grep for usage)
5. **For refactors: verify the old code path is dead** — grep for references to removed/renamed functions
6. **For rewrites: verify nothing imports from `_legacy/`** — legacy code is reference only

## Testing

### Test Quality
- Each test should test ONE thing
- Test names should describe the expected behavior
- Tests should be independent — order should not matter
- Prefer many small, focused tests over few large tests

### Test-Driven Debugging
When fixing a bug:
1. First write a failing test that reproduces the bug
2. Then fix the bug
3. Verify the test now passes

### When Tests Become Bloated
If you find yourself iterating heavily on tests:
1. STOP
2. Step back and understand what behavior you're actually testing
3. Delete the bloated test
4. Write a fresh, minimal test for that single behavior

### Running Tests

This project uses `pytest-testmon` to only re-run tests affected by code changes.

**During development (after each code change):**
```bash
uv run pytest tests/unit/path/to/test_file.py -v
```

**After finishing a feature or fix:**
```bash
uv run pytest --testmon tests/unit -q
```

**Before declaring a task done:**
```bash
uv run pytest --testmon tests -q
```

**Rules:**
- **NEVER run `pytest tests/ -v` (full suite without testmon)** — takes 2+ minutes
- **Only run specific integration test files** when you changed integration-level code
- Unit tests: `tests/unit/` (~760 tests, ~13s). Integration: `tests/integration/` (~300 tests, ~2min)
- The end-of-turn hook runs testmon automatically — don't duplicate its work

### E2E Tests (when asked to run them)
E2E tests are expensive (real LLM calls, full pipeline). Only run when explicitly asked.
```bash
# Run all e2e tests
uv run pytest tests/e2e -v -m e2e

# Run a specific e2e file
uv run pytest tests/e2e/test_pipeline_phases.py -v
```

**When an e2e test fails, this is the protocol:**
1. Quote the full assertion error and traceback
2. Identify which production module is responsible (not the test file)
3. Read the relevant source code
4. Form a hypothesis about the bug in the production code
5. Fix the production code
6. Re-run only the failed test to verify
7. If you cannot determine the fix, report the failure with your analysis — do NOT patch the test

**Remember: `tests/e2e/` files are READ-ONLY. See "E2E Tests Are Read-Only Ground Truth" above.**

## Code Quality

### Changes Should Be Minimal
- Prefer small, targeted changes over broad rewrites
- Each commit should do ONE thing
- If you're changing many files, question whether you're taking the right approach

### Avoid Premature Abstraction
- Write concrete code first
- Only abstract when you see actual duplication (rule of three)
- Simple, readable code beats clever, abstract code

## Definition of Done

- [ ] All existing tests still pass
- [ ] New functionality has tests
- [ ] Type checking passes
- [ ] Linting passes
- [ ] Output verified (not just "it compiles")
- [ ] No debug statements left in code
- [ ] Error handling is in place
- [ ] Edge cases are handled

## Cross-Project Work

For work spanning multiple repos:
1. Plan lives in the *coordinating* repo
2. Each repo gets its own sub-plan with repo-local acceptance criteria
3. Integration testing happens in a dedicated step AFTER per-repo work
4. Never assume another repo's state — verify with tests/imports
5. Pin dependencies between repos during cross-project work

## Quality Gates (Automated via Hooks)

These are enforced mechanically — you don't need to remember them, but know they exist:
- **Post-edit**: `ruff format` runs after every file edit
- **End-of-turn**: `ruff check` + `mypy` + `pytest --testmon` blocks if any fail

## Current Work

**Before starting work, read these files to understand current state:**

| File | Purpose |
|------|---------|
| `docs/BACKLOG.md` | Task tracker — what's done, what's next |
| `docs/PROGRESS.md` | Session log — recent work history |

Check Linear for any active plan linked to the current task (Linear MCP is available).

## Project Reference

For architecture, module structure, tech stack, design decisions, prototypes, file locations, and dependencies — see `docs/REFERENCE.md`.

### Quick Reference Commands
```bash
# Run pipeline
dataraum run /path/to/data --output ./output

# Check status / entropy / contracts
dataraum status ./output
dataraum entropy ./output
dataraum contracts ./output

# Start MCP server
dataraum-mcp

# Run migration
python -m dataraum.storage.migrations up
```

### Code Patterns
```python
# Error handling — use Result type, not exceptions
from dataraum.core.models import Result

async def some_operation() -> Result[SomeOutput]:
    try:
        return Result.ok(output, warnings=["minor issue"])
    except SomeExpectedError as e:
        return Result.fail(str(e))

# Database connections — always use context managers
with manager.session_scope() as session:
    with manager.duckdb_cursor() as cursor:
        result = some_operation(cursor, session)
```

### Code Style
- Type hints on all functions
- Pydantic models for data classes
- No classes where functions suffice
- Max function length: ~50 lines
