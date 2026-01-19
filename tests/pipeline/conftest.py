"""Pipeline test fixtures."""

import pytest

from dataraum_context.pipeline.base import PhaseContext, PhaseResult
from dataraum_context.pipeline.phases.base import BasePhase


class MockPhase(BasePhase):
    """A mock phase for testing."""

    def __init__(
        self,
        name: str,
        dependencies: list[str] | None = None,
        outputs: list[str] | None = None,
        should_fail: bool = False,
        skip_reason: str | None = None,
    ):
        self._name = name
        self._dependencies = dependencies or []
        self._outputs = outputs or []
        self._should_fail = should_fail
        self._skip_reason = skip_reason
        self.run_count = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return f"Mock phase: {self._name}"

    @property
    def dependencies(self) -> list[str]:
        return self._dependencies

    @property
    def outputs(self) -> list[str]:
        return self._outputs

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        self.run_count += 1
        if self._should_fail:
            return PhaseResult.failed("Intentional failure")
        return PhaseResult.success(
            outputs={k: f"{self._name}_{k}" for k in self._outputs},
            records_processed=10,
            records_created=5,
        )

    def should_skip(self, ctx: PhaseContext) -> str | None:
        return self._skip_reason


@pytest.fixture
def mock_phase() -> type[MockPhase]:
    """Factory for creating mock phases."""
    return MockPhase
