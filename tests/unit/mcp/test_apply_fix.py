"""Tests for the apply_fix MCP tool implementation."""

import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dataraum.entropy.fix_executor import (
    ActionCategory,
    ActionDefinition,
    ActionRegistry,
    FixExecutor,
    FixRequest,
)
from dataraum.storage.base import init_database


class TestApplyFixIntegration:
    """Test fix execution via the same path the MCP tool uses."""

    def test_fix_with_mock_executor(self):
        """Test a mock fix through the FixExecutor (same path as MCP apply_fix)."""
        calls: list[dict] = []

        def mock_executor(session, duckdb_conn, target, parameters):
            calls.append({"target": target, "params": parameters})
            return {"improved": True, "evidence": "Mock fix applied"}

        registry = ActionRegistry()
        registry.register(
            ActionDefinition(
                action_type="test_fix",
                category=ActionCategory.ANNOTATE,
                description="Test fix",
                hard_verifiable=False,
                executor=mock_executor,
            )
        )
        executor = FixExecutor(registry)

        engine = create_engine("sqlite:///:memory:")
        init_database(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        result = executor.execute(
            FixRequest(
                action_type="test_fix",
                target="column:orders.amount",
                parameters={"reason": "test"},
                actor="mcp_agent",
            ),
            session=session,
        )

        assert result.success
        assert result.improved
        assert result.decision is not None
        assert result.decision.actor == "mcp_agent"
        assert len(calls) == 1
        assert calls[0]["target"] == "column:orders.amount"
        assert calls[0]["params"] == {"reason": "test"}

        session.close()
        engine.dispose()

    def test_fix_unknown_action(self):
        registry = ActionRegistry()
        executor = FixExecutor(registry)

        result = executor.execute(
            FixRequest(action_type="nonexistent", target="column:t.c"),
            session=None,  # type: ignore[arg-type]
        )

        assert not result.success
        assert "Unknown action type" in (result.error or "")

    def test_fix_result_serializable(self):
        """Test that fix results can be serialized to JSON (as MCP tool does)."""
        calls = []

        def mock_executor(session, duckdb_conn, target, parameters):
            calls.append(True)
            return {"improved": True, "evidence": "Fixed it"}

        registry = ActionRegistry()
        registry.register(
            ActionDefinition(
                action_type="json_test",
                category=ActionCategory.TRANSFORM,
                description="JSON test",
                hard_verifiable=True,
                executor=mock_executor,
            )
        )
        executor = FixExecutor(registry)

        engine = create_engine("sqlite:///:memory:")
        init_database(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        result = executor.execute(
            FixRequest(
                action_type="json_test",
                target="column:t.c",
                actor="mcp_agent",
            ),
            session=session,
        )

        # Build the same dict the MCP tool builds
        output = {
            "success": result.success,
            "improved": result.improved,
            "action_type": "json_test",
            "target": "column:t.c",
        }
        if result.decision:
            output["decision"] = {
                "decision_id": result.decision.decision_id,
                "evidence": result.decision.evidence_summary,
                "actor": result.decision.actor,
            }

        # Must be JSON-serializable
        serialized = json.dumps(output)
        parsed = json.loads(serialized)
        assert parsed["success"] is True
        assert parsed["improved"] is True
        assert parsed["decision"]["actor"] == "mcp_agent"

        session.close()
        engine.dispose()
