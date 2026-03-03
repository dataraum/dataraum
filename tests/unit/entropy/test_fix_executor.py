"""Tests for fix executor and action registry."""

from unittest.mock import patch

from dataraum.entropy.action_executors import get_seed_actions
from dataraum.entropy.fix_executor import (
    ActionCategory,
    ActionDefinition,
    ActionRegistry,
    FixExecutor,
    FixRequest,
    FixResult,
    get_default_action_registry,
)


class TestActionRegistry:
    def test_register_and_get(self):
        registry = ActionRegistry()
        defn = ActionDefinition(
            action_type="test_action",
            category=ActionCategory.ANNOTATE,
            description="Test action",
            hard_verifiable=False,
        )
        registry.register(defn)
        assert registry.has("test_action")
        assert registry.get("test_action") is defn

    def test_get_missing_returns_none(self):
        registry = ActionRegistry()
        assert registry.get("nonexistent") is None
        assert not registry.has("nonexistent")

    def test_list_actions(self):
        registry = ActionRegistry()
        defn1 = ActionDefinition(
            action_type="a1",
            category=ActionCategory.TRANSFORM,
            description="Action 1",
            hard_verifiable=True,
        )
        defn2 = ActionDefinition(
            action_type="a2",
            category=ActionCategory.ANNOTATE,
            description="Action 2",
            hard_verifiable=False,
        )
        registry.register(defn1)
        registry.register(defn2)
        assert len(registry.list_actions()) == 2


class TestSeedActions:
    def test_seed_actions_defined(self):
        actions = get_seed_actions()
        action_types = {a.action_type for a in actions}
        assert "override_type" in action_types
        assert "declare_unit" in action_types
        assert "add_business_name" in action_types
        assert "declare_null_meaning" in action_types
        assert "confirm_relationship" in action_types
        assert "create_filtered_view" in action_types

    def test_all_seed_actions_have_executors(self):
        for action in get_seed_actions():
            assert action.executor is not None, f"{action.action_type} has no executor"

    def test_seed_action_categories(self):
        actions = {a.action_type: a for a in get_seed_actions()}
        assert actions["override_type"].category == ActionCategory.TRANSFORM
        assert actions["declare_unit"].category == ActionCategory.ANNOTATE
        assert actions["create_filtered_view"].category == ActionCategory.TRANSFORM

    def test_hard_verifiable_flags(self):
        actions = {a.action_type: a for a in get_seed_actions()}
        assert actions["override_type"].hard_verifiable is True
        assert actions["declare_unit"].hard_verifiable is False
        assert actions["create_filtered_view"].hard_verifiable is True


class TestDefaultRegistry:
    def test_default_registry_has_seed_actions(self):
        registry = get_default_action_registry()
        assert registry.has("override_type")
        assert registry.has("declare_unit")
        assert len(registry.list_actions()) == 6


class TestFixExecutor:
    def test_unknown_action_returns_error(self):
        registry = ActionRegistry()
        executor = FixExecutor(registry)
        result = executor.execute(
            FixRequest(action_type="nonexistent", target="column:t.c"),
            session=None,  # type: ignore[arg-type]
        )
        assert not result.success
        assert "Unknown action type" in (result.error or "")

    def test_no_executor_returns_error(self):
        registry = ActionRegistry()
        registry.register(
            ActionDefinition(
                action_type="test",
                category=ActionCategory.ANNOTATE,
                description="Test",
                hard_verifiable=False,
                executor=None,
            )
        )
        executor = FixExecutor(registry)
        result = executor.execute(
            FixRequest(action_type="test", target="column:t.c"),
            session=None,  # type: ignore[arg-type]
        )
        assert not result.success
        assert "No executor" in (result.error or "")

    def test_executor_with_mock(self, tmp_path):
        """Test that the executor correctly calls the action and creates a decision."""
        calls: list[dict[str, str]] = []

        def mock_executor(
            session: object,
            duckdb_conn: object,
            target: str,
            parameters: dict,
        ) -> dict:
            calls.append({"target": target})
            return {"improved": True, "evidence": "Mock fix applied"}

        registry = ActionRegistry()
        registry.register(
            ActionDefinition(
                action_type="mock_fix",
                category=ActionCategory.ANNOTATE,
                description="Mock fix",
                hard_verifiable=False,
                executor=mock_executor,
            )
        )
        executor = FixExecutor(registry)

        # Create a real in-memory SQLite DB to avoid mapper initialization issues
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        from dataraum.storage.base import init_database

        engine = create_engine("sqlite:///:memory:")
        init_database(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        result = executor.execute(
            FixRequest(
                action_type="mock_fix",
                target="column:orders.amount",
                actor="user",
                gate_type="structural",
                blocked_phase="statistics",
            ),
            session=session,
        )

        assert result.success
        assert result.improved
        assert result.decision is not None
        assert result.decision.action_type == "mock_fix"
        assert result.decision.target == "column:orders.amount"
        assert result.decision.actor == "user"
        assert len(calls) == 1

        session.close()
        engine.dispose()

    def test_hard_verifiable_action_takes_snapshots(self, tmp_path):
        """Hard-verifiable actions should have before/after scores populated."""
        from dataraum.entropy.hard_snapshot import HardSnapshot

        calls: list[dict[str, str]] = []

        def mock_executor(
            session: object,
            duckdb_conn: object,
            target: str,
            parameters: dict,
        ) -> dict:
            calls.append({"target": target})
            return {"improved": True, "evidence": "Type overridden"}

        registry = ActionRegistry()
        registry.register(
            ActionDefinition(
                action_type="hard_fix",
                category="transform",
                description="Hard fix",
                hard_verifiable=True,
                executor=mock_executor,
            )
        )
        executor = FixExecutor(registry)

        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        from dataraum.storage.base import init_database

        engine = create_engine("sqlite:///:memory:")
        init_database(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        before_snap = HardSnapshot(
            scores={"type_fidelity": 0.6},
            detectors_run=["type_fidelity"],
        )
        after_snap = HardSnapshot(
            scores={"type_fidelity": 0.1},
            detectors_run=["type_fidelity"],
        )

        with patch(
            "dataraum.entropy.hard_snapshot.take_hard_snapshot",
            side_effect=[before_snap, after_snap],
        ):
            result = executor.execute(
                FixRequest(
                    action_type="hard_fix",
                    target="column:orders.amount",
                    actor="user",
                ),
                session=session,
            )

        assert result.success
        assert result.improved
        assert result.before_scores == {"type_fidelity": 0.6}
        assert result.after_scores == {"type_fidelity": 0.1}
        assert result.decision is not None
        assert result.decision.before_scores == {"type_fidelity": 0.6}
        assert result.decision.after_scores == {"type_fidelity": 0.1}
        # Score delta should be negative (improvement)
        assert result.score_deltas["type_fidelity"] < 0

        session.close()
        engine.dispose()

    def test_non_hard_verifiable_skips_snapshots(self, tmp_path):
        """Non-hard-verifiable actions should not take snapshots."""

        def mock_executor(
            session: object,
            duckdb_conn: object,
            target: str,
            parameters: dict,
        ) -> dict:
            return {"improved": False, "evidence": "Annotation added"}

        registry = ActionRegistry()
        registry.register(
            ActionDefinition(
                action_type="soft_fix",
                category="annotate",
                description="Soft fix",
                hard_verifiable=False,
                executor=mock_executor,
            )
        )
        executor = FixExecutor(registry)

        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        from dataraum.storage.base import init_database

        engine = create_engine("sqlite:///:memory:")
        init_database(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Should NOT call take_hard_snapshot at all
        with patch(
            "dataraum.entropy.hard_snapshot.take_hard_snapshot",
            side_effect=AssertionError("Should not be called"),
        ):
            result = executor.execute(
                FixRequest(
                    action_type="soft_fix",
                    target="column:orders.amount",
                ),
                session=session,
            )

        assert result.success
        assert not result.improved
        assert result.before_scores == {}
        assert result.after_scores == {}

        session.close()
        engine.dispose()


class TestFixResult:
    def test_score_deltas(self):
        result = FixResult(
            success=True,
            improved=True,
            before_scores={"type_fidelity": 0.6, "null_ratio": 0.3},
            after_scores={"type_fidelity": 0.1, "null_ratio": 0.3},
        )
        deltas = result.score_deltas
        assert deltas["type_fidelity"] < 0  # Improved
        assert deltas["null_ratio"] == 0  # Unchanged
