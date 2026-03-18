"""Tests for pipeline overrides module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from dataraum.pipeline.overrides import (
    apply_postprocess_overrides,
    apply_relationship_confirmations,
    apply_semantic_overrides,
)


class TestApplySemanticOverrides:
    """Test apply_semantic_overrides with mock session/config."""

    def test_no_overrides_key(self) -> None:
        """Config without 'overrides' is a no-op."""
        session = MagicMock()
        apply_semantic_overrides(session, {}, ["t1"])
        session.execute.assert_not_called()

    def test_empty_overrides(self) -> None:
        """Empty overrides dict is a no-op."""
        session = MagicMock()
        apply_semantic_overrides(session, {"overrides": {}}, ["t1"])
        session.execute.assert_not_called()

    def test_non_dict_overrides(self) -> None:
        """Non-dict overrides value is a no-op."""
        session = MagicMock()
        apply_semantic_overrides(session, {"overrides": "invalid"}, ["t1"])
        session.execute.assert_not_called()


class TestApplyRelationshipConfirmations:
    """Test apply_relationship_confirmations with mock session/config."""

    def test_no_confirmed_relationships(self) -> None:
        """Config without confirmed_relationships is a no-op."""
        session = MagicMock()
        apply_relationship_confirmations(session, {}, ["t1"])
        session.execute.assert_not_called()

    def test_empty_confirmed(self) -> None:
        """Empty confirmed_relationships is a no-op."""
        session = MagicMock()
        apply_relationship_confirmations(
            session, {"overrides": {"confirmed_relationships": {}}}, ["t1"]
        )
        session.execute.assert_not_called()

    def test_invalid_key_format_skipped(self) -> None:
        """Keys without '->' are skipped."""
        session = MagicMock()
        # Need to mock table lookup to get past the initial guard
        tables_result = MagicMock()
        tables_result.scalars.return_value.all.return_value = []
        session.execute.return_value = tables_result

        apply_relationship_confirmations(
            session,
            {"overrides": {"confirmed_relationships": {"invalid_key": {"type": "fk"}}}},
            ["t1"],
        )
        # Should not crash — just skip the invalid key


class TestApplyPostprocessOverrides:
    """Test the top-level apply_postprocess_overrides function."""

    def test_calls_sub_functions(self) -> None:
        """Verify it loads config and calls both sub-functions."""
        session = MagicMock()

        # Mock table query to return one table
        mock_table = MagicMock()
        mock_table.table_id = "t1"
        tables_result = MagicMock()
        tables_result.scalars.return_value.all.return_value = [mock_table]
        session.execute.return_value = tables_result

        with (
            patch(
                "dataraum.core.config.load_phase_config",
                return_value={"overrides": {}},
            ) as mock_load,
            patch(
                "dataraum.pipeline.overrides.apply_semantic_overrides",
            ) as mock_semantic,
            patch(
                "dataraum.pipeline.overrides.apply_relationship_confirmations",
            ) as mock_rel,
            patch("dataraum.entropy.config.clear_entropy_config_cache"),
        ):
            apply_postprocess_overrides(session, "src-1", "/tmp/config")

        mock_load.assert_called_once()
        mock_semantic.assert_called_once()
        mock_rel.assert_called_once()

    def test_no_tables_returns_early(self) -> None:
        """If no typed tables exist, returns without calling overrides."""
        session = MagicMock()
        tables_result = MagicMock()
        tables_result.scalars.return_value.all.return_value = []
        session.execute.return_value = tables_result

        with (
            patch(
                "dataraum.core.config.load_phase_config",
                return_value={},
            ),
            patch(
                "dataraum.pipeline.overrides.apply_semantic_overrides",
            ) as mock_semantic,
        ):
            apply_postprocess_overrides(session, "src-1", "/tmp/config")

        mock_semantic.assert_not_called()
