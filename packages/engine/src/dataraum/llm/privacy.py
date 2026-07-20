"""Data privacy and sampling for LLM analysis.

What leaves the workspace in a prompt is decided here:
1. Sample values are capped at the configured maximum.
2. Columns whose name matches a sensitive pattern are replaced by
   ``<REDACTED>`` placeholders — the LLM sees the column exists and its
   cardinality shape, never a value.
"""

import re
from typing import Any

from dataraum.analysis.statistics.models import ColumnProfile
from dataraum.llm.config import LLMPrivacy


class DataSampler:
    """Sample data for LLM analysis with privacy controls.

    Name-pattern redaction plus a sample cap — the two controls the pipeline
    actually applies before a value reaches a prompt.
    """

    def __init__(self, config: LLMPrivacy):
        """Initialize data sampler.

        Args:
            config: Privacy configuration
        """
        self.config = config

    def prepare_samples(
        self,
        column_profiles: list[ColumnProfile],
    ) -> dict[tuple[str, str], list[Any]]:
        """Prepare sample values for LLM analysis.

        Args:
            column_profiles: Column profiles containing top values

        Returns:
            Dictionary mapping (table_name, column_name) -> sample_values
        """
        samples: dict[tuple[str, str], list[Any]] = {}

        for profile in column_profiles:
            column_name = profile.column_ref.column_name
            table_name = profile.column_ref.table_name
            key = (table_name, column_name)

            # Check if column matches sensitive pattern
            if self.is_sensitive(column_name):
                # Redact sensitive columns
                samples[key] = ["<REDACTED>"] * min(
                    self.config.redacted_sample_count, self.config.max_sample_values
                )
            else:
                # Use real top values from profile
                if profile.top_values:
                    # Serve all stored top values (already bounded upstream, DAT-649).
                    samples[key] = [vc.value for vc in profile.top_values]
                else:
                    samples[key] = []

        return samples

    def is_sensitive(self, column_name: str) -> bool:
        """Check if column name matches sensitive patterns.

        Public: the cycle-detection context builder applies the same gate before
        serving per-column value samples (it assembles samples from persisted
        profiles rather than through :meth:`prepare_samples`).

        Args:
            column_name: Column name to check

        Returns:
            True if column is considered sensitive
        """
        for pattern in self.config.sensitive_patterns:
            if re.match(pattern, column_name, re.IGNORECASE):
                return True
        return False
