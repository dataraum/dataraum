"""Data privacy and sampling for LLM analysis.

Simple placeholder implementation that:
1. Limits sample values to configured maximum
2. Redacts sensitive columns based on pattern matching
3. Placeholder for future SDV synthetic data generation
"""

import re
from typing import Any

from dataraum.analysis.statistics.models import ColumnProfile
from dataraum.llm.config import LLMPrivacy


class DataSampler:
    """Sample data for LLM analysis with privacy controls.

    This is a simple placeholder implementation. For production use
    with sensitive data, implement SDV synthetic data generation
    as a separate service.
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
    ) -> dict[str, list[Any]]:
        """Prepare sample values for LLM analysis.

        Args:
            column_profiles: Column profiles containing top values

        Returns:
            Dictionary mapping column_name -> sample_values
        """
        samples = {}

        for profile in column_profiles:
            column_name = profile.column_ref.column_name

            # Check if column matches sensitive pattern
            if self._is_sensitive(column_name):
                # Redact sensitive columns
                samples[column_name] = ["<REDACTED>"] * min(
                    self.config.redacted_sample_count, self.config.max_sample_values
                )
            else:
                # Use real top values from profile
                if profile.top_values:
                    samples[column_name] = [
                        vc.value for vc in profile.top_values[: self.config.max_sample_values]
                    ]
                else:
                    samples[column_name] = []

        return samples

    def _is_sensitive(self, column_name: str) -> bool:
        """Check if column name matches sensitive patterns.

        Args:
            column_name: Column name to check

        Returns:
            True if column is considered sensitive
        """
        for pattern in self.config.sensitive_patterns:
            if re.match(pattern, column_name, re.IGNORECASE):
                return True
        return False
