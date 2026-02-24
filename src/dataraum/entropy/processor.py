"""Entropy processor for running detectors.

This module orchestrates entropy detection by running all applicable
detectors for a given context and returning raw EntropyObjects.

Usage:
    from dataraum.entropy.processor import EntropyProcessor

    processor = EntropyProcessor()
    entropy_objects = processor.process_column(
        table_name="orders",
        column_name="amount",
        analysis_results={
            "typing": type_candidate,
            "statistics": column_profile,
            "semantic": semantic_annotation,
        }
    )
"""

from typing import Any

from dataraum.core.logging import get_logger
from dataraum.entropy.detectors.base import (
    DetectorContext,
    DetectorRegistry,
    get_default_registry,
)
from dataraum.entropy.models import EntropyObject

logger = get_logger(__name__)


class EntropyProcessor:
    """Orchestrates entropy detection across detectors.

    Runs detectors and returns raw EntropyObject results.
    """

    def __init__(
        self,
        registry: DetectorRegistry | None = None,
    ):
        """Initialize processor.

        Args:
            registry: Detector registry to use. Defaults to global registry.
        """
        self.registry = registry or get_default_registry()

    def process_column(
        self,
        table_name: str,
        column_name: str,
        analysis_results: dict[str, Any],
        source_id: str | None = None,
        table_id: str | None = None,
        column_id: str | None = None,
    ) -> list[EntropyObject]:
        """Process a single column and return its entropy objects.

        Args:
            table_name: Name of the table
            column_name: Name of the column
            analysis_results: Dict of module name to analysis result
            source_id: Optional source ID
            table_id: Optional table ID
            column_id: Optional column ID

        Returns:
            List of EntropyObject instances from all detectors
        """
        context = DetectorContext(
            source_id=source_id,
            table_id=table_id,
            table_name=table_name,
            column_id=column_id,
            column_name=column_name,
            analysis_results=analysis_results,
        )

        return self._run_detectors(context)

    def _run_detectors(self, context: DetectorContext) -> list[EntropyObject]:
        """Run all applicable detectors and collect results.

        Args:
            context: Detector context

        Returns:
            List of EntropyObject instances from all detectors
        """
        detectors = self.registry.get_runnable_detectors(context)

        if not detectors:
            logger.debug(f"No detectors can run for {context.target_ref}")
            return []

        logger.debug(f"Running {len(detectors)} detectors for {context.target_ref}")

        all_objects: list[EntropyObject] = []

        for detector in detectors:
            try:
                objects = detector.detect(context)
                all_objects.extend(objects)
            except Exception as e:
                logger.error(f"Detector {detector.detector_id} failed: {e}")

        return all_objects
