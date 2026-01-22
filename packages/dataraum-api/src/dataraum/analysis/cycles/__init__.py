"""Business Cycle Detection Module.

Uses an expert LLM agent with semantic metadata to detect business cycles
without hardcoded pattern matching.
"""

from dataraum.analysis.cycles.agent import BusinessCycleAgent
from dataraum.analysis.cycles.config import (
    format_cycle_vocabulary_for_context,
    get_cycle_types,
    get_cycles_config,
    get_domain_config,
    map_to_canonical_type,
)
from dataraum.analysis.cycles.db_models import (
    BusinessCycleAnalysisRun,
    DetectedBusinessCycle,
)
from dataraum.analysis.cycles.models import (
    BusinessCycleAnalysis,
    CycleStage,
    DetectedCycle,
    EntityFlow,
)

__all__ = [
    # Agent
    "BusinessCycleAgent",
    # Config
    "get_cycles_config",
    "get_cycle_types",
    "get_domain_config",
    "format_cycle_vocabulary_for_context",
    "map_to_canonical_type",
    # Pydantic models
    "BusinessCycleAnalysis",
    "DetectedCycle",
    "CycleStage",
    "EntityFlow",
    # DB models
    "BusinessCycleAnalysisRun",
    "DetectedBusinessCycle",
]
