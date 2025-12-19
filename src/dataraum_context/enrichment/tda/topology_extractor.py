# Re-export from new location for backward compatibility
# TODO: Remove this file after all imports are updated to use analysis.topology.tda.extractor
from dataraum_context.analysis.topology.tda.extractor import TableTopologyExtractor

__all__ = ["TableTopologyExtractor"]
