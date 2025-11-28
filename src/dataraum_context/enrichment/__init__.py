"""Enrichment layer - semantic, topological, and temporal metadata extraction."""

from dataraum_context.enrichment.semantic import enrich_semantic
from dataraum_context.enrichment.topology import enrich_topology

__all__ = [
    "enrich_semantic",
    "enrich_topology",
]
