"""Context assembly module - Phase 6.

This module assembles comprehensive context documents from all 5 pillars:
1. Statistical Context - From statistical profiling and correlation analysis
2. Topological Context - From TDA and relationship detection
3. Semantic Context - From LLM or manual semantic analysis
4. Temporal Context - From temporal enrichment
5. Quality Context - From quality synthesis

The context assembly layer is responsible for:
- Converting SQLAlchemy storage models to Pydantic business models
- Aggregating pillar-specific contexts into unified ContextDocument
- Coordinating LLM-generated content (summaries, suggested queries)
- Producing ContextDocument for AI consumption via MCP tools
"""

from dataraum_context.context.assembly import assemble_context_document
from dataraum_context.context.converters import (
    convert_statistical_profile,
    convert_topological_metrics,
)

__all__ = [
    "assemble_context_document",
    "convert_statistical_profile",
    "convert_topological_metrics",
]
