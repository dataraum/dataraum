"""Quality assessment framework (Pillar 5).

The quality module synthesizes quality metrics from all other pillars:
- Statistical quality (Pillar 1) - now in analysis/statistics
- Topological quality (Pillar 2)
- Semantic quality (Pillar 3)
- Temporal quality (Pillar 4) - now in analysis/temporal

Plus domain-specific quality rules for various business domains.

Architecture:
    analysis/
      statistics/      # Statistical quality metrics (Benford, outliers)
      temporal/        # Temporal quality metrics (consolidated module)

    quality/
      topological.py   # Topological quality metrics
      domains/         # Domain-specific quality rules
        financial.py   # Financial accounting
        marketing.py   # Marketing analytics (future)
      synthesis.py     # Aggregate quality from all pillars (Phase 5)

Usage:
    # Statistical quality (Phase 9A moved to analysis/statistics)
    from dataraum_context.analysis.statistics import (
        assess_statistical_quality,
        BenfordAnalysis,
        StatisticalQualityResult,
    )

    # Topological quality
    from dataraum_context.quality import topological

    # Temporal quality
    from dataraum_context.analysis import temporal

    # Domain-specific quality
    from dataraum_context.quality.domains import financial
    result = await financial.analyze_financial_quality(table_id, conn, session)

    # Multi-pillar quality synthesis (Phase 5)
    from dataraum_context.quality import synthesis
    quality_context = await synthesis.synthesize_quality_context(source_id, conn, session)
"""

from dataraum_context.quality import domains, synthesis, topological

__all__ = [
    "domains",
    "synthesis",
    "topological",
]
