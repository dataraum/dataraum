"""Enrichment coordinator - orchestrates all enrichment steps."""

from typing import Any

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.enrichment.semantic import enrich_semantic
from dataraum_context.enrichment.temporal import enrich_temporal
from dataraum_context.enrichment.topology import enrich_topology
from dataraum_context.llm import LLMService


class EnrichmentCoordinator:
    """Coordinates enrichment pipeline.

    Orchestrates semantic, topological, and temporal enrichment
    in the appropriate order with proper error handling.
    """

    def __init__(
        self,
        llm_service: LLMService,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ):
        """Initialize coordinator.

        Args:
            llm_service: LLM service for semantic analysis
            duckdb_conn: DuckDB connection for data access
        """
        self.llm_service = llm_service
        self.duckdb_conn = duckdb_conn

    async def enrich_all(
        self,
        session: AsyncSession,
        table_ids: list[str],
        ontology: str = "general",
        include_topology: bool = True,
        include_temporal: bool = True,
    ) -> Result[dict[str, Any]]:
        """Run all enrichment steps.

        Args:
            session: Database session
            table_ids: Tables to enrich
            ontology: Ontology to use for semantic analysis
            include_topology: Run topology enrichment (TDA)
            include_temporal: Run temporal enrichment

        Returns:
            Result containing enrichment summary with counts
        """
        results = {}
        warnings = []

        # 1. Semantic enrichment (required - provides foundation for others)
        try:
            semantic_result = await enrich_semantic(
                session=session,
                llm_service=self.llm_service,
                table_ids=table_ids,
                ontology=ontology,
            )

            if not semantic_result.success or not semantic_result.value:
                return Result.fail(f"Semantic enrichment failed: {semantic_result.error}")

            results["semantic"] = {
                "annotations": len(semantic_result.value.annotations),
                "entities": len(semantic_result.value.entity_detections),
                "relationships": len(semantic_result.value.relationships),
            }

            if semantic_result.warnings:
                warnings.extend(semantic_result.warnings)

        except Exception as e:
            return Result.fail(f"Semantic enrichment exception: {e}")

        # 2. Topology enrichment (optional, TDA-based)
        if include_topology:
            try:
                topology_result = await enrich_topology(
                    session=session,
                    duckdb_conn=self.duckdb_conn,
                    table_ids=table_ids,
                )

                if topology_result.success and topology_result.value:
                    results["topology"] = {
                        "relationships": len(topology_result.value.relationships),
                        "join_paths": len(topology_result.value.join_paths),
                    }
                else:
                    warnings.append(f"Topology enrichment failed: {topology_result.error}")
                    results["topology"] = {"error": topology_result.error}

            except Exception as e:
                warnings.append(f"Topology enrichment exception: {e}")
                results["topology"] = {"error": str(e)}

        # 3. Temporal enrichment (optional, for time columns)
        if include_temporal:
            try:
                temporal_result = await enrich_temporal(
                    session=session,
                    duckdb_conn=self.duckdb_conn,
                    table_ids=table_ids,
                )

                if temporal_result.success and temporal_result.value:
                    results["temporal"] = {
                        "profiles": len(temporal_result.value.profiles),
                    }
                else:
                    warnings.append(f"Temporal enrichment failed: {temporal_result.error}")
                    results["temporal"] = {"error": temporal_result.error}

            except Exception as e:
                warnings.append(f"Temporal enrichment exception: {e}")
                results["temporal"] = {"error": str(e)}

        return Result.ok(results, warnings=warnings)
