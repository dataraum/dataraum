"""Transformation graph parser.

Parses already-merged metric-graph definition dicts into
:class:`~dataraum.graphs.models.TransformationGraph` objects. The definitions
come from the overlay-aware declared set
(:func:`dataraum.graphs.config.get_metric_definitions` — shipped graphs ⊕
``metric`` overlay teach rows); the loader no longer reads directories itself
(DAT-481 retired the file-only ``load_all`` footgun — see #264).

Usage:
    from dataraum.graphs.loader import GraphLoader
    from dataraum.graphs.config import get_metric_definitions

    loader = GraphLoader()
    loader.graphs.update(loader.graphs_from_definitions(get_metric_definitions("finance")))
    metrics = loader.get_metric_graphs()
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .models import (
    GraphMetadata,
    GraphSource,
    GraphStep,
    Interpretation,
    InterpretationRange,
    OutputDef,
    OutputType,
    ParameterDef,
    StepSource,
    StepType,
    StepValidation,
    TransformationGraph,
)


class GraphLoadError(Exception):
    """Error loading a transformation graph."""

    def __init__(self, path: Path, message: str):
        self.path = path
        self.message = message
        super().__init__(f"{path}: {message}")


class GraphLoader:
    """Parse metric transformation-graph definition dicts into graphs.

    Seeded via :meth:`graphs_from_definitions` from the overlay-aware declared
    set (:func:`dataraum.graphs.config.get_metric_definitions`); holds the
    parsed graphs in :attr:`graphs`. It does NOT read directories — DAT-481
    retired the file-only ``load_all`` (the #264 footgun: it bypassed the
    overlay, so framed/taught metrics were invisible).
    """

    def __init__(self) -> None:
        self.graphs: dict[str, TransformationGraph] = {}

    def graphs_from_definitions(
        self, definitions: dict[str, dict[str, Any]]
    ) -> dict[str, TransformationGraph]:
        """Parse already-merged graph definition dicts into graphs.

        The single metric-parse entry point. BOTH the operating_model metrics
        phase AND the add_source semantic grounding-hint path (``ground_columns``)
        load their declared set via
        :func:`dataraum.graphs.config.get_metric_definitions` (shipped graphs ⊕
        ``metric`` overlay teach rows) and parse them here, so a taught/framed
        metric is groundable + executable exactly like a shipped one.

        Raises:
            GraphLoadError: a definition is malformed (missing ``graph_id`` /
                ``metadata.name`` / invalid enum). The metrics phase catches this
                per metric and records the artifact as declared-with-reason, so a
                broken definition is visibly impossible, never silently dropped.
        """
        graphs: dict[str, TransformationGraph] = {}
        for graph_id, data in definitions.items():
            # A sentinel "path" — these dicts come from the overlay-merged
            # collection, not a file on disk; it only labels parse errors.
            graphs[graph_id] = self._parse_graph(Path(f"<overlay:{graph_id}>"), data)
        return graphs

    def _parse_graph(self, path: Path, data: dict[str, Any]) -> TransformationGraph:
        """Parse a graph from YAML data."""
        graph_id = data.get("graph_id")
        if not graph_id:
            raise GraphLoadError(path, "Missing required field: graph_id")

        version = data.get("version", "1.0")

        metadata = self._parse_metadata(path, data.get("metadata", {}))
        output = self._parse_output(path, data.get("output", {}))
        parameters = self._parse_parameters(data.get("parameters", {}))
        steps = self._parse_steps(path, data.get("dependencies", {}))
        interpretation = self._parse_interpretation(data.get("interpretation"))

        return TransformationGraph(
            graph_id=graph_id,
            version=version,
            metadata=metadata,
            output=output,
            steps=steps,
            parameters=parameters,
            interpretation=interpretation,
        )

    def _parse_metadata(self, path: Path, data: dict[str, Any]) -> GraphMetadata:
        """Parse graph metadata."""
        name = data.get("name", "")
        if not name:
            raise GraphLoadError(path, "Missing metadata.name")

        source_str = data.get("source", "system")
        try:
            source = GraphSource(source_str)
        except ValueError as e:
            raise GraphLoadError(path, f"Invalid source: {source_str}") from e

        return GraphMetadata(
            name=name,
            description=data.get("description", ""),
            category=data.get("category", ""),
            source=source,
            created_by=data.get("created_by"),
            created_at=data.get("created_at"),
            tags=data.get("tags", []),
            inspiration_snippet_id=data.get("inspiration_snippet_id"),
        )

    def _parse_output(self, path: Path, data: dict[str, Any]) -> OutputDef:
        """Parse output definition."""
        output_type_str = data.get("type", "scalar")

        try:
            output_type = OutputType(output_type_str)
        except ValueError as e:
            raise GraphLoadError(path, f"Invalid output type: {output_type_str}") from e

        return OutputDef(
            output_type=output_type,
            metric_id=data.get("metric_id"),
            unit=data.get("unit"),
            decimal_places=data.get("decimal_places"),
        )

    def _parse_parameters(self, data: dict[str, Any] | list[Any]) -> list[ParameterDef]:
        """Parse parameter definitions.

        Supports both dict format (name as key) and list format (name as field).
        """
        parameters = []

        if isinstance(data, list):
            for param_data in data:
                if isinstance(param_data, dict) and "name" in param_data:
                    parameters.append(
                        ParameterDef(
                            name=param_data["name"],
                            param_type=param_data.get("param_type", "string"),
                            default=param_data.get("default"),
                            description=param_data.get("description"),
                            options=param_data.get("options"),
                        )
                    )
            return parameters

        for name, param_data in data.items():
            if isinstance(param_data, dict):
                parameters.append(
                    ParameterDef(
                        name=name,
                        param_type=param_data.get("type", "string"),
                        default=param_data.get("default"),
                        description=param_data.get("description"),
                        options=param_data.get("options"),
                    )
                )
        return parameters

    def _parse_steps(self, path: Path, data: dict[str, Any]) -> dict[str, GraphStep]:
        """Parse graph steps from dependencies section."""
        steps = {}
        for step_id, step_data in data.items():
            steps[step_id] = self._parse_step(path, step_id, step_data)
        return steps

    def _parse_step(self, path: Path, step_id: str, data: dict[str, Any]) -> GraphStep:
        """Parse a single graph step."""
        step_type_str = data.get("type", "extract")
        try:
            step_type = StepType(step_type_str)
        except ValueError as e:
            raise GraphLoadError(path, f"Invalid step type for {step_id}: {step_type_str}") from e

        source = None
        source_data = data.get("source")
        if source_data and isinstance(source_data, dict):
            source = StepSource(
                table=source_data.get("table"),
                column=source_data.get("column"),
                standard_field=source_data.get("standard_field"),
                statement=source_data.get("statement"),
            )

        # Declared post-execution checks (DAT-616): the catalogue's per-extract
        # `validation:` block was dropped on the floor before — now parsed and
        # enforced by graphs.verifier against the executed value.
        validations = [
            StepValidation(
                condition=v["condition"],
                severity=v.get("severity", "error"),
                message=v.get("message", ""),
            )
            for v in data.get("validation") or []
            if isinstance(v, dict) and v.get("condition")
        ]

        return GraphStep(
            step_id=step_id,
            step_type=step_type,
            source=source,
            aggregation=data.get("aggregation"),
            value=data.get("value") or data.get("default"),
            parameter=data.get("parameter"),
            expression=data.get("expression"),
            depends_on=data.get("depends_on", []),
            output_step=data.get("output_step", False),
            validations=validations,
        )

    def _parse_interpretation(self, data: dict[str, Any] | None) -> Interpretation | None:
        """Parse interpretation rules for metrics."""
        if not data:
            return None

        ranges = []
        for range_data in data.get("ranges", []):
            ranges.append(
                InterpretationRange(
                    min_value=float(range_data.get("min", 0)),
                    max_value=float(range_data.get("max", 0)),
                    label=range_data.get("label", ""),
                    description=range_data.get("description", ""),
                )
            )

        return Interpretation(ranges=ranges) if ranges else None

    def get_metric_graphs(self) -> list[TransformationGraph]:
        """Get all metric graphs."""
        return list(self.graphs.values())

    def get_all_abstract_fields(self) -> set[str]:
        """Get all abstract fields used across all graphs.

        Returns:
            Set of abstract field names (from extract steps with standard_field)
        """
        fields: set[str] = set()
        for graph in self.graphs.values():
            for step in graph.steps.values():
                if step.source and step.source.standard_field:
                    fields.add(step.source.standard_field)
        return fields

