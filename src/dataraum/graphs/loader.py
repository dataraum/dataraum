"""Transformation graph loader.

Loads metric graphs from YAML files in vertical config directories.

Usage:
    from dataraum.graphs.loader import GraphLoader

    loader = GraphLoader(vertical="finance")
    loader.load_all()
    metrics = loader.get_metric_graphs()
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .models import (
    AppliesTo,
    Classification,
    GraphMetadata,
    GraphSource,
    GraphStep,
    GraphType,
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
    """Load transformation graphs from YAML files.

    Directory structure:
        config/graphs/
        ├── filters/
        │   ├── system/     # System-defined technical filters
        │   └── user/       # User-defined scope filters
        └── metrics/
            ├── working_capital/
            ├── liquidity/
            └── profitability/
    """

    def __init__(self, graphs_dir: Path | None = None, *, vertical: str | None = None):
        """Initialize loader.

        Args:
            graphs_dir: Root directory containing graphs.
                        Defaults to config/verticals/<vertical>/
            vertical: Vertical name, required when graphs_dir is None.
        """
        if graphs_dir is None:
            if vertical is None:
                raise ValueError("vertical is required when graphs_dir is not provided")
            from dataraum.core.vertical import VerticalConfig

            graphs_dir = VerticalConfig(vertical).base_dir
        self.graphs_dir = graphs_dir
        self.graphs: dict[str, TransformationGraph] = {}
        self._load_errors: list[GraphLoadError] = []

    def load_all(self) -> dict[str, TransformationGraph]:
        """Load all transformation graphs from the directory.

        Returns:
            Dict mapping graph_id to TransformationGraph
        """
        self.graphs.clear()
        self._load_errors.clear()

        if not self.graphs_dir.exists():
            return {}

        # Load metrics
        metrics_dir = self.graphs_dir / "metrics"
        if metrics_dir.exists():
            self._load_directory(metrics_dir, GraphType.METRIC)

        return self.graphs

    def _load_directory(self, directory: Path, expected_type: GraphType) -> None:
        """Recursively load graphs from a directory.

        Supports multi-document YAML files (separated by ---).
        """
        for yaml_file in directory.rglob("*.yaml"):
            try:
                graphs = self.load_graphs_from_file(yaml_file)
                for graph in graphs:
                    if graph.graph_type != expected_type:
                        self._load_errors.append(
                            GraphLoadError(
                                yaml_file,
                                f"Expected {expected_type.value} graph '{graph.graph_id}', "
                                f"got {graph.graph_type.value}",
                            )
                        )
                        continue
                    self.graphs[graph.graph_id] = graph
            except GraphLoadError as e:
                self._load_errors.append(e)
            except Exception as e:
                self._load_errors.append(GraphLoadError(yaml_file, str(e)))

    def load_graphs_from_file(self, yaml_path: Path) -> list[TransformationGraph]:
        """Load all transformation graphs from a YAML file.

        Supports multi-document YAML files (separated by ---).

        Args:
            yaml_path: Path to the YAML file

        Returns:
            List of TransformationGraph instances

        Raises:
            GraphLoadError: If the YAML is invalid or missing required fields
        """
        with open(yaml_path) as f:
            documents = list(yaml.safe_load_all(f))

        graphs = []
        for doc in documents:
            if doc:  # Skip empty documents
                graphs.append(self._parse_graph(yaml_path, doc))

        return graphs

    def _parse_graph(self, path: Path, data: dict[str, Any]) -> TransformationGraph:
        """Parse a graph from YAML data."""
        # Required fields
        graph_id = data.get("graph_id")
        if not graph_id:
            raise GraphLoadError(path, "Missing required field: graph_id")

        graph_type_str = data.get("graph_type")
        if not graph_type_str:
            raise GraphLoadError(path, "Missing required field: graph_type")

        try:
            graph_type = GraphType(graph_type_str)
        except ValueError as e:
            raise GraphLoadError(path, f"Invalid graph_type: {graph_type_str}") from e

        version = data.get("version", "1.0")

        # Parse metadata
        metadata = self._parse_metadata(path, data.get("metadata", {}))

        # Parse output definition
        output = self._parse_output(path, data.get("output", {}), graph_type)

        # Parse parameters
        parameters = self._parse_parameters(data.get("parameters", {}))

        # Parse steps/dependencies
        steps = self._parse_steps(path, data.get("dependencies", {}))

        # Parse interpretation (for metrics)
        interpretation = self._parse_interpretation(data.get("interpretation"))

        return TransformationGraph(
            graph_id=graph_id,
            graph_type=graph_type,
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

        # Parse applies_to criteria
        applies_to = None
        applies_to_data = data.get("applies_to")
        if applies_to_data and isinstance(applies_to_data, dict):
            applies_to = AppliesTo(
                semantic_role=applies_to_data.get("semantic_role"),
                data_type=applies_to_data.get("data_type"),
                column_pattern=applies_to_data.get("column_pattern"),
                column_pairs=applies_to_data.get("column_pairs"),
                has_profile=applies_to_data.get("has_profile"),
            )

        return GraphMetadata(
            name=name,
            description=data.get("description", ""),
            category=data.get("category", ""),
            source=source,
            created_by=data.get("created_by"),
            created_at=data.get("created_at"),
            tags=data.get("tags", []),
            applies_to=applies_to,
            inspiration_snippet_id=data.get("inspiration_snippet_id"),
        )

    def _parse_output(self, path: Path, data: dict[str, Any], graph_type: GraphType) -> OutputDef:
        """Parse output definition."""
        output_type_str = data.get("type", "")

        # Default output type based on graph type
        if not output_type_str:
            if graph_type == GraphType.FILTER:
                output_type_str = "classification"
            else:
                output_type_str = "scalar"

        try:
            output_type = OutputType(output_type_str)
        except ValueError as e:
            raise GraphLoadError(path, f"Invalid output type: {output_type_str}") from e

        return OutputDef(
            output_type=output_type,
            categories=data.get("categories"),
            metric_id=data.get("metric_id"),
            unit=data.get("unit"),
            decimal_places=data.get("decimal_places"),
        )

    def _parse_parameters(self, data: dict[str, Any] | list[Any]) -> list[ParameterDef]:
        """Parse parameter definitions.

        Supports both dict format (name as key) and list format (name as field).
        """
        parameters = []

        # Handle list format: [{name: "x", param_type: "int", ...}, ...]
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

        # Handle dict format: {name: {type: "int", ...}, ...}
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
        level = data.get("level", 1)

        step_type_str = data.get("type", "extract")
        try:
            step_type = StepType(step_type_str)
        except ValueError as e:
            raise GraphLoadError(path, f"Invalid step type for {step_id}: {step_type_str}") from e

        # Parse source (for extract steps)
        source = None
        source_data = data.get("source")
        if source_data and isinstance(source_data, dict):
            source = StepSource(
                table=source_data.get("table"),
                column=source_data.get("column"),
                standard_field=source_data.get("standard_field"),
                statement=source_data.get("statement"),
            )

        # Parse on_false/on_true classification (for predicates)
        on_false = None
        on_true = None
        if on_false_str := data.get("on_false"):
            try:
                on_false = Classification(on_false_str)
            except ValueError:
                pass
        if on_true_str := data.get("on_true"):
            try:
                on_true = Classification(on_true_str)
            except ValueError:
                pass

        # Parse validations
        validations = []
        for val_data in data.get("validation", []):
            if isinstance(val_data, dict):
                validations.append(
                    StepValidation(
                        condition=val_data.get("condition", ""),
                        severity=val_data.get("severity", "warning"),
                        message=val_data.get("message", ""),
                    )
                )

        return GraphStep(
            step_id=step_id,
            level=level,
            step_type=step_type,
            source=source,
            aggregation=data.get("aggregation"),
            value=data.get("value") or data.get("default"),
            parameter=data.get("parameter"),
            condition=data.get("condition"),
            on_false=on_false,
            on_true=on_true,
            reason=data.get("reason"),
            expression=data.get("expression"),
            logic=data.get("logic"),
            depends_on=data.get("depends_on", []),
            validations=validations,
            enabled=data.get("enabled"),
            output_step=data.get("output_step", False),
            severity=data.get("severity"),
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

    # Accessor methods

    def get_metric_graphs(self) -> list[TransformationGraph]:
        """Get all metric graphs."""
        return [g for g in self.graphs.values() if g.graph_type == GraphType.METRIC]

    def get_load_errors(self) -> list[GraphLoadError]:
        """Get any errors encountered during loading."""
        return self._load_errors.copy()

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

    def validate_standard_fields(self, vertical: str) -> list[str]:
        """Warn about standard_field values not found in ontology.

        Args:
            vertical: Vertical name (e.g. 'finance')

        Returns:
            List of warning messages for unknown fields
        """
        from dataraum.analysis.semantic.ontology import OntologyLoader

        loader = OntologyLoader()
        ontology = loader.load(vertical)
        if not ontology:
            return []

        concept_names = {c.name for c in ontology.concepts}
        abstract_fields = self.get_all_abstract_fields()
        unknown = abstract_fields - concept_names

        warnings = []
        for field_name in sorted(unknown):
            warnings.append(f"standard_field '{field_name}' not found in {vertical} ontology")
        return warnings
