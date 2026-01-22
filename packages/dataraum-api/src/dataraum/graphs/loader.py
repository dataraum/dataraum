"""Unified transformation graph loader.

Loads transformation graphs (filters and metrics) from YAML files.
Supports both system-defined and user-defined graphs.

Usage:
    from dataraum.graphs.loader import GraphLoader

    loader = GraphLoader()
    loader.load_all()

    # Get a specific graph
    graph = loader.get_graph("technical_quality")

    # Get all filter graphs
    filters = loader.get_filter_graphs()

    # Get all metric graphs
    metrics = loader.get_metric_graphs()
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .models import (
    AppliesTo,
    Classification,
    FilterRequirement,
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

    def __init__(self, graphs_dir: Path | None = None):
        """Initialize loader.

        Args:
            graphs_dir: Root directory containing graphs.
                        Defaults to config/graphs/
        """
        if graphs_dir is None:
            # Default: 4 levels up from src/dataraum/graphs/
            graphs_dir = Path(__file__).parent.parent.parent.parent / "config" / "graphs"
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

        # Load filters
        filters_dir = self.graphs_dir / "filters"
        if filters_dir.exists():
            self._load_directory(filters_dir, GraphType.FILTER)

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

    def load_graph(self, yaml_path: Path) -> TransformationGraph:
        """Load a single transformation graph from YAML.

        Args:
            yaml_path: Path to the YAML file

        Returns:
            TransformationGraph instance (first document only)

        Raises:
            GraphLoadError: If the YAML is invalid or missing required fields
        """
        graphs = self.load_graphs_from_file(yaml_path)
        if not graphs:
            raise GraphLoadError(yaml_path, "Empty YAML file")
        return graphs[0]

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

        # Parse filter requirements (for metrics)
        requires_filters = self._parse_filter_requirements(data.get("requires_filters", []))

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
            requires_filters=requires_filters,
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

    def _parse_filter_requirements(self, data: list[dict[str, Any]]) -> list[FilterRequirement]:
        """Parse filter requirements for metric graphs."""
        requirements = []
        for req_data in data:
            graph_id = req_data.get("graph_id")
            if graph_id:
                classification_str = req_data.get("required_classification", "clean")
                try:
                    classification = Classification(classification_str)
                except ValueError:
                    classification = Classification.CLEAN

                requirements.append(
                    FilterRequirement(
                        graph_id=graph_id,
                        required=req_data.get("required", True),
                        required_classification=classification,
                    )
                )
        return requirements

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

    def get_graph(self, graph_id: str) -> TransformationGraph | None:
        """Get a specific graph by ID."""
        return self.graphs.get(graph_id)

    def get_filter_graphs(self) -> list[TransformationGraph]:
        """Get all filter graphs."""
        return [g for g in self.graphs.values() if g.graph_type == GraphType.FILTER]

    def get_metric_graphs(self) -> list[TransformationGraph]:
        """Get all metric graphs."""
        return [g for g in self.graphs.values() if g.graph_type == GraphType.METRIC]

    def get_system_graphs(self) -> list[TransformationGraph]:
        """Get all system-defined graphs."""
        return [g for g in self.graphs.values() if g.metadata.source == GraphSource.SYSTEM]

    def get_user_graphs(self) -> list[TransformationGraph]:
        """Get all user-defined graphs."""
        return [g for g in self.graphs.values() if g.metadata.source == GraphSource.USER]

    def get_graphs_by_category(self, category: str) -> list[TransformationGraph]:
        """Get all graphs in a specific category."""
        return [g for g in self.graphs.values() if g.metadata.category == category]

    def get_load_errors(self) -> list[GraphLoadError]:
        """Get any errors encountered during loading."""
        return self._load_errors.copy()

    def get_dependent_metrics(self, filter_graph_id: str) -> list[TransformationGraph]:
        """Find all metrics that depend on a specific filter graph.

        Args:
            filter_graph_id: The filter graph ID

        Returns:
            List of metric graphs that require this filter
        """
        dependents = []
        for graph in self.get_metric_graphs():
            for req in graph.requires_filters:
                if req.graph_id == filter_graph_id:
                    dependents.append(graph)
                    break
        return dependents

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

    def validate_graph(self, graph: TransformationGraph) -> list[str]:
        """Validate a transformation graph for consistency.

        Args:
            graph: The graph to validate

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Check for output step
        output_step = graph.get_output_step()
        if not output_step:
            errors.append("No output_step defined")

        # Check dependency references
        step_ids = set(graph.steps.keys())
        for step in graph.steps.values():
            for dep in step.depends_on:
                if dep not in step_ids:
                    errors.append(f"Step {step.step_id} references unknown dependency: {dep}")

        # Check level ordering (dependencies should have lower level)
        for step in graph.steps.values():
            for dep_id in step.depends_on:
                if dep_id in step_ids:
                    dep_step = graph.steps[dep_id]
                    if dep_step.level >= step.level:
                        errors.append(
                            f"Step {step.step_id} (level {step.level}) depends on "
                            f"{dep_id} (level {dep_step.level}) - invalid level order"
                        )

        # Filter-specific checks
        if graph.graph_type == GraphType.FILTER:
            # Check predicate steps have classification actions
            for step in graph.steps.values():
                if step.step_type == StepType.PREDICATE:
                    if not step.on_false and not step.on_true:
                        errors.append(
                            f"Predicate step {step.step_id} has no on_false or on_true action"
                        )

        # Metric-specific checks
        if graph.graph_type == GraphType.METRIC:
            # Check that required filter graphs exist (if loader has them)
            for req in graph.requires_filters:
                if req.required and req.graph_id not in self.graphs:
                    # Not an error, just a warning - filter might be loaded separately
                    pass

        return errors

    def get_applicable_filters(
        self,
        column_name: str,
        semantic_role: str | None = None,
        data_type: str | None = None,
        has_profile: bool = False,
    ) -> list[TransformationGraph]:
        """Get filter graphs that apply to a column based on its metadata.

        Matches filters based on:
        - semantic_role: key, timestamp, measure, foreign_key
        - data_type: DOUBLE, DATE, VARCHAR, etc.
        - column_pattern: regex pattern matching column name
        - has_profile: whether statistical profile exists

        Args:
            column_name: The column name to match against patterns
            semantic_role: The semantic role of the column (from semantic analysis)
            data_type: The resolved data type of the column
            has_profile: Whether the column has a statistical profile

        Returns:
            List of filter graphs that apply to this column
        """
        import re

        applicable = []

        for graph in self.get_filter_graphs():
            applies_to = graph.metadata.applies_to
            if not applies_to:
                continue

            # Check semantic role match
            if applies_to.semantic_role:
                if semantic_role != applies_to.semantic_role:
                    continue

            # Check data type match
            if applies_to.data_type:
                if data_type != applies_to.data_type:
                    continue

            # Check column pattern match
            if applies_to.column_pattern:
                try:
                    if not re.match(applies_to.column_pattern, column_name, re.IGNORECASE):
                        continue
                except re.error:
                    # Invalid regex, skip this filter
                    continue

            # Check has_profile requirement
            if applies_to.has_profile is not None:
                if has_profile != applies_to.has_profile:
                    continue

            # Skip column_pairs for now - these require cross-column matching
            if applies_to.column_pairs:
                continue

            applicable.append(graph)

        return applicable

    def get_cross_column_filters(self) -> list[TransformationGraph]:
        """Get filter graphs that require cross-column matching.

        These are filters with column_pairs defined in applies_to,
        requiring special handling to match start/end date pairs, etc.

        Returns:
            List of filter graphs with column_pairs criteria
        """
        return [
            graph
            for graph in self.get_filter_graphs()
            if graph.metadata.applies_to and graph.metadata.applies_to.column_pairs
        ]

    def get_filters_for_dataset(
        self,
        columns: list[dict[str, Any]],
    ) -> dict[str, list[TransformationGraph]]:
        """Get all applicable filters for each column in a dataset.

        This is the main integration point for auto-applying quality rules
        during analysis phases.

        Args:
            columns: List of column metadata dicts, each with:
                - column_name: str (required)
                - semantic_role: str | None (from semantic analysis)
                - data_type: str | None (resolved type)
                - has_profile: bool (whether statistical profile exists)

        Returns:
            Dict mapping column_name to list of applicable filter graphs

        Example:
            >>> loader = GraphLoader()
            >>> loader.load_all()
            >>> columns = [
            ...     {"column_name": "id", "semantic_role": "key", "data_type": "BIGINT"},
            ...     {"column_name": "amount", "semantic_role": "measure", "data_type": "DOUBLE"},
            ...     {"column_name": "email", "data_type": "VARCHAR"},
            ... ]
            >>> filters = loader.get_filters_for_dataset(columns)
            >>> filters["id"]  # Returns key column filters
            >>> filters["amount"]  # Returns measure + double filters
            >>> filters["email"]  # Returns email pattern filter
        """
        result: dict[str, list[TransformationGraph]] = {}

        for col in columns:
            column_name = col.get("column_name", "")
            if not column_name:
                continue

            filters = self.get_applicable_filters(
                column_name=column_name,
                semantic_role=col.get("semantic_role"),
                data_type=col.get("data_type"),
                has_profile=col.get("has_profile", False),
            )
            result[column_name] = filters

        return result

    def get_quality_filter_summary(
        self,
        columns: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Get a summary of quality filters applicable to a dataset.

        Useful for displaying which quality checks will be applied.

        Args:
            columns: List of column metadata dicts

        Returns:
            Summary dict with:
                - total_filters: Total unique filters that apply
                - filters_by_column: Count of filters per column
                - filter_coverage: Columns with filters / total columns
                - filter_ids: List of all unique filter graph IDs
        """
        filters_by_column = self.get_filters_for_dataset(columns)

        all_filter_ids: set[str] = set()
        for col_filters in filters_by_column.values():
            for f in col_filters:
                all_filter_ids.add(f.graph_id)

        columns_with_filters = sum(1 for f in filters_by_column.values() if f)

        return {
            "total_filters": len(all_filter_ids),
            "filters_by_column": {col: len(filters) for col, filters in filters_by_column.items()},
            "filter_coverage": columns_with_filters / len(columns) if columns else 0,
            "filter_ids": sorted(all_filter_ids),
        }
