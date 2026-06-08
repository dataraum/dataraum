"""Validation Agent - LLM-powered SQL generation for validation checks.

This agent generates SQL queries for validation checks by passing the full
schema (potentially multiple tables) to the LLM and letting it identify
relevant columns and generate cross-table JOINs when needed.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

import duckdb

from dataraum.analysis.validation.models import (
    GeneratedSQL,
    ValidationResult,
    ValidationSpec,
    ValidationSQLOutput,
    ValidationStatus,
)
from dataraum.analysis.validation.resolver import (
    format_multi_table_schema_for_prompt,
)
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
    ToolDefinition,
)

logger = get_logger(__name__)


# Prompt template name for SQL generation
SQL_GENERATION_TEMPLATE_NAME = "validation_sql"


class ValidationAgent(LLMFeature):
    """LLM-powered validation agent.

    Generates SQL for validation checks by passing multiple table schemas
    to the LLM for interpretation. The LLM can generate cross-table JOINs
    when validations require data from multiple tables.
    """

    MAX_TOKENS = 2000
    MAX_STORED_ROWS = 10
    DEFAULT_TOLERANCE = 0.01

    def validate_context(self, schema: dict[str, Any]) -> list[str]:
        """Validate that the schema has sufficient context for validation.

        Args:
            schema: Multi-table schema dict

        Returns:
            List of issues found (empty if context is sufficient)
        """
        issues = []

        tables = schema.get("tables", [])
        if not tables:
            issues.append("No tables available")
            return issues

        # Check for columns
        total_columns = sum(len(t.get("columns", [])) for t in tables)
        if total_columns == 0:
            issues.append("No columns found in any table")
            return issues

        # DAT-439 decision: zero semantic annotations stays a WARNING, never a
        # blocking issue. The absence is already durably visible — bind records
        # the pinned base-run map (including its empty ``semantic_runs``) into
        # every artifact's ``grounded_against`` provenance, and the workflow's
        # resolve activity warns on unresolved heads. Blocking here would
        # forbid validating legitimately annotation-free workspaces; the LLM
        # degrades gracefully to bare column names/types.
        columns_with_semantic = sum(
            1 for t in tables for c in t.get("columns", []) if c.get("semantic")
        )
        if columns_with_semantic == 0:
            logger.warning(
                "No semantic annotations found. "
                "Validation may be less accurate without column semantics."
            )

        return issues

    @staticmethod
    def _scope_table_ids_from_sql(
        sql: str,
        schema: dict[str, Any],
        all_table_ids: list[str],
    ) -> list[str]:
        """Derive which tables a validation SQL actually references.

        Matches the schema's known ``duckdb_path`` values against the SQL
        with word-boundary anchors — works for bare (``FROM x__y``) and
        quoted (``FROM "x__y"``) forms. Returns an empty list (with
        warning) if no tables can be resolved — never falls back to
        ``all_table_ids``.
        """
        # Post-DAT-341: duckdb_path is the bare ``<source>__<table>`` form;
        # tables resolve via the manager's ``USE lake.typed``.
        path_to_id: dict[str, str] = {}
        for t in schema.get("tables", []):
            duckdb_path = t.get("duckdb_path", t["table_name"])
            path_to_id[duckdb_path] = t.get("table_id", "")

        referenced_names = {
            name for name in path_to_id if re.search(rf"(?<!\w){re.escape(name)}(?!\w)", sql)
        }

        scoped_ids = [
            path_to_id[name] for name in referenced_names if path_to_id[name] in all_table_ids
        ]

        if not scoped_ids and referenced_names:
            logger.warning(
                "validation_table_scope_empty",
                referenced_tables=list(referenced_names),
                available_tables=list(path_to_id.keys()),
            )

        return scoped_ids

    def bind_validation(
        self,
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_ids: list[str],
        spec: ValidationSpec,
        schema: dict[str, Any],
    ) -> tuple[GeneratedSQL | None, ValidationResult | None]:
        """``validation.bind`` — ground a declared spec against the workspace.

        Generates SQL via the LLM and proves it binds (EXPLAIN). Exactly one
        element of the returned tuple is set:

        * ``(generated, None)`` — the spec grounds: SQL exists and plans.
        * ``(None, failure)`` — ungroundable: SQL generation failed (ERROR),
          the LLM declared it inapplicable to this workspace (SKIPPED), or
          the SQL doesn't plan (ERROR). The failure result carries the reason
          the caller records on the artifact (visibly impossible, never
          silently absent).
        """
        table_names = [t["table_name"] for t in schema.get("tables", [])]
        combined_table_name = ", ".join(table_names)

        # Generate SQL via LLM
        sql_result = self._generate_sql(spec, schema)

        if not sql_result.success or not sql_result.value:
            return None, ValidationResult(
                validation_id=spec.validation_id,
                spec_name=spec.name,
                status=ValidationStatus.ERROR,
                severity=spec.severity,
                table_ids=table_ids,
                table_name=combined_table_name,
                passed=False,
                message=sql_result.error or "SQL generation failed",
            )

        generated = sql_result.value

        # Scope table_ids to tables actually referenced in the SQL
        if generated.sql_query:
            scoped_table_ids = self._scope_table_ids_from_sql(
                generated.sql_query, schema, table_ids
            )
        else:
            scoped_table_ids = []

        # The LLM declared the validation inapplicable to this workspace
        if not generated.is_valid:
            return None, ValidationResult(
                validation_id=spec.validation_id,
                spec_name=spec.name,
                status=ValidationStatus.SKIPPED,
                severity=spec.severity,
                table_ids=scoped_table_ids,
                table_name=combined_table_name,
                passed=False,
                message=generated.validation_error or "Validation cannot be performed",
                columns_used=generated.columns_used,
            )

        # Prove the binding: the SQL must plan before the spec counts as grounded
        try:
            duckdb_conn.execute(f"EXPLAIN {generated.sql_query}")
        except Exception as e:
            logger.error("sql_validation_failed", validation_id=spec.validation_id, error=str(e))
            return None, ValidationResult(
                validation_id=spec.validation_id,
                spec_name=spec.name,
                status=ValidationStatus.ERROR,
                severity=spec.severity,
                table_ids=scoped_table_ids,
                table_name=combined_table_name,
                passed=False,
                message=f"Generated SQL is invalid: {e}",
                sql_used=generated.sql_query,
                columns_used=generated.columns_used,
            )

        return generated, None

    def execute_validation(
        self,
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_ids: list[str],
        spec: ValidationSpec,
        schema: dict[str, Any],
        generated: GeneratedSQL,
    ) -> ValidationResult:
        """``validation.execute`` — run a grounded spec's SQL and evaluate it.

        PASSED/FAILED is the *measurement* (the data's quality), not the
        lifecycle outcome — a grounded validation that fails its check still
        executed cleanly. Only an execution ERROR means the artifact did not
        reach ``executed``.
        """
        table_names = [t["table_name"] for t in schema.get("tables", [])]
        combined_table_name = ", ".join(table_names)
        scoped_table_ids = (
            self._scope_table_ids_from_sql(generated.sql_query, schema, table_ids)
            if generated.sql_query
            else []
        )

        try:
            result_obj = duckdb_conn.execute(generated.sql_query)
            col_names = [desc[0] for desc in result_obj.description]
            raw_rows = result_obj.fetchall()
            result_rows: list[dict[str, Any]] = [
                dict(zip(col_names, row, strict=True)) for row in raw_rows
            ]
            row_count = len(result_rows)

            # Evaluate results based on check type. ERROR = the evaluation is
            # INCONCLUSIVE (ran, but the result shape cannot be judged) — the
            # phase keeps the artifact at ``grounded`` with the reason, and
            # the result never pollutes the FAILED measurements (DAT-439).
            status, message, details = self._evaluate_result(
                spec=spec,
                result_rows=result_rows,
                row_count=row_count,
            )

            return ValidationResult(
                validation_id=spec.validation_id,
                spec_name=spec.name,
                status=status,
                severity=spec.severity,
                table_ids=scoped_table_ids,
                table_name=combined_table_name,
                passed=status == ValidationStatus.PASSED,
                message=message,
                details=details,
                sql_used=generated.sql_query,
                columns_used=generated.columns_used,
                result_rows=result_rows[: self.MAX_STORED_ROWS],
                row_count=row_count,
            )

        except Exception as e:
            logger.error("sql_execution_failed", validation_id=spec.validation_id, error=str(e))
            return ValidationResult(
                validation_id=spec.validation_id,
                spec_name=spec.name,
                status=ValidationStatus.ERROR,
                severity=spec.severity,
                table_ids=scoped_table_ids,
                table_name=combined_table_name,
                passed=False,
                message=f"SQL execution error: {e}",
                sql_used=generated.sql_query,
                columns_used=generated.columns_used,
            )

    def _generate_sql(
        self,
        spec: ValidationSpec,
        schema: dict[str, Any],
    ) -> Result[GeneratedSQL]:
        """Generate SQL via LLM using tool-based structured output.

        Uses Pydantic model as tool definition for reliable structured output.

        Args:
            spec: Validation spec
            schema: Multi-table schema with relationships

        Returns:
            Result containing GeneratedSQL
        """
        # Get feature config first
        feature_config = self.config.features.validation
        if not feature_config or not feature_config.enabled:
            return Result.fail("Validation feature is disabled in config")

        # Format schema for prompt with emphasis on exact column names
        schema_text = format_multi_table_schema_for_prompt(schema)

        # Build context for template
        sql_hints = f"<sql_hints>{spec.sql_hints}</sql_hints>" if spec.sql_hints else ""
        expected = (
            f"<expected_outcome>{spec.expected_outcome}</expected_outcome>"
            if spec.expected_outcome
            else ""
        )

        context = {
            "spec_name": spec.name,
            "spec_description": spec.description,
            "check_type": spec.check_type,
            "parameters": json.dumps(spec.parameters) if spec.parameters else "None",
            "sql_hints": sql_hints,
            "expected_outcome": expected,
            "schema": schema_text,
        }

        # Render prompt using template
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                SQL_GENERATION_TEMPLATE_NAME, context
            )
        except Exception as e:
            return Result.fail(f"Failed to render validation prompt: {e}")

        # Create tool definition from Pydantic model
        tool = ToolDefinition(
            name="generate_validation_sql",
            description=(
                "Generate a DuckDB SQL query for the validation check. "
                "Analyze the schema to identify relevant columns and tables."
            ),
            input_schema=ValidationSQLOutput.model_json_schema(),
        )

        model = self.provider.get_model_for_tier(feature_config.model_tier)

        # Call LLM with tool use
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": "generate_validation_sql"},
            max_tokens=self.MAX_TOKENS,
            temperature=temperature,
            model=model,
        )

        result = self.provider.converse(request)
        if not result.success or not result.value:
            return Result.fail(result.error or "LLM call failed")

        response = result.value

        # No tool call = degraded generation. There is no rescue: under the
        # lifecycle this is a bind ERROR — the artifact stays ``declared``
        # with the reason. (DAT-439 deleted the JSON-parse-from-text fallback
        # that silently rescued unstructured responses.)
        if not response.tool_calls:
            logger.warning(
                "validation_llm_no_tool_call",
                validation_id=spec.validation_id,
                has_content=bool(response.content),
            )
            return Result.fail(
                "LLM did not use the generate_validation_sql tool — no structured output"
            )

        # Parse tool response using Pydantic model
        tool_call = response.tool_calls[0]
        if tool_call.name != "generate_validation_sql":
            return Result.fail(f"Unexpected tool call: {tool_call.name}")

        try:
            output = ValidationSQLOutput.model_validate(tool_call.input)
        except Exception as e:
            return Result.fail(f"Failed to validate tool response: {e}")

        # can_validate without SQL is a degraded generation, not a skip —
        # labeling it SKIPPED would mislabel the degradation as legitimate
        # inapplicability (DAT-439 sweep). Bind ERROR instead.
        if output.can_validate and not output.sql:
            return Result.fail(
                "LLM declared the validation feasible (can_validate=true) but returned no SQL"
            )

        # Convert to GeneratedSQL
        generated = GeneratedSQL(
            validation_id=spec.validation_id,
            sql_query=output.sql or "",
            explanation=output.explanation,
            columns_used=output.columns_used,
            generated_at=datetime.now(UTC),
            model_used=model,
            is_valid=output.can_validate,
            validation_error=output.skip_reason,
        )

        return Result.ok(generated)

    def _evaluate_result(
        self,
        spec: ValidationSpec,
        result_rows: list[dict[str, Any]],
        row_count: int,
    ) -> tuple[ValidationStatus, str, dict[str, Any]]:
        """Evaluate validation result based on check type.

        PASSED/FAILED is a *judged measurement* of the data. ERROR means the
        evaluation is INCONCLUSIVE: the SQL ran, but the result shape cannot
        be judged (no recognizable columns, zero rows on a summary check, an
        unrecognized check type). An inconclusive evaluation is not a data
        failure — reporting it FAILED would pollute the failure measurements
        ``cross_table_consistency`` scores, so it must never reach FAILED
        (DAT-439; the artifact stays ``grounded`` with the reason).

        Args:
            spec: Validation spec
            result_rows: Query result rows
            row_count: Total row count

        Returns:
            Tuple of (status, message, details) with status PASSED/FAILED/ERROR
        """
        check_type = spec.check_type
        params = spec.parameters
        tolerance = params.get("tolerance", self.DEFAULT_TOLERANCE)

        def measured(passed: bool) -> ValidationStatus:
            return ValidationStatus.PASSED if passed else ValidationStatus.FAILED

        if check_type == "balance":
            # Balance checks compare two values
            if row_count == 0:
                return (
                    ValidationStatus.ERROR,
                    "Balance check inconclusive: query returned no rows",
                    {"check_type": check_type},
                )

            row = result_rows[0]

            # Look for difference column first (preferred: LLM computes the diff)
            if "difference" in row or "diff" in row:
                diff = abs(float(row.get("difference", row.get("diff", 0)) or 0))
                # Promote magnitude into flat details so the scorer can
                # read it directly (it expects details["magnitude"]).
                mag = abs(float(row.get("magnitude") or 0)) or abs(diff) or 1
                return (
                    measured(diff <= tolerance),
                    f"Balance difference: {diff:.2f} (tolerance: {tolerance})",
                    {
                        "check_type": check_type,
                        "difference": diff,
                        "magnitude": mag,
                        "tolerance": tolerance,
                        "row": row,
                    },
                )

            # Look for standard balance column names
            value_cols = [k for k in row.keys() if "total" in k.lower() or "sum" in k.lower()]
            if len(value_cols) >= 2:
                val1 = float(row[value_cols[0]] or 0)
                val2 = float(row[value_cols[1]] or 0)
                diff = abs(val1 - val2)
                return (
                    measured(diff <= tolerance),
                    f"Balance check: {value_cols[0]}={val1:.2f}, {value_cols[1]}={val2:.2f}, diff={diff:.2f}",
                    {
                        "check_type": check_type,
                        "values": row,
                        "difference": diff,
                        "tolerance": tolerance,
                    },
                )

            # No recognizable columns — inconclusive, never FAILED
            return (
                ValidationStatus.ERROR,
                f"Balance check inconclusive: could not identify balance columns in result. "
                f"Columns returned: {list(row.keys())}",
                {"check_type": check_type, "row": row},
            )

        elif check_type == "constraint":
            # Constraint checks return violating rows; an empty result IS the
            # judgement (no violations), unlike the summary checks above.
            if row_count == 0:
                return (
                    ValidationStatus.PASSED,
                    "No constraint violations found",
                    {"check_type": check_type},
                )
            # Extract total_rows from result columns if the LLM included it
            details: dict[str, Any] = {"check_type": check_type, "violation_count": row_count}
            if result_rows:
                for key in ("total_rows", "total_count", "total"):
                    val = result_rows[0].get(key)
                    if val is not None:
                        details["total_rows"] = int(val)
                        break
                # Check for violation_count column (LLM may return a single summary row)
                vc = result_rows[0].get("violation_count")
                if vc is not None and row_count == 1:
                    # Single row with violation_count → summary, not raw violations
                    details["violation_count"] = int(vc)
            return (
                ValidationStatus.FAILED,
                f"Found {details['violation_count']} constraint violations",
                details,
            )

        elif check_type == "comparison":
            # Comparison checks (e.g., Assets = Liabilities + Equity)
            if row_count == 0:
                return (
                    ValidationStatus.ERROR,
                    "Comparison check inconclusive: query returned no rows",
                    {"check_type": check_type},
                )

            row = result_rows[0]
            tolerance = params.get("tolerance", self.DEFAULT_TOLERANCE)

            # Check for an equation_holds or is_valid column
            if "equation_holds" in row:
                passed = bool(row["equation_holds"])
                return (
                    measured(passed),
                    f"Equation check: {'passed' if passed else 'failed'}",
                    {**row, "check_type": check_type},
                )

            if "is_valid" in row:
                passed = bool(row["is_valid"])
                return (
                    measured(passed),
                    f"Comparison check: {'passed' if passed else 'failed'}",
                    {**row, "check_type": check_type},
                )

            # Check for difference column
            if "difference" in row:
                diff = abs(float(row["difference"] or 0))
                return (
                    measured(diff <= tolerance),
                    f"Comparison difference: {diff:.2f}",
                    {"check_type": check_type, "difference": diff},
                )

            # No recognizable columns — inconclusive, never FAILED (the
            # smoke-proven three_way_match shape, DAT-439).
            return (
                ValidationStatus.ERROR,
                f"Comparison check inconclusive: could not identify comparison columns in result. "
                f"Columns returned: {list(row.keys())}",
                {"check_type": check_type, "row": row},
            )

        elif check_type == "aggregate":
            # Aggregate checks return summary values with a rate metric
            if row_count == 0:
                return (
                    ValidationStatus.ERROR,
                    "Aggregate check inconclusive: query returned no rows",
                    {"check_type": check_type},
                )

            row = result_rows[0]
            details = {**row, "check_type": check_type}

            # Check orphan_rate / violation_rate against tolerance
            rate = None
            for key in ("orphan_rate", "violation_rate", "mismatch_rate", "error_rate"):
                val = row.get(key)
                if val is not None:
                    rate = float(val)
                    break

            if rate is not None:
                return (measured(rate <= tolerance), f"Aggregate rate: {rate:.4f}", details)

            # DAT-439 decision: no rate metric stays PASSED — the prompt
            # contract for aggregate checks is "summary values for review"
            # (no rate required); the rate judgement above is opportunistic.
            return (ValidationStatus.PASSED, "Aggregate check completed", details)

        else:
            # Unrecognized check type: the evaluator has no semantics to
            # judge with — inconclusive, never a row_count>0 guess (DAT-439
            # sweep; previously "assume passing if any results").
            return (
                ValidationStatus.ERROR,
                f"Cannot evaluate check_type {check_type!r}: no evaluation semantics defined "
                f"(query returned {row_count} rows)",
                {"check_type": check_type, "row_count": row_count},
            )


__all__ = ["ValidationAgent"]
