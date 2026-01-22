"""Validation Agent - LLM-powered SQL generation for validation checks.

This agent generates SQL queries for validation checks by passing the full
schema (potentially multiple tables) to the LLM and letting it identify
relevant columns and generate cross-table JOINs when needed.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import duckdb
from sqlalchemy.orm import Session

from dataraum.analysis.validation.config import load_all_validation_specs
from dataraum.analysis.validation.db_models import (
    ValidationResultRecord,
    ValidationRunRecord,
)
from dataraum.analysis.validation.models import (
    GeneratedSQL,
    ValidationResult,
    ValidationRunResult,
    ValidationSeverity,
    ValidationSpec,
    ValidationSQLOutput,
    ValidationStatus,
)
from dataraum.analysis.validation.resolver import (
    format_multi_table_schema_for_prompt,
    get_multi_table_schema_for_llm,
)
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
    ToolDefinition,
)

if TYPE_CHECKING:
    from dataraum.llm.cache import LLMCache
    from dataraum.llm.config import LLMConfig
    from dataraum.llm.prompts import PromptRenderer
    from dataraum.llm.providers.base import LLMProvider

logger = get_logger(__name__)


# Prompt template name for SQL generation
SQL_GENERATION_TEMPLATE_NAME = "validation_sql"


class ValidationAgent(LLMFeature):
    """LLM-powered validation agent.

    Generates SQL for validation checks by passing multiple table schemas
    to the LLM for interpretation. The LLM can generate cross-table JOINs
    when validations require data from multiple tables.
    """

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
        cache: LLMCache,
    ) -> None:
        """Initialize validation agent.

        Args:
            config: LLM configuration
            provider: LLM provider instance
            prompt_renderer: Prompt template renderer
            cache: Response cache
        """
        super().__init__(config, provider, prompt_renderer, cache)

    def run_validations(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_ids: list[str],
        validation_ids: list[str] | None = None,
        category: str | None = None,
        persist: bool = True,
    ) -> Result[ValidationRunResult]:
        """Run validation checks across multiple tables.

        Args:
            session: Database session
            duckdb_conn: DuckDB connection for executing SQL
            table_ids: Tables to validate (all schemas passed to LLM)
            validation_ids: Specific validations to run (None = all applicable)
            category: Filter by category (e.g., 'financial')
            persist: Whether to save results to the database (default True)

        Returns:
            Result containing ValidationRunResult
        """
        run_id = str(uuid4())
        started_at = datetime.now(UTC)
        results: list[ValidationResult] = []

        # Get multi-table schema with relationships
        schema = get_multi_table_schema_for_llm(session, table_ids)
        if "error" in schema:
            return Result.fail(str(schema["error"]))

        # Validate context - fail early if missing critical information
        context_issues = self._validate_context(schema)
        if context_issues:
            return Result.fail(f"Insufficient context for validation: {'; '.join(context_issues)}")

        # Get table names for result
        table_names = [t["table_name"] for t in schema.get("tables", [])]
        combined_table_name = ", ".join(table_names)

        # Determine which validations to run
        specs = self._get_applicable_specs(validation_ids, category)

        if not specs:
            return Result.ok(
                ValidationRunResult(
                    run_id=run_id,
                    table_ids=table_ids,
                    table_name=combined_table_name,
                    started_at=started_at,
                    completed_at=datetime.now(UTC),
                    total_checks=0,
                )
            )

        # Run each validation
        for spec in specs:
            result = self._run_single_validation(
                duckdb_conn=duckdb_conn,
                table_ids=table_ids,
                spec=spec,
                schema=schema,
            )
            results.append(result)

        # Summarize results
        passed = sum(1 for r in results if r.status == ValidationStatus.PASSED)
        failed = sum(1 for r in results if r.status == ValidationStatus.FAILED)
        skipped = sum(1 for r in results if r.status == ValidationStatus.SKIPPED)
        errors = sum(1 for r in results if r.status == ValidationStatus.ERROR)

        has_critical = any(
            r.status == ValidationStatus.FAILED and r.severity == ValidationSeverity.CRITICAL
            for r in results
        )

        overall = ValidationStatus.PASSED
        if failed > 0 or errors > 0:
            overall = ValidationStatus.FAILED

        run_result = ValidationRunResult(
            run_id=run_id,
            table_ids=table_ids,
            table_name=combined_table_name,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            results=results,
            total_checks=len(results),
            passed_checks=passed,
            failed_checks=failed,
            skipped_checks=skipped,
            error_checks=errors,
            overall_status=overall,
            has_critical_failures=has_critical,
        )

        # Persist results to database
        if persist:
            self._persist_results(session, run_result)

        return Result.ok(run_result)

    def _validate_context(self, schema: dict[str, Any]) -> list[str]:
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

        # Check for semantic annotations (warning only, not blocking)
        columns_with_semantic = sum(
            1 for t in tables for c in t.get("columns", []) if c.get("semantic")
        )
        if columns_with_semantic == 0:
            logger.warning(
                "No semantic annotations found. "
                "Validation may be less accurate without column semantics."
            )

        return issues

    def _get_applicable_specs(
        self,
        validation_ids: list[str] | None,
        category: str | None,
    ) -> list[ValidationSpec]:
        """Get validation specs to run.

        Args:
            validation_ids: Specific IDs to run
            category: Category filter

        Returns:
            List of ValidationSpecs
        """
        all_specs = load_all_validation_specs()

        if validation_ids:
            return [all_specs[vid] for vid in validation_ids if vid in all_specs]

        if category:
            return [s for s in all_specs.values() if s.category == category]

        return list(all_specs.values())

    def _run_single_validation(
        self,
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_ids: list[str],
        spec: ValidationSpec,
        schema: dict[str, Any],
    ) -> ValidationResult:
        """Run a single validation check.

        Args:
            duckdb_conn: DuckDB connection
            table_ids: Table IDs being validated
            spec: Validation spec to run
            schema: Multi-table schema

        Returns:
            ValidationResult
        """
        table_names = [t["table_name"] for t in schema.get("tables", [])]
        combined_table_name = ", ".join(table_names)

        # Generate SQL via LLM
        sql_result = self._generate_sql(spec, schema)

        if not sql_result.success or not sql_result.value:
            return ValidationResult(
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

        # Check if validation can be performed
        if not generated.is_valid:
            return ValidationResult(
                validation_id=spec.validation_id,
                spec_name=spec.name,
                status=ValidationStatus.SKIPPED,
                severity=spec.severity,
                table_ids=table_ids,
                table_name=combined_table_name,
                passed=False,
                message=generated.validation_error or "Validation cannot be performed",
                columns_used=generated.columns_used,
            )

        # Execute SQL
        try:
            result_df = duckdb_conn.execute(generated.sql_query).df()
            result_rows: list[dict[str, Any]] = result_df.to_dict(orient="records")  # type: ignore[assignment]
            row_count = len(result_rows)

            # Evaluate results based on check type
            passed, message, details = self._evaluate_result(
                spec=spec,
                result_rows=result_rows,
                row_count=row_count,
            )

            return ValidationResult(
                validation_id=spec.validation_id,
                spec_name=spec.name,
                status=ValidationStatus.PASSED if passed else ValidationStatus.FAILED,
                severity=spec.severity,
                table_ids=table_ids,
                table_name=combined_table_name,
                passed=passed,
                message=message,
                details=details,
                sql_used=generated.sql_query,
                columns_used=generated.columns_used,
                result_rows=result_rows[:10],  # Limit stored rows
                row_count=row_count,
            )

        except Exception as e:
            logger.error(f"SQL execution failed for {spec.validation_id}: {e}")
            return ValidationResult(
                validation_id=spec.validation_id,
                spec_name=spec.name,
                status=ValidationStatus.ERROR,
                severity=spec.severity,
                table_ids=table_ids,
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
            max_tokens=2000,
            temperature=temperature,
            model=model,
        )

        result = self.provider.converse(request)
        if not result.success or not result.value:
            return Result.fail(result.error or "LLM call failed")

        response = result.value

        # Extract tool call result
        if not response.tool_calls:
            # LLM didn't use the tool - try to parse text as fallback
            if response.content:
                try:
                    response_data = json.loads(response.content)
                    output = ValidationSQLOutput.model_validate(response_data)
                except Exception:
                    return Result.fail(f"LLM did not use tool. Response: {response.content[:200]}")
            else:
                return Result.fail("LLM did not use the generate_validation_sql tool")
        else:
            # Parse tool response using Pydantic model
            tool_call = response.tool_calls[0]
            if tool_call.name != "generate_validation_sql":
                return Result.fail(f"Unexpected tool call: {tool_call.name}")

            try:
                output = ValidationSQLOutput.model_validate(tool_call.input)
            except Exception as e:
                return Result.fail(f"Failed to validate tool response: {e}")

        # Convert to GeneratedSQL
        generated = GeneratedSQL(
            validation_id=spec.validation_id,
            sql_query=output.sql or "",
            explanation=output.explanation,
            columns_used=output.columns_used,
            generated_at=datetime.now(UTC),
            model_used=model,
            is_valid=output.can_validate and output.sql is not None,
            validation_error=output.skip_reason,
        )

        return Result.ok(generated)

    def _evaluate_result(
        self,
        spec: ValidationSpec,
        result_rows: list[dict[str, Any]],
        row_count: int,
    ) -> tuple[bool, str, dict[str, Any]]:
        """Evaluate validation result based on check type.

        Args:
            spec: Validation spec
            result_rows: Query result rows
            row_count: Total row count

        Returns:
            Tuple of (passed, message, details)
        """
        check_type = spec.check_type
        params = spec.parameters

        if check_type == "balance":
            # Balance checks compare two values
            if row_count == 0:
                return (False, "No results returned", {})

            row = result_rows[0]
            tolerance = params.get("tolerance", 0.01)

            # Find the columns to compare
            value_cols = [k for k in row.keys() if "total" in k.lower() or "sum" in k.lower()]
            if len(value_cols) >= 2:
                val1 = float(row[value_cols[0]] or 0)
                val2 = float(row[value_cols[1]] or 0)
                diff = abs(val1 - val2)
                passed = diff <= tolerance
                return (
                    passed,
                    f"Balance check: {value_cols[0]}={val1:.2f}, {value_cols[1]}={val2:.2f}, diff={diff:.2f}",
                    {"values": row, "difference": diff, "tolerance": tolerance},
                )

            # Fall back to checking if difference column is within tolerance
            if "difference" in row or "diff" in row:
                diff = abs(float(row.get("difference", row.get("diff", 0)) or 0))
                passed = diff <= tolerance
                return (
                    passed,
                    f"Balance difference: {diff:.2f} (tolerance: {tolerance})",
                    {"difference": diff, "tolerance": tolerance},
                )

            return (True, "Balance check passed", {"row": row})

        elif check_type == "constraint":
            # Constraint checks return violating rows
            if row_count == 0:
                return (True, "No constraint violations found", {})
            return (
                False,
                f"Found {row_count} constraint violations",
                {"violation_count": row_count},
            )

        elif check_type == "comparison":
            # Comparison checks (e.g., Assets = Liabilities + Equity)
            if row_count == 0:
                return (False, "No results returned", {})

            row = result_rows[0]
            tolerance = params.get("tolerance", 0.01)

            # Check for an equation_holds or is_valid column
            if "equation_holds" in row:
                passed = bool(row["equation_holds"])
                return (passed, f"Equation check: {'passed' if passed else 'failed'}", row)

            if "is_valid" in row:
                passed = bool(row["is_valid"])
                return (passed, f"Comparison check: {'passed' if passed else 'failed'}", row)

            # Check for difference column
            if "difference" in row:
                diff = abs(float(row["difference"] or 0))
                passed = diff <= tolerance
                return (
                    passed,
                    f"Comparison difference: {diff:.2f}",
                    {"difference": diff},
                )

            return (True, "Comparison check completed", row)

        elif check_type == "aggregate":
            # Aggregate checks return summary values
            if row_count == 0:
                return (False, "No results returned", {})

            row = result_rows[0]
            return (True, "Aggregate check completed", row)

        else:
            # Custom check - assume passing if any results
            return (
                row_count > 0,
                f"Custom check returned {row_count} rows",
                {"row_count": row_count},
            )

    def _persist_results(
        self,
        session: Session,
        run_result: ValidationRunResult,
    ) -> None:
        """Persist validation results to the database.

        Args:
            session: Database session
            run_result: Validation run result to persist
        """
        # Create run record
        run_record = ValidationRunRecord(
            run_id=run_result.run_id,
            table_ids=run_result.table_ids,
            table_name=run_result.table_name,
            started_at=run_result.started_at,
            completed_at=run_result.completed_at,
            total_checks=run_result.total_checks,
            passed_checks=run_result.passed_checks,
            failed_checks=run_result.failed_checks,
            skipped_checks=run_result.skipped_checks,
            error_checks=run_result.error_checks,
            overall_status=run_result.overall_status.value,
            has_critical_failures=run_result.has_critical_failures,
            results=[r.model_dump(mode="json") for r in run_result.results],
        )
        session.add(run_record)

        # Create individual result records
        for result in run_result.results:
            # Serialize details to ensure JSON compatibility
            result_data = result.model_dump(mode="json")
            result_record = ValidationResultRecord(
                run_id=run_result.run_id,
                validation_id=result.validation_id,
                table_ids=result.table_ids,
                status=result.status.value,
                severity=result.severity.value,
                passed=result.passed,
                message=result.message,
                executed_at=result.executed_at,
                sql_used=result.sql_used,
                details=result_data.get("details"),
            )
            session.add(result_record)

        logger.info(
            f"Persisted validation run {run_result.run_id} with {len(run_result.results)} results"
        )


__all__ = ["ValidationAgent"]
