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
from uuid import uuid4

import duckdb
from sqlalchemy.orm import Session

from dataraum.analysis.validation.config import load_all_validation_specs
from dataraum.analysis.validation.db_models import (
    ValidationResultRecord,
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

    def run_validations(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_ids: list[str],
        validation_ids: list[str] | None = None,
        category: str | None = None,
        persist: bool = True,
        *,
        vertical: str,
    ) -> Result[ValidationRunResult]:
        """Run validation checks across multiple tables.

        Args:
            session: Database session
            duckdb_conn: DuckDB connection for executing SQL
            table_ids: Tables to validate (all schemas passed to LLM)
            validation_ids: Specific validations to run (None = all applicable)
            category: Filter by category (e.g., 'financial')
            persist: Whether to save results to the database (default True)
            vertical: Vertical name (e.g. 'finance')

        Returns:
            Result containing ValidationRunResult
        """
        run_id = str(uuid4())
        started_at = datetime.now(UTC)
        results: list[ValidationResult] = []

        # Get multi-table schema with relationships and row counts
        schema = get_multi_table_schema_for_llm(session, table_ids, duckdb_conn=duckdb_conn)
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
        specs = self._get_applicable_specs(validation_ids, category, vertical)

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
        vertical: str,
    ) -> list[ValidationSpec]:
        """Get validation specs to run.

        Args:
            validation_ids: Specific IDs to run
            category: Category filter
            vertical: Vertical name (e.g. 'finance')

        Returns:
            List of ValidationSpecs
        """
        all_specs = load_all_validation_specs(vertical)

        if validation_ids:
            return [all_specs[vid] for vid in validation_ids if vid in all_specs]

        if category:
            return [s for s in all_specs.values() if s.category == category]

        return list(all_specs.values())

    @staticmethod
    def _scope_table_ids_from_sql(
        sql: str,
        schema: dict[str, Any],
        all_table_ids: list[str],
    ) -> list[str]:
        """Derive which tables a validation SQL actually references.

        Parses typed_* table names from the SQL and maps them back to
        table_ids via the schema. Returns empty list (with warning) if
        no tables can be resolved — never falls back to all_table_ids.
        """
        # Build duckdb_path → id map from schema (SQL references duckdb paths
        # like typed_bank_transactions, not logical names like bank_transactions)
        path_to_id: dict[str, str] = {}
        for t in schema.get("tables", []):
            duckdb_path = t.get("duckdb_path", t["table_name"])
            path_to_id[duckdb_path] = t.get("table_id", "")

        # Find all typed_* table references in SQL
        referenced_names = set(re.findall(r"\btyped_\w+", sql))

        scoped_ids = []
        for name in referenced_names:
            tid = path_to_id.get(name)
            if tid and tid in all_table_ids:
                scoped_ids.append(tid)

        if not scoped_ids and referenced_names:
            logger.warning(
                "validation_table_scope_empty",
                referenced_tables=list(referenced_names),
                available_tables=list(path_to_id.keys()),
            )

        return scoped_ids

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

        # Scope table_ids to tables actually referenced in the SQL
        if generated.sql_query:
            scoped_table_ids = self._scope_table_ids_from_sql(
                generated.sql_query, schema, table_ids
            )
        else:
            scoped_table_ids = []

        # Check if validation can be performed
        if not generated.is_valid:
            return ValidationResult(
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

        # Validate SQL with EXPLAIN before execution
        try:
            duckdb_conn.execute(f"EXPLAIN {generated.sql_query}")
        except Exception as e:
            logger.error("sql_validation_failed", validation_id=spec.validation_id, error=str(e))
            return ValidationResult(
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

        # Execute SQL
        try:
            result_obj = duckdb_conn.execute(generated.sql_query)
            col_names = [desc[0] for desc in result_obj.description]
            raw_rows = result_obj.fetchall()
            result_rows: list[dict[str, Any]] = [
                dict(zip(col_names, row, strict=True)) for row in raw_rows
            ]
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
                table_ids=scoped_table_ids,
                table_name=combined_table_name,
                passed=passed,
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

        # Extract tool call result
        if not response.tool_calls:
            # LLM didn't use the tool - try to parse text as fallback
            logger.warning(
                "validation_llm_no_tool_call",
                validation_id=spec.validation_id,
                has_content=bool(response.content),
            )
            if response.content:
                try:
                    response_data = json.loads(response.content)
                    output = ValidationSQLOutput.model_validate(response_data)
                except Exception as e:
                    logger.warning(
                        "validation_json_fallback_failed",
                        validation_id=spec.validation_id,
                        error=str(e),
                    )
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
                return (False, "No results returned", {"check_type": check_type})

            row = result_rows[0]
            tolerance = params.get("tolerance", self.DEFAULT_TOLERANCE)

            # Look for difference column first (preferred: LLM computes the diff)
            if "difference" in row or "diff" in row:
                diff = abs(float(row.get("difference", row.get("diff", 0)) or 0))
                passed = diff <= tolerance
                return (
                    passed,
                    f"Balance difference: {diff:.2f} (tolerance: {tolerance})",
                    {
                        "check_type": check_type,
                        "difference": diff,
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
                passed = diff <= tolerance
                return (
                    passed,
                    f"Balance check: {value_cols[0]}={val1:.2f}, {value_cols[1]}={val2:.2f}, diff={diff:.2f}",
                    {
                        "check_type": check_type,
                        "values": row,
                        "difference": diff,
                        "tolerance": tolerance,
                    },
                )

            # No recognizable columns — fail explicitly rather than silently pass
            return (
                False,
                f"Balance check inconclusive: could not identify balance columns in result. "
                f"Columns returned: {list(row.keys())}",
                {"check_type": check_type, "row": row},
            )

        elif check_type == "constraint":
            # Constraint checks return violating rows.
            if row_count == 0:
                return (True, "No constraint violations found", {"check_type": check_type})
            # Extract total_rows from result columns if the LLM included it
            details: dict[str, Any] = {"check_type": check_type, "violation_count": row_count}
            if result_rows:
                for key in ("total_rows", "total_count", "total"):
                    val = result_rows[0].get(key)
                    if val is not None:
                        details["total_rows"] = int(val)
                        break
                # Also check for violation_count column (LLM may return summary rows)
                vc = result_rows[0].get("violation_count")
                if vc is not None and row_count <= 5:
                    # Few rows with a violation_count column → summary, not raw violations
                    details["violation_count"] = sum(
                        int(r.get("violation_count", 0)) for r in result_rows
                    )
            return (
                False,
                f"Found {details['violation_count']} constraint violations",
                details,
            )

        elif check_type == "comparison":
            # Comparison checks (e.g., Assets = Liabilities + Equity)
            if row_count == 0:
                return (False, "No results returned", {"check_type": check_type})

            row = result_rows[0]
            tolerance = params.get("tolerance", self.DEFAULT_TOLERANCE)

            # Check for an equation_holds or is_valid column
            if "equation_holds" in row:
                passed = bool(row["equation_holds"])
                return (
                    passed,
                    f"Equation check: {'passed' if passed else 'failed'}",
                    {**row, "check_type": check_type},
                )

            if "is_valid" in row:
                passed = bool(row["is_valid"])
                return (
                    passed,
                    f"Comparison check: {'passed' if passed else 'failed'}",
                    {**row, "check_type": check_type},
                )

            # Check for difference column
            if "difference" in row:
                diff = abs(float(row["difference"] or 0))
                passed = diff <= tolerance
                return (
                    passed,
                    f"Comparison difference: {diff:.2f}",
                    {"check_type": check_type, "difference": diff},
                )

            return (
                False,
                f"Comparison check inconclusive: could not identify comparison columns in result. "
                f"Columns returned: {list(row.keys())}",
                {"check_type": check_type, "row": row},
            )

        elif check_type == "aggregate":
            # Aggregate checks return summary values
            if row_count == 0:
                return (False, "No results returned", {"check_type": check_type})

            row = result_rows[0]
            return (True, "Aggregate check completed", {**row, "check_type": check_type})

        else:
            # Custom check - assume passing if any results
            return (
                row_count > 0,
                f"Custom check returned {row_count} rows",
                {"check_type": check_type, "row_count": row_count},
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
        # Create individual result records with client-side IDs
        for result in run_result.results:
            # Serialize details to ensure JSON compatibility
            result_data = result.model_dump(mode="json")
            result_record = ValidationResultRecord(
                result_id=str(uuid4()),
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

        logger.debug(
            "validation_results_persisted",
            run_id=run_result.run_id,
            count=len(run_result.results),
        )


__all__ = ["ValidationAgent"]
