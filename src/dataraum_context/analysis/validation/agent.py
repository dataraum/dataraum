"""Validation Agent - LLM-powered SQL generation for validation checks.

This agent generates SQL queries for validation checks by passing the full
schema (potentially multiple tables) to the LLM and letting it identify
relevant columns and generate cross-table JOINs when needed.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.validation.config import load_all_validation_specs
from dataraum_context.analysis.validation.db_models import (
    ValidationResultRecord,
    ValidationRunRecord,
)
from dataraum_context.analysis.validation.models import (
    GeneratedSQL,
    ValidationResult,
    ValidationRunResult,
    ValidationSeverity,
    ValidationSpec,
    ValidationStatus,
)
from dataraum_context.analysis.validation.resolver import (
    format_multi_table_schema_for_prompt,
    get_multi_table_schema_for_llm,
)
from dataraum_context.core.models.base import Result
from dataraum_context.llm.features._base import LLMFeature

if TYPE_CHECKING:
    from dataraum_context.llm.cache import LLMCache
    from dataraum_context.llm.config import LLMConfig
    from dataraum_context.llm.prompts import PromptRenderer
    from dataraum_context.llm.providers.base import LLMProvider

logger = logging.getLogger(__name__)


# System prompt for SQL generation
SQL_GENERATION_SYSTEM = """You are an expert SQL developer specializing in data validation.
Your task is to generate DuckDB SQL queries that implement validation checks.

Rules:
1. Analyze all available table schemas and semantic annotations to identify relevant columns
2. Use DuckDB SQL syntax
3. You can JOIN multiple tables when needed for validation (use the detected relationships as hints)
4. The query should return validation results that can be evaluated
5. For balance checks: return the values being compared (e.g., total_debits, total_credits, difference)
6. For constraint checks: return violating rows
7. For comparison checks: return values and a boolean indicator (equation_holds or is_valid)
8. For aggregate checks: return the aggregated values
9. Always use proper quoting for column names with special characters
10. Return a single SQL query (no multiple statements)
11. If the required columns for the validation are not found across any table, indicate this clearly
"""

SQL_GENERATION_TEMPLATE = """Generate a DuckDB SQL query for this validation check.

## Validation Spec
Name: {spec_name}
Description: {spec_description}
Check Type: {check_type}
Parameters: {parameters}

{sql_hints}

{expected_outcome}

## Dataset Schema
{schema}

## Instructions
1. Analyze ALL tables and their semantic annotations to identify which columns are relevant
2. Use the detected relationships to JOIN tables when needed
3. Generate a SQL query that implements this validation check
4. The query should return results that can be evaluated based on the check type:
   - balance: return totals and difference
   - comparison: return values and equation_holds boolean
   - constraint: return violating rows (empty = passed)
   - aggregate: return aggregated values

Return ONLY a JSON object with this structure:
{{
  "sql": "SELECT ... FROM ... JOIN ...",
  "explanation": "Brief explanation of what this query checks and which tables/columns are used",
  "columns_used": ["table1.col1", "table2.col2"],
  "tables_used": ["table1", "table2"],
  "can_validate": true,
  "skip_reason": null
}}

If the schema doesn't contain the necessary columns for this validation, return:
{{
  "sql": null,
  "explanation": "Why validation cannot be performed",
  "columns_used": [],
  "tables_used": [],
  "can_validate": false,
  "skip_reason": "Missing required columns: ..."
}}
"""


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

    async def run_validations(
        self,
        session: AsyncSession,
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
        schema = await get_multi_table_schema_for_llm(session, table_ids)
        if "error" in schema:
            return Result.fail(str(schema["error"]))

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
            result = await self._run_single_validation(
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
            await self._persist_results(session, run_result)

        return Result.ok(run_result)

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

    async def _run_single_validation(
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
        sql_result = await self._generate_sql(spec, schema)

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

    async def _generate_sql(
        self,
        spec: ValidationSpec,
        schema: dict[str, Any],
    ) -> Result[GeneratedSQL]:
        """Generate SQL via LLM.

        Args:
            spec: Validation spec
            schema: Multi-table schema with relationships

        Returns:
            Result containing GeneratedSQL
        """
        # Format schema for prompt
        schema_text = format_multi_table_schema_for_prompt(schema)

        # Build prompt
        sql_hints = f"SQL Hints: {spec.sql_hints}" if spec.sql_hints else ""
        expected = f"Expected Outcome: {spec.expected_outcome}" if spec.expected_outcome else ""

        prompt = SQL_GENERATION_TEMPLATE.format(
            spec_name=spec.name,
            spec_description=spec.description,
            check_type=spec.check_type,
            parameters=json.dumps(spec.parameters) if spec.parameters else "None",
            sql_hints=sql_hints,
            expected_outcome=expected,
            schema=schema_text,
        )

        # Get feature config
        feature_config = self.config.features.validation
        if not feature_config or not feature_config.enabled:
            return Result.fail("Validation feature is disabled in config")

        # Call LLM
        from dataraum_context.llm.providers.base import LLMRequest

        model = self.provider.get_model_for_tier(feature_config.model_tier)

        request = LLMRequest(
            prompt=prompt,
            system=SQL_GENERATION_SYSTEM,
            max_tokens=2000,
            temperature=0.0,  # Deterministic for SQL
            response_format="json",
        )

        result = await self.provider.complete(request)
        if not result.success or not result.value:
            return Result.fail(result.error or "LLM call failed")

        # Parse response
        try:
            response_data = json.loads(result.value.content)
            sql = response_data.get("sql")
            explanation = response_data.get("explanation", "")
            columns_used = response_data.get("columns_used", [])
            can_validate = response_data.get("can_validate", True)
            skip_reason = response_data.get("skip_reason")

            generated = GeneratedSQL(
                validation_id=spec.validation_id,
                sql_query=sql or "",
                explanation=explanation,
                columns_used=columns_used,
                generated_at=datetime.now(UTC),
                model_used=model,
                is_valid=can_validate and sql is not None,
                validation_error=skip_reason,
            )

            return Result.ok(generated)

        except json.JSONDecodeError as e:
            return Result.fail(f"Failed to parse LLM response: {e}")

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

    async def _persist_results(
        self,
        session: AsyncSession,
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
