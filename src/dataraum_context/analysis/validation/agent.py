"""Validation Agent - LLM-powered SQL generation for validation checks.

This agent generates SQL queries for validation checks by passing the full
schema to the LLM and letting it identify relevant columns.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.validation.config import load_all_validation_specs
from dataraum_context.analysis.validation.models import (
    GeneratedSQL,
    ValidationResult,
    ValidationRunResult,
    ValidationSeverity,
    ValidationSpec,
    ValidationStatus,
)
from dataraum_context.analysis.validation.resolver import (
    format_schema_for_prompt,
    get_table_schema_for_llm,
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
1. Analyze the table schema and semantic annotations to identify relevant columns
2. Use DuckDB SQL syntax
3. The query should return validation results that can be evaluated
4. For balance checks: return the values being compared (e.g., total_debits, total_credits, difference)
5. For constraint checks: return violating rows
6. For comparison checks: return values and a boolean indicator (equation_holds or is_valid)
7. For aggregate checks: return the aggregated values
8. Always use proper quoting for column names with special characters
9. Return a single SQL query (no multiple statements)
10. If the required columns for the validation are not found in the schema, indicate this clearly
"""

SQL_GENERATION_TEMPLATE = """Generate a DuckDB SQL query for this validation check.

## Validation Spec
Name: {spec_name}
Description: {spec_description}
Check Type: {check_type}
Parameters: {parameters}

{sql_hints}

{expected_outcome}

## Table Schema
{schema}

## Instructions
1. Analyze the schema and semantic annotations to identify which columns are relevant for this validation
2. Generate a SQL query that implements this validation check
3. The query should return results that can be evaluated based on the check type:
   - balance: return totals and difference
   - comparison: return values and equation_holds boolean
   - constraint: return violating rows (empty = passed)
   - aggregate: return aggregated values

Return ONLY a JSON object with this structure:
{{
  "sql": "SELECT ...",
  "explanation": "Brief explanation of what this query checks",
  "columns_used": ["col1", "col2"],
  "can_validate": true,
  "skip_reason": null
}}

If the schema doesn't contain the necessary columns for this validation, return:
{{
  "sql": null,
  "explanation": "Why validation cannot be performed",
  "columns_used": [],
  "can_validate": false,
  "skip_reason": "Missing required columns: ..."
}}
"""


class ValidationAgent(LLMFeature):
    """LLM-powered validation agent.

    Generates SQL for validation checks by passing the full schema
    to the LLM for interpretation.
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
        table_id: str,
        validation_ids: list[str] | None = None,
        category: str | None = None,
    ) -> Result[ValidationRunResult]:
        """Run validation checks on a table.

        Args:
            session: Database session
            duckdb_conn: DuckDB connection for executing SQL
            table_id: Table to validate
            validation_ids: Specific validations to run (None = all applicable)
            category: Filter by category (e.g., 'financial')

        Returns:
            Result containing ValidationRunResult
        """
        run_id = str(uuid4())
        started_at = datetime.utcnow()
        results: list[ValidationResult] = []

        # Get table schema
        schema = await get_table_schema_for_llm(session, table_id)
        if "error" in schema:
            return Result.fail(str(schema["error"]))

        table_name = schema["table_name"]

        # Determine which validations to run
        specs = self._get_applicable_specs(validation_ids, category)

        if not specs:
            return Result.ok(
                ValidationRunResult(
                    run_id=run_id,
                    table_id=table_id,
                    table_name=table_name,
                    started_at=started_at,
                    completed_at=datetime.utcnow(),
                    total_checks=0,
                )
            )

        # Run each validation
        for spec in specs:
            result = await self._run_single_validation(
                duckdb_conn=duckdb_conn,
                table_id=table_id,
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
            table_id=table_id,
            table_name=table_name,
            started_at=started_at,
            completed_at=datetime.utcnow(),
            results=results,
            total_checks=len(results),
            passed_checks=passed,
            failed_checks=failed,
            skipped_checks=skipped,
            error_checks=errors,
            overall_status=overall,
            has_critical_failures=has_critical,
        )

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
        table_id: str,
        spec: ValidationSpec,
        schema: dict[str, Any],
    ) -> ValidationResult:
        """Run a single validation check.

        Args:
            duckdb_conn: DuckDB connection
            table_id: Table being validated
            spec: Validation spec to run
            schema: Table schema

        Returns:
            ValidationResult
        """
        table_name = schema["table_name"]

        # Generate SQL via LLM
        sql_result = await self._generate_sql(spec, schema)

        if not sql_result.success or not sql_result.value:
            return ValidationResult(
                validation_id=spec.validation_id,
                spec_name=spec.name,
                status=ValidationStatus.ERROR,
                severity=spec.severity,
                table_id=table_id,
                table_name=table_name,
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
                table_id=table_id,
                table_name=table_name,
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
                table_id=table_id,
                table_name=table_name,
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
                table_id=table_id,
                table_name=table_name,
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
            schema: Table schema

        Returns:
            Result containing GeneratedSQL
        """
        # Format schema for prompt
        schema_text = format_schema_for_prompt(schema)

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
                generated_at=datetime.utcnow(),
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


__all__ = ["ValidationAgent"]
