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

from dataraum.analysis.validation.evaluate import evaluate_result
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
)
from dataraum.llm.structured_output import parse_structured_output

logger = get_logger(__name__)


# Prompt template name for SQL generation
SQL_GENERATION_TEMPLATE_NAME = "validation_sql"


class ValidationAgent(LLMFeature):
    """LLM-powered validation agent.

    Generates SQL for validation checks by passing multiple table schemas
    to the LLM for interpretation. The LLM can generate cross-table JOINs
    when validations require data from multiple tables.
    """

    MAX_STORED_ROWS = 10

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
        conventions: str = "",
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
        sql_result = self._generate_sql(spec, schema, conventions=conventions)

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
            status, message, details = evaluate_result(spec=spec, result_rows=result_rows)

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
        conventions: str = "",
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
            # DAT-645: the vertical's conventions, piped verbatim (engine never
            # interprets them) — the same source of truth extraction uses.
            "conventions": conventions,
        }

        # Render prompt using template
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                SQL_GENERATION_TEMPLATE_NAME, context
            )
        except Exception as e:
            return Result.fail(f"Failed to render validation prompt: {e}")

        model = self.provider.get_model_for_tier(feature_config.model_tier)

        # Call LLM — structured output (DAT-807): constrained decoding against
        # ValidationSQLOutput's schema; the answer is JSON message content. This
        # call site was already on a constrained grammar (a strict forced tool),
        # so the mechanism swap is the closest to a no-op of the nine.
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            output_schema=ValidationSQLOutput.model_json_schema(),
            label="validation_sql",
            effort=feature_config.effort,
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        # converse raises a typed ProviderError on an API failure (DAT-503) —
        # retryability rides the exception to the worker's durable boundary, so
        # we don't re-wrap it. A returned Result is always a success.
        response = self.provider.converse(request).unwrap()

        # A payload that does not parse = degraded generation. There is no
        # rescue: under the lifecycle this is a bind ERROR — the artifact stays
        # ``declared`` with the reason. (DAT-439 deleted the JSON-parse-from-text
        # fallback that silently rescued unstructured responses.)
        parsed = parse_structured_output(response, ValidationSQLOutput, label="validation_sql")
        if not parsed.success:
            return Result.fail(parsed.error or "validation_sql failed")
        output = parsed.unwrap()

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
            columns_used=output.columns_used,
            generated_at=datetime.now(UTC),
            model_used=model,
            is_valid=output.can_validate,
            # "" is the DAT-807 not-applicable sentinel; the field is optional
            # downstream, so normalize it back rather than storing an empty string.
            validation_error=output.skip_reason or None,
        )

        return Result.ok(generated)


__all__ = ["ValidationAgent"]
