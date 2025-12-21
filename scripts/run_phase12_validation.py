#!/usr/bin/env python3
"""Phase 12: Generic Validation with LLM Agent.

This script runs LLM-powered validation checks on typed tables,
using semantic annotations to identify relevant columns.

Prerequisites:
    - Phase 5 must be completed (semantic analysis with annotations)
    - ANTHROPIC_API_KEY must be set in environment or .env file

Usage:
    uv run python scripts/run_phase12_validation.py
    uv run python scripts/run_phase12_validation.py --category financial
    uv run python scripts/run_phase12_validation.py --table-id <uuid>
"""

import argparse
import asyncio
import json
import os
import sys

from dotenv import load_dotenv
from infra import (
    cleanup_connections,
    get_duckdb_conn,
    get_test_session,
    get_typed_table_ids,
    print_database_summary,
)
from sqlalchemy import func, select

# Load environment variables from .env
load_dotenv()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run LLM-powered validation checks on typed tables."
    )
    parser.add_argument(
        "--table-id",
        type=str,
        help="Specific table ID to validate (default: all typed tables)",
    )
    parser.add_argument(
        "--category",
        type=str,
        default="financial",
        help="Validation category to run (default: financial)",
    )
    parser.add_argument(
        "--validation-id",
        type=str,
        nargs="+",
        help="Specific validation IDs to run",
    )
    parser.add_argument(
        "--list-specs",
        action="store_true",
        help="List available validation specs and exit",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/validation_results.json",
        help="Output file for results (default: data/validation_results.json)",
    )
    return parser.parse_args()


async def list_validation_specs():
    """List all available validation specs."""
    from dataraum_context.analysis.validation import load_all_validation_specs

    specs = load_all_validation_specs()

    print("\n" + "=" * 70)
    print("AVAILABLE VALIDATION SPECS")
    print("=" * 70)

    for spec_id, spec in sorted(specs.items()):
        print(f"\n{spec.name} ({spec_id})")
        print(f"  Category: {spec.category}")
        print(f"  Severity: {spec.severity.value}")
        print(f"  Type: {spec.check_type}")
        print(f"  Tags: {', '.join(spec.tags)}")
        if spec.description:
            desc = spec.description.strip()[:100]
            if len(spec.description.strip()) > 100:
                desc += "..."
            print(f"  Description: {desc}")

    print(f"\nTotal: {len(specs)} validation specs available")


async def main() -> int:
    """Run Phase 12: Generic Validation."""
    args = parse_args()

    # Handle --list-specs
    if args.list_specs:
        await list_validation_specs()
        return 0

    print("=" * 70)
    print("Phase 12: Generic Validation (LLM Agent)")
    print("=" * 70)

    # Check for API key
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("\nERROR: ANTHROPIC_API_KEY not found!")
        print("Please set ANTHROPIC_API_KEY in your environment or .env file.")
        return 1

    print(f"\nUsing Anthropic API key: {api_key[:8]}...{api_key[-4:]}")

    # Get connections
    session_factory = await get_test_session()
    duckdb_conn = get_duckdb_conn()

    async with session_factory() as session:
        from dataraum_context.analysis.semantic.db_models import SemanticAnnotation

        # Check prerequisites
        print("\n1. Checking prerequisites...")
        print("-" * 50)

        # Check for semantic annotations
        annotation_count = (
            await session.execute(select(func.count(SemanticAnnotation.annotation_id)))
        ).scalar() or 0

        if annotation_count == 0:
            print("   WARNING: No semantic annotations found!")
            print("   The LLM will have less context to work with.")
            print("   Consider running run_phase5_semantic.py first.")
        else:
            print(f"   Found {annotation_count} semantic annotations")

        # Get table IDs to validate
        if args.table_id:
            table_ids = [args.table_id]
            print(f"   Validating specific table: {args.table_id}")
        else:
            table_ids = await get_typed_table_ids(session)
            print(f"   Found {len(table_ids)} typed tables to validate")

        if not table_ids:
            print("   ERROR: No tables to validate!")
            await cleanup_connections()
            return 1

        # Setup LLM provider and validation agent
        print("\n2. Setting up LLM provider...")
        print("-" * 50)

        from dataraum_context.analysis.validation import ValidationAgent
        from dataraum_context.llm.cache import LLMCache
        from dataraum_context.llm.config import load_llm_config
        from dataraum_context.llm.prompts import PromptRenderer
        from dataraum_context.llm.providers import create_provider

        try:
            llm_config = load_llm_config()
            provider_config = llm_config.providers["anthropic"]
            provider = create_provider(
                "anthropic",
                {
                    "api_key_env": provider_config.api_key_env,
                    "default_model": provider_config.default_model,
                    "models": provider_config.models,
                    "base_url_env": provider_config.base_url_env,
                },
            )
            print(f"   Using model: {provider_config.default_model}")

            # Create validation agent
            prompt_renderer = PromptRenderer()
            cache = LLMCache()
            agent = ValidationAgent(
                config=llm_config,
                provider=provider,
                prompt_renderer=prompt_renderer,
                cache=cache,
            )

        except Exception as e:
            print(f"   ERROR creating LLM provider: {e}")
            await cleanup_connections()
            return 1

        # Run validations (multi-table: all tables passed together)
        print("\n3. Running validations...")
        print("-" * 50)

        print(f"\n   Validating {len(table_ids)} tables together (cross-table JOINs enabled)...")

        result = await agent.run_validations(
            session=session,
            duckdb_conn=duckdb_conn,
            table_ids=table_ids,
            validation_ids=args.validation_id,
            category=args.category if not args.validation_id else None,
        )

        if not result.success:
            print(f"   ERROR: {result.error}")
            await cleanup_connections()
            return 1

        run_result = result.unwrap()
        all_results = [run_result.model_dump(mode="json")]

        total_passed = run_result.passed_checks
        total_failed = run_result.failed_checks
        total_skipped = run_result.skipped_checks
        total_errors = run_result.error_checks

        print(f"   Tables: {run_result.table_name}")
        print(f"   Checks: {run_result.total_checks}")
        print(
            f"   Results: {run_result.passed_checks} passed, {run_result.failed_checks} failed, {run_result.skipped_checks} skipped"
        )

        # Show individual check results
        for check_result in run_result.results:
            status_icon = {
                "passed": "✓",
                "failed": "✗",
                "skipped": "○",
                "error": "!",
            }.get(check_result.status.value, "?")
            print(f"      {status_icon} {check_result.spec_name}: {check_result.message[:60]}")

        # Print summary
        print("\n" + "=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)

        print(f"\nTables validated: {len(table_ids)}")
        print(f"Category: {args.category or 'all'}")
        print("\nResults:")
        print(f"  Passed:  {total_passed}")
        print(f"  Failed:  {total_failed}")
        print(f"  Skipped: {total_skipped}")
        print(f"  Errors:  {total_errors}")

        if total_failed > 0:
            print("\n" + "-" * 50)
            print("FAILED CHECKS:")
            for run in all_results:
                for check in run.get("results", []):
                    if check.get("status") == "failed":
                        print(f"\n  {check['spec_name']} on {run['table_name']}")
                        print(f"    Severity: {check['severity']}")
                        print(f"    Message: {check['message']}")
                        if check.get("details"):
                            print(f"    Details: {json.dumps(check['details'], indent=6)}")

        # Save results
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(
                {
                    "summary": {
                        "tables_validated": len(table_ids),
                        "category": args.category,
                        "total_passed": total_passed,
                        "total_failed": total_failed,
                        "total_skipped": total_skipped,
                        "total_errors": total_errors,
                    },
                    "results": all_results,
                },
                f,
                indent=2,
                default=str,
            )
        print(f"\nFull results saved to: {args.output}")

        # Database summary
        await print_database_summary(session, duckdb_conn)

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 12 COMPLETE")
    print("=" * 70)

    # Return non-zero if there were failures
    return 1 if total_failed > 0 else 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
