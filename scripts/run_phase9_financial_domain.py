#!/usr/bin/env python3
"""Phase 9: Financial Domain Quality Analysis.

This script runs financial domain-specific quality checks on typed tables:
- Double-entry balance validation (debits = credits)
- Trial balance checks (Assets = Liabilities + Equity)
- Sign convention validation
- Fiscal period integrity

Prerequisites:
    - Phase 2 must be completed (typed tables exist)

Usage:
    uv run python scripts/run_phase9_financial_domain.py
"""

import asyncio
import sys

from infra import (
    cleanup_connections,
    get_duckdb_conn,
    get_test_session,
    print_database_summary,
    print_phase_status,
)
from sqlalchemy import func, select


async def main() -> int:
    """Run Phase 9: Financial Domain Quality Analysis."""
    print("=" * 70)
    print("Phase 9: Financial Domain Quality Analysis")
    print("=" * 70)

    # Get connections
    session_factory = await get_test_session()
    duckdb_conn = get_duckdb_conn()

    async with session_factory() as session:
        from dataraum_context.domains.financial import (
            FinancialDomainAnalyzer,
            analyze_financial_quality,
        )
        from dataraum_context.domains.financial.db_models import FinancialQualityMetrics
        from dataraum_context.storage import Table

        # Check prerequisites - need typed tables
        print("\n1. Checking prerequisites...")
        print("-" * 50)

        typed_tables_stmt = select(Table).where(Table.layer == "typed")
        typed_tables = (await session.execute(typed_tables_stmt)).scalars().all()

        if not typed_tables:
            print("   ERROR: No typed tables found!")
            print("   Please run run_phase2_typing.py first.")
            await cleanup_connections()
            return 1

        print(f"   Found {len(typed_tables)} typed tables")

        # Check what already has financial quality metrics
        assessed_stmt = select(FinancialQualityMetrics.table_id).distinct()
        assessed_table_ids = set((await session.execute(assessed_stmt)).scalars().all())

        unassessed_tables = [t for t in typed_tables if t.table_id not in assessed_table_ids]

        if not unassessed_tables:
            print("\n   All tables already assessed for financial quality!")
            for tt in typed_tables:
                print_phase_status(f"financial_{tt.table_name}", True)
            await print_database_summary(session, duckdb_conn)
            await cleanup_connections()
            return 0

        # Show what needs assessment
        print("\n   Financial quality status:")
        for tt in typed_tables:
            is_assessed = tt.table_id in assessed_table_ids
            print_phase_status(f"financial_{tt.table_name}", is_assessed)

        # Run financial quality analysis
        print("\n2. Running financial domain analysis...")
        print("-" * 50)

        analyzer = FinancialDomainAnalyzer()
        print(f"   Using analyzer: {analyzer.domain_name}")

        assessed_count = 0
        issues_found = 0

        for typed_table in unassessed_tables:
            print(f"\n   Processing: {typed_table.table_name}...")

            result = await analyze_financial_quality(typed_table.table_id, duckdb_conn, session)

            if not result.success:
                print(f"      ERROR: {result.error}")
                continue

            fq = result.unwrap()
            assessed_count += 1

            # Print double-entry check
            if fq.double_entry_balanced:
                print("      Double-entry: BALANCED")
            else:
                print(f"      Double-entry: IMBALANCED (diff: {fq.balance_difference:,.2f})")
                issues_found += 1

            # Print double-entry details if available
            if fq.double_entry_details:
                de = fq.double_entry_details
                print(f"         Total debits:  {de.total_debits:>15,.2f}")
                print(f"         Total credits: {de.total_credits:>15,.2f}")
                print(f"         Net difference:{de.net_difference:>15,.2f}")

            # Print trial balance check
            if fq.accounting_equation_holds is not None:
                if fq.accounting_equation_holds:
                    print("      Trial balance: HOLDS (A = L + E)")
                else:
                    print("      Trial balance: DOES NOT HOLD")
                    issues_found += 1

                if fq.assets_total is not None:
                    print(f"         Assets:      {fq.assets_total:>15,.2f}")
                    print(f"         Liabilities: {fq.liabilities_total:>15,.2f}")
                    print(f"         Equity:      {fq.equity_total:>15,.2f}")

            # Print sign convention compliance
            print(f"      Sign conventions: {fq.sign_convention_compliance * 100:.1f}% compliant")
            if fq.sign_violations:
                issues_found += len(fq.sign_violations)
                print(f"         Violations: {len(fq.sign_violations)}")
                for v in fq.sign_violations[:3]:  # Show first 3
                    print(
                        f"           - {v.account_identifier}: expected {v.expected_sign}, got {v.actual_sign}"
                    )
                if len(fq.sign_violations) > 3:
                    print(f"           ... and {len(fq.sign_violations) - 3} more")

            # Print fiscal period integrity
            if fq.fiscal_period_complete:
                print("      Fiscal period: COMPLETE")
            else:
                print("      Fiscal period: INCOMPLETE")
                issues_found += 1

            if fq.period_end_cutoff_clean:
                print("      Period cutoff: CLEAN")
            else:
                print("      Period cutoff: HAS ISSUES")
                issues_found += 1

            if fq.period_integrity_details:
                for pi in fq.period_integrity_details[:2]:  # Show first 2 periods
                    print(
                        f"         {pi.fiscal_period}: {pi.transaction_count} txns, "
                        f"missing {pi.missing_days} days"
                    )

            # Print quality issues summary
            if fq.quality_issues:
                print(f"      Quality issues: {len(fq.quality_issues)}")
                for issue in fq.quality_issues[:3]:
                    print(
                        f"         [{issue.severity.upper()}] {issue.issue_type}: {issue.description}"
                    )
                if len(fq.quality_issues) > 3:
                    print(f"         ... and {len(fq.quality_issues) - 3} more")

        # Summary
        print("\n" + "-" * 50)
        print("3. Financial Quality Summary")
        print("-" * 50)
        print(f"   Tables assessed: {assessed_count}")
        print(f"   Issues found: {issues_found}")

        await print_database_summary(session, duckdb_conn)

        # Show financial quality metrics count
        metrics_count = (
            await session.execute(select(func.count(FinancialQualityMetrics.metric_id)))
        ).scalar()
        print(f"\nFinancial quality metrics: {metrics_count}")

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 9 COMPLETE")
    print("=" * 70)
    print("\nFinancial domain analysis complete. Check the database for detailed results.")
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
