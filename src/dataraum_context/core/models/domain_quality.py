"""Domain-specific quality models.

Pydantic models for domain-specific quality metrics,
focusing on financial domain validation.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class DoubleEntryResult(BaseModel):
    """Double-entry balance check result."""

    check_id: UUID
    total_debits: float
    total_credits: float
    net_difference: float
    is_balanced: bool
    debit_account_count: int
    credit_account_count: int
    currency: str | None = None
    period_start: datetime | None = None
    period_end: datetime | None = None


class TrialBalanceResult(BaseModel):
    """Trial balance validation result."""

    check_id: UUID
    assets: float
    liabilities: float
    equity: float
    equation_difference: float
    equation_holds: bool
    tolerance: float
    period_start: datetime | None = None
    period_end: datetime | None = None


class SignConventionViolation(BaseModel):
    """Sign convention violation details."""

    violation_id: UUID
    account_identifier: str
    account_type: str
    expected_sign: str  # 'debit' or 'credit'
    actual_sign: str
    amount: float
    violation_severity: str  # 'minor', 'moderate', 'severe'
    description: str
    transaction_date: datetime | None = None


class IntercompanyTransactionMatch(BaseModel):
    """Intercompany transaction matching status."""

    transaction_id: UUID
    source_entity: str
    target_entity: str
    amount: float
    transaction_date: datetime
    is_matched: bool
    matching_transaction_id: UUID | None = None
    elimination_status: str  # 'eliminated', 'pending', 'orphaned'
    description: str | None = None


class FiscalPeriodIntegrityCheck(BaseModel):
    """Fiscal period integrity validation."""

    check_id: UUID
    fiscal_period: str
    period_start: datetime
    period_end: datetime
    is_complete: bool
    missing_days: int
    cutoff_clean: bool
    late_transactions: int
    early_transactions: int
    transaction_count: int
    total_amount: float


class FinancialQualityResult(BaseModel):
    """Complete financial quality assessment."""

    metric_id: UUID
    table_id: UUID
    computed_at: datetime

    # Double-entry
    double_entry_balanced: bool
    balance_difference: float
    balance_tolerance: float
    double_entry_details: DoubleEntryResult | None = None

    # Trial balance
    trial_balance_check: bool
    accounting_equation_holds: bool | None = None
    assets_total: float | None = None
    liabilities_total: float | None = None
    equity_total: float | None = None
    trial_balance_details: TrialBalanceResult | None = None

    # Sign conventions
    sign_convention_compliance: float = Field(ge=0.0, le=1.0)
    sign_violations: list[SignConventionViolation] = Field(default_factory=list)

    # Consolidation
    intercompany_elimination_rate: float | None = Field(None, ge=0.0, le=1.0)
    orphaned_intercompany: int | None = None
    intercompany_details: list[IntercompanyTransactionMatch] = Field(default_factory=list)

    # Period integrity
    fiscal_period_complete: bool
    period_end_cutoff_clean: bool
    period_integrity_details: list[FiscalPeriodIntegrityCheck] = Field(default_factory=list)

    # Overall score
    financial_quality_score: float = Field(ge=0.0, le=1.0)

    # Issues
    quality_issues: list[FinancialQualityIssue] = Field(default_factory=list)
    has_issues: bool


class FinancialQualityIssue(BaseModel):
    """Financial quality issue."""

    issue_type: str  # 'double_entry_imbalance', 'sign_violation', 'period_incomplete', etc.
    severity: str  # 'minor', 'moderate', 'severe', 'critical'
    description: str
    affected_accounts: list[str] = Field(default_factory=list)
    recommendation: str | None = None


class DomainQualityResult(BaseModel):
    """Generic domain quality result."""

    metric_id: UUID
    table_id: UUID
    domain: str
    computed_at: datetime

    # Generic metrics storage
    metrics: dict[str, Any]

    # Overall compliance
    domain_compliance_score: float = Field(ge=0.0, le=1.0)
    violations: list[dict[str, Any]] = Field(default_factory=list)

    # Domain-specific result (if financial)
    financial_quality: FinancialQualityResult | None = None


class SignConventionConfig(BaseModel):
    """Configuration for sign conventions by account type."""

    asset: str = "debit"
    liability: str = "credit"
    equity: str = "credit"
    revenue: str = "credit"
    expense: str = "debit"

    def get_expected_sign(self, account_type: str) -> str:
        """Get expected sign for an account type."""
        account_type_lower = account_type.lower()

        if account_type_lower in ["asset", "assets"]:
            return self.asset
        elif account_type_lower in ["liability", "liabilities"]:
            return self.liability
        elif account_type_lower in ["equity", "stockholders equity", "shareholders equity"]:
            return self.equity
        elif account_type_lower in ["revenue", "income", "sales"]:
            return self.revenue
        elif account_type_lower in ["expense", "expenses", "cost"]:
            return self.expense
        else:
            # Default for unknown types
            return "debit"


class FinancialQualityConfig(BaseModel):
    """Configuration for financial quality checks."""

    double_entry_tolerance: float = 0.01
    trial_balance_tolerance: float = 0.01
    sign_conventions: SignConventionConfig = Field(default_factory=SignConventionConfig)
    check_intercompany: bool = True
    check_fiscal_periods: bool = True
    fiscal_year_end_month: int | None = None  # e.g., 12 for December
