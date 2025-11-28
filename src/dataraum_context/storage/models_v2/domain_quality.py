"""Domain-specific quality metrics models.

SQLAlchemy models for storing domain-specific quality metrics,
focusing on financial domain rules (double-entry, trial balance, etc.).
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from dataraum_context.storage.models_v2.core import Table


class DomainQualityMetrics(Base):
    """Domain-specific quality metrics for a table.

    Stores quality metrics specific to a business domain (financial, marketing, etc.).
    The metrics field is a flexible JSONB storage for domain-specific data.
    """

    __tablename__ = "domain_quality_metrics"

    metric_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    table_id: Mapped[UUID] = mapped_column(ForeignKey("tables.table_id"))
    domain: Mapped[str] = mapped_column(String)  # 'financial', 'marketing', etc.
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Generic storage for domain-specific metrics
    metrics: Mapped[dict[str, Any]] = mapped_column(JSON)

    # Overall domain compliance
    domain_compliance_score: Mapped[float] = mapped_column(Float)
    violations: Mapped[list[dict[str, Any]]] = mapped_column(JSON)

    # Relationships
    table: Mapped["Table"] = relationship(back_populates="domain_quality_metrics")


class FinancialQualityMetrics(Base):
    """Financial accounting-specific quality metrics.

    Stores detailed financial quality checks like double-entry balance,
    trial balance, sign conventions, etc.
    """

    __tablename__ = "financial_quality_metrics"

    metric_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    table_id: Mapped[UUID] = mapped_column(ForeignKey("tables.table_id"))
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Double-entry accounting
    double_entry_balanced: Mapped[bool] = mapped_column(Boolean)
    balance_difference: Mapped[float] = mapped_column(Float)
    balance_tolerance: Mapped[float] = mapped_column(Float)

    # Trial balance
    trial_balance_check: Mapped[bool] = mapped_column(Boolean)
    accounting_equation_holds: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    assets_total: Mapped[float | None] = mapped_column(Float, nullable=True)
    liabilities_total: Mapped[float | None] = mapped_column(Float, nullable=True)
    equity_total: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Sign conventions
    sign_convention_compliance: Mapped[float] = mapped_column(Float)  # 0-1
    sign_violations: Mapped[list[dict[str, Any]]] = mapped_column(JSON)

    # Consolidation (if multi-entity)
    intercompany_elimination_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    orphaned_intercompany: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Period integrity
    fiscal_period_complete: Mapped[bool] = mapped_column(Boolean)
    period_end_cutoff_clean: Mapped[bool] = mapped_column(Boolean)

    # Overall financial quality score
    financial_quality_score: Mapped[float] = mapped_column(Float)

    # Relationships
    table: Mapped["Table"] = relationship(back_populates="financial_quality_metrics")


class DoubleEntryCheck(Base):
    """Detailed double-entry balance check results.

    Stores the breakdown of debits and credits for validation.
    """

    __tablename__ = "double_entry_checks"

    check_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    metric_id: Mapped[UUID] = mapped_column(ForeignKey("financial_quality_metrics.metric_id"))
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Debit accounts
    total_debits: Mapped[float] = mapped_column(Float)
    debit_account_count: Mapped[int] = mapped_column(Integer)

    # Credit accounts
    total_credits: Mapped[float] = mapped_column(Float)
    credit_account_count: Mapped[int] = mapped_column(Integer)

    # Balance
    net_difference: Mapped[float] = mapped_column(Float)
    is_balanced: Mapped[bool] = mapped_column(Boolean)

    # Context
    currency: Mapped[str | None] = mapped_column(String, nullable=True)
    period_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    period_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class TrialBalanceCheck(Base):
    """Trial balance validation results.

    Stores the accounting equation validation (Assets = Liabilities + Equity).
    """

    __tablename__ = "trial_balance_checks"

    check_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    metric_id: Mapped[UUID] = mapped_column(ForeignKey("financial_quality_metrics.metric_id"))
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Accounting equation components
    assets: Mapped[float] = mapped_column(Float)
    liabilities: Mapped[float] = mapped_column(Float)
    equity: Mapped[float] = mapped_column(Float)

    # Validation
    equation_difference: Mapped[float] = mapped_column(Float)  # Assets - (Liabilities + Equity)
    equation_holds: Mapped[bool] = mapped_column(Boolean)
    tolerance: Mapped[float] = mapped_column(Float)

    # Context
    period_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    period_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SignConventionViolation(Base):
    """Sign convention violations.

    Stores examples of accounts that violate expected sign conventions
    (e.g., negative revenue, positive expense).
    """

    __tablename__ = "sign_convention_violations"

    violation_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    metric_id: Mapped[UUID] = mapped_column(ForeignKey("financial_quality_metrics.metric_id"))
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Account info
    account_identifier: Mapped[str] = mapped_column(String)
    account_type: Mapped[str] = mapped_column(String)  # 'Asset', 'Revenue', etc.
    expected_sign: Mapped[str] = mapped_column(String)  # 'debit' or 'credit'
    actual_sign: Mapped[str] = mapped_column(String)  # 'debit' or 'credit'

    # Details
    amount: Mapped[float] = mapped_column(Float)
    violation_severity: Mapped[str] = mapped_column(String)  # 'minor', 'moderate', 'severe'
    description: Mapped[str] = mapped_column(String)

    # Context
    transaction_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class IntercompanyTransaction(Base):
    """Intercompany transaction tracking.

    Stores intercompany transactions for consolidation validation.
    """

    __tablename__ = "intercompany_transactions"

    transaction_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    metric_id: Mapped[UUID] = mapped_column(ForeignKey("financial_quality_metrics.metric_id"))

    # Transaction details
    source_entity: Mapped[str] = mapped_column(String)
    target_entity: Mapped[str] = mapped_column(String)
    amount: Mapped[float] = mapped_column(Float)
    transaction_date: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Matching status
    is_matched: Mapped[bool] = mapped_column(Boolean)
    matching_transaction_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("intercompany_transactions.transaction_id"), nullable=True
    )
    elimination_status: Mapped[str] = mapped_column(String)  # 'eliminated', 'pending', 'orphaned'

    # Details
    description: Mapped[str | None] = mapped_column(String, nullable=True)


class FiscalPeriodIntegrity(Base):
    """Fiscal period integrity checks.

    Validates that fiscal periods are complete and have clean cutoffs.
    """

    __tablename__ = "fiscal_period_integrity"

    check_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    metric_id: Mapped[UUID] = mapped_column(ForeignKey("financial_quality_metrics.metric_id"))
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Period definition
    fiscal_period: Mapped[str] = mapped_column(String)  # 'Q1-2024', 'FY-2024', etc.
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Completeness
    is_complete: Mapped[bool] = mapped_column(Boolean)
    missing_days: Mapped[int] = mapped_column(Integer)

    # Cutoff validation
    cutoff_clean: Mapped[bool] = mapped_column(Boolean)
    late_transactions: Mapped[int] = mapped_column(Integer)  # Transactions after period close
    early_transactions: Mapped[int] = mapped_column(Integer)  # Transactions before period start

    # Quality indicators
    transaction_count: Mapped[int] = mapped_column(Integer)
    total_amount: Mapped[float] = mapped_column(Float)
