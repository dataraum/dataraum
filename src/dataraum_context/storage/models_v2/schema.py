"""Schema Version Tracking.

SQLAlchemy model for database schema version management.
Used by migration system to track applied schema versions.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from dataraum_context.storage.models_v2.base import Base


class DBSchemaVersion(Base):
    """Track schema versions for database migrations.

    Note: Named DBSchemaVersion to avoid conflict with core.models.
    """

    __tablename__ = "schema_version"

    version: Mapped[str] = mapped_column(String, primary_key=True)
    applied_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
