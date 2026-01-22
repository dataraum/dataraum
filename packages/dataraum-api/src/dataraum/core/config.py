"""Configuration management.

Uses pydantic-settings for type-safe configuration from environment variables.
"""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings.

    All settings can be overridden via environment variables.
    Prefix: DATARAUM_
    """

    model_config = SettingsConfigDict(
        env_prefix="DATARAUM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Ignore extra env vars (like ANTHROPIC_API_KEY)
    )

    # Database (SQLAlchemy)
    # SQLite for local dev, PostgreSQL for production
    database_url: str = Field(
        default="sqlite+aiosqlite:///./dataraum.db",
        description="SQLAlchemy database URL. Use postgresql+asyncpg://... for production",
    )

    # DuckDB
    duckdb_path: str = Field(
        default=":memory:",
        description="Path to DuckDB database file, or :memory: for in-memory",
    )
    duckdb_memory_limit: str = Field(
        default="4GB",
        description="Memory limit for DuckDB",
    )
    duckdb_threads: int = Field(
        default=4,
        description="Number of threads for DuckDB",
    )

    # Configuration paths
    config_path: Path = Field(
        default=Path("config"),
        description="Path to configuration files (ontologies, patterns, rules)",
    )

    # Profiling
    profile_sample_size: int = Field(
        default=100_000,
        description="Number of rows to sample for profiling (0 = all)",
    )
    profile_histogram_buckets: int = Field(
        default=20,
        description="Number of histogram buckets",
    )
    profile_top_k_values: int = Field(
        default=10,
        description="Number of top values to track",
    )

    # Type inference
    type_inference_min_confidence: float = Field(
        default=0.95,
        description="Minimum confidence for automatic type decisions",
    )

    # Quality
    quality_anomaly_threshold: float = Field(
        default=3.0,
        description="Standard deviations for anomaly detection",
    )

    # Default ontology
    default_ontology: str = Field(
        default="general",
        description="Default ontology to use",
    )

    # API
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    api_workers: int = Field(default=1)

    # Logging
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="json")  # 'json' or 'console'


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
