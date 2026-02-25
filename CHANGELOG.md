# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Agent refactoring: streamlined cycle, validation, graph, and query agents

## [0.1.0] - 2025-12-01

### Added
- 18-phase analysis pipeline (staging, profiling, enrichment, quality, context)
- MCP server with 6 tools: `analyze`, `get_context`, `get_entropy`, `evaluate_contract`, `query`, `get_actions`
- Entropy system with uncertainty quantification across 8 dimensions
- Data readiness contracts (`aggregation_safe`, `executive_dashboard`, etc.)
- CLI (`dataraum`) with run, status, entropy, and contracts commands
- TUI for interactive pipeline monitoring
- Semantic analysis via LLM (Claude, OpenAI) or manual overrides
- Domain ontologies for financial, marketing, and custom verticals
- DuckDB compute engine with SQLite metadata storage
- Temporal analysis (granularity, gaps, seasonality, trends)
- Topological relationship detection and join path inference
- Statistical profiling (distributions, cardinality, null rates, patterns)
- Quality rule generation and scoring
- Privacy support via synthetic data generation (SDV)
- PostgreSQL backend option
