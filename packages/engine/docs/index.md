# DataRaum Context Engine

**The understanding layer for your data.** A structured, queryable operation model that grounds LLMs in a specific company's data — what exists, what it means, how it flows, and what truths can be derived.

Traditional semantic layers tell BI tools "what things are called." DataRaum tells AI **what the data means, how it behaves, how it relates, and what you can compute from it.**

The system combines a metadata pipeline (profiles the data), an entropy layer (measures uncertainty), and an interactive MCP surface (explore, explain, teach) so the operation model improves progressively through use.

## Status

DataRaum is mid-pivot to a TypeScript cockpit + Python engine REST architecture. The previous MCP transport and CLI are retired; the v1 cockpit and engine REST are in active development. See the project README and the `dataraum-cockpit` repo for the current state.

## Concepts

- [Pipeline](pipeline.md) — the 18-phase analysis pipeline and cold-start bootstrap
- [Entropy](entropy.md) — uncertainty quantification across all metadata dimensions
- [Architecture](architecture.md) — operation model, teach overlay, snippet provenance, key design decisions

## Reference

- [Configuration](configuration.md) — config directory structure, verticals, teach-driven overlays
- [Data Model](data-model.md) — metadata schema and storage

## Development

- [Contributing](contributing.md) — development setup, testing, and code patterns
