# DataRaum Context Engine

**The understanding layer for your data.** A structured, queryable world model that grounds LLMs in a specific company's data — what exists, what it means, how it flows, and what truths can be derived.

Traditional semantic layers tell BI tools "what things are called." DataRaum tells AI **what the data means, how it behaves, how it relates, and what you can compute from it.**

The system combines a metadata pipeline (profiles the data), an entropy layer (measures uncertainty), and an interactive MCP surface (explore, explain, teach) so the world model improves progressively through use.

## Getting Started

- [MCP Setup](mcp-setup.md) — configure DataRaum as an MCP server in Claude Code, Claude Desktop, and Claude for Work
- [CLI Reference](cli.md) — run the pipeline and inspect results from the command line

## Concepts

- [Pipeline](pipeline.md) — the 18-phase analysis pipeline and cold-start bootstrap
- [Entropy](entropy.md) — uncertainty quantification across all metadata dimensions
- [Architecture](architecture.md) — world model, teach overlay, snippet provenance, key design decisions

## Reference

- [Configuration](configuration.md) — config directory structure, verticals, teach-driven overlays
- [Data Model](data-model.md) — metadata schema and storage

## Development

- [Contributing](contributing.md) — development setup, testing, and code patterns
