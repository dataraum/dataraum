# Backlog

Prioritized backlog for the dataraum-context project.

**Tracking:**
- **Day-to-day work:** [Linear — Dataraum](https://linear.app/dataraum) (private)
- **Public feature requests:** [GitHub Issues](https://github.com/dataraum/dataraum-context/issues)

**Linear projects:**
- **Phase 1: Open Source Core** — Current focus (DAT-5 to DAT-16)
- **Phase 2: Cloud Trial** — Cloud infrastructure, connectors, auth (DAT-17 to DAT-24, DAT-42)
- **Phase 3: Team & Enterprise** — SSO, RBAC, governance, compliance (DAT-25 to DAT-34)

---

## Current Focus

### Next Up
- [ ] Run e2e tests to verify cycle health scoring (`tests/e2e/test_cycle_health.py`)
- [ ] Dependency audit (pandas vs pyarrow, ruptures, networkx)
- [ ] Docs cleanup: triage `docs/projects/`, swap `docs_new/` → `docs/`, update README + CLAUDE.md cross-refs

### Active Linear Issues (Phase 1)
- DAT-35: Entropy bugs follow-up
- DAT-37: Graph module: cache invalidation, filter linking, multi-table
- DAT-40: Code cleanup: print→logging, missing indexes, missing FKs
- DAT-41: Filter generation and formatter integration
- DAT-44: Config externalization (query agent, entropy behavior, temporal slice)
- DAT-45: Entropy improvements (contract extends, ontology text, isolation forest, topology persistence)
- DAT-46: Agent async consistency and resilience
- DAT-47: MCP tool expansion (metric explanations, executions, examples)

### Open GitHub Issues
- #7: Extensible plugin system
- #9: Complete multi-table business cycle detection
- #10: REST API for pipeline metadata
- #13: Additional data source connectors
- #14: Smarter date format detection
- #15: Numeric and range-based data slicing
- #16: Cross-run data quality trending
- #17: Richer statistical profiling
- #18: String and composite correlation detection
- #19: Composite key detection in relationships
- #20: Surface validation results in interfaces (partially done — backend context complete)
- #21: Numeric drift detection for temporal slices
- #22: Domain-specific vertical configuration (partially done — VerticalConfig abstraction complete)

---

## Recently Completed

- Agent refactoring: phases A–D, E2E tests, temporal_behavior, entropy calibration
- Cycle-scoped validations + cycle health scoring
- Module-by-module streamlining (18 modules)
- Post-refactor verification: TUI screens, MCP tools, ontology↔standard_field alignment
