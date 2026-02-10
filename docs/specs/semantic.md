# Semantic Module Specification

## Reasoning & Summary

The `analysis/semantic/` module provides LLM-powered semantic analysis of database schemas. It determines the business meaning of tables and columns, maps them to domain ontology concepts, and confirms/discovers relationships.

**Problem it solves:** Raw column names and types (e.g., `col_amt DECIMAL`) are not sufficient for AI-driven analytics. This module enriches metadata with business context: "This is `Transaction Amount`, a `measure` mapped to the `revenue` concept, with `high` confidence."

**Key insight:** The LLM doesn't analyze data in isolation. It receives rich context from prior pipeline phases (statistics, relationships, correlations) and an ontology definition, making its analysis grounded and deterministic.

## Architecture

```
analysis/semantic/
├── __init__.py      # Public API exports
├── agent.py         # SemanticAgent: LLM interaction, prompt building, response parsing
├── processor.py     # enrich_semantic(): Orchestrator — calls agent, stores results
├── models.py        # Pydantic models: tool output schemas + internal models
├── db_models.py     # SQLAlchemy: SemanticAnnotation, TableEntity
├── ontology.py      # OntologyLoader: YAML ontology loading + caching
└── utils.py         # Shared utilities: column/table mapping, correlation loading
```

**~1820 LOC total** across 7 files.

## Data Flow

```
Prior Phases (statistics, relationships, correlations)
  ↓
processor.enrich_semantic()
  ├── agent.analyze()
  │   ├── _load_profiles()          → Column statistics from DB
  │   ├── load_correlations()       → FDs, numeric correlations, derived columns
  │   ├── OntologyLoader.load()     → Domain concepts from YAML
  │   ├── _build_tables_json()      → JSON schema for prompt
  │   ├── _format_relationship_candidates()  → Pre-computed join candidates
  │   ├── PromptRenderer.render_split()      → System + user prompts
  │   ├── provider.converse()       → LLM call with tool use
  │   └── _parse_tool_output()      → SemanticEnrichmentResult
  ├── Store annotations → semantic_annotations table
  ├── Store entities → table_entities table
  └── Store relationships → relationships table (with RI metrics)
```

## Data Model

### SemanticAnnotation (DB)

| Column | Type | Description |
|--------|------|-------------|
| `annotation_id` | PK | UUID |
| `column_id` | FK → columns | One annotation per column (unique constraint) |
| `semantic_role` | String | `key`, `foreign_key`, `measure`, `dimension`, `timestamp`, `attribute` |
| `entity_type` | String | Domain-specific: `customer_id`, `transaction_amount`, etc. |
| `business_name` | String | Human-readable name |
| `business_description` | Text | LLM-generated description |
| `business_concept` | String | Ontology concept mapping (e.g., `revenue`, `accounts_receivable`) |
| `annotation_source` | String | `llm`, `manual`, `config_override` |
| `annotated_by` | String | Model name or user ID |
| `confidence` | Float | 0.0–1.0 |

**Consumed by:** 15+ modules including entropy detectors, graph context assembly, query agent, MCP formatters, TUI screens.

### TableEntity (DB)

| Column | Type | Description |
|--------|------|-------------|
| `entity_id` | PK | UUID |
| `table_id` | FK → tables | Table being classified |
| `detected_entity_type` | String | `customers`, `orders`, `transactions`, etc. |
| `description` | Text | LLM-generated table description |
| `confidence` | Float | 0.0–1.0 |
| `grain_columns` | JSON | Columns forming the unique grain (primary key) |
| `is_fact_table` | Boolean | Fact vs dimension classification |
| `is_dimension_table` | Boolean | Inverse of is_fact_table |
| `detection_source` | String | `llm`, `heuristic`, `manual` |

**Consumed by:** Graph context assembly, entropy detectors, query agent schema building.

## LLM Tool Schema

The agent uses Anthropic tool use with a Pydantic-generated JSON schema (`SemanticAnalysisOutput`). The LLM is forced to produce structured output containing:

- **Tables**: Entity type, description, fact/dimension classification, grain, time column
- **Columns**: Semantic role, entity type, business term, business concept (from ontology), confidence
- **Relationships**: From/to table+column, type, cardinality, confidence, reasoning

## Context Enrichment

The LLM prompt includes context from prior phases:

| Source | What's Included | Purpose |
|--------|----------------|---------|
| Statistics | Column profiles, cardinality ratios, sample values | Ground column role detection |
| Relationships | Pre-computed join candidates with overlap scores, RI metrics, topology | Confirm/filter relationships |
| Correlations | Functional dependencies, strong numeric correlations, derived columns | Identify key columns, computed fields |
| Ontology | Domain concepts with indicators and descriptions | Map columns to business concepts |
| Graph Topology | Hub/leaf/bridge classification, schema cycles | Understand table roles in schema |

## Configuration

### Pipeline Config (`config/system/pipeline.yaml`)

```yaml
semantic:
  ontology: financial_reporting  # Must match config/ontologies/<name>.yaml
```

### LLM Config (`config/system/llm.yaml`)

```yaml
features:
  semantic_analysis:
    enabled: true
    model_tier: balanced  # Uses the more capable model
```

### Ontologies (`config/ontologies/*.yaml`)

Define domain-specific business concepts:

```yaml
name: financial_reporting
concepts:
  - name: revenue
    description: Income from operations
    indicators: [revenue, sales, income, turnover]
    typical_role: measure
  - name: accounts_receivable
    description: Money owed by customers
    indicators: [receivable, ar, debtor]
```

## Pipeline Phase

**Phase name:** `semantic`
**Dependencies:** `statistics`, `relationships`, `correlations`
**Outputs:** `annotations`, `entities`, `confirmed_relationships`
**Skip condition:** All columns already have LLM annotations
**Fail-fast:** No ontology configured → immediate failure with actionable error message

## Cleanup History

**Fixed in this refactor:**
- **Cardinality bug** (`agent.py`): `many_to_one` was mapped to `Cardinality.ONE_TO_MANY` instead of `Cardinality.MANY_TO_ONE` — impacts downstream entropy calculations
- **Bare except** (`processor.py`): Silent `except: pass` replaced with `logger.warning()` for RI metrics computation failures
- **Logging added** to `processor.py` (was missing `get_logger`)
- **Config fallback removed** (`semantic_phase.py`): `ctx.config.get("ontology", "financial_reporting")` → fail-fast if not configured
- **Dead code removed**: `SemanticAnalysisOutput.summary` (parsed from LLM but never accessed), `EntityDetection.evidence` (always `{}`), `Relationship.is_confirmed` (never modified from default)
- **Hardcoded max_tokens** (`agent.py`): Changed from `8192` to `self.config.limits.max_output_tokens_per_request` — consistent with all other agents
- **Inline imports moved to module level** (`agent.py`, `ontology.py`): `func`, `StatisticalProfile`, `NumericStats`, `StringStats`, `ValueCount`, `get_config_dir`

## Roadmap

- **Incremental re-analysis**: Support re-analyzing specific tables without re-running the full phase
- **Confidence calibration**: Validate LLM confidence scores against ground truth in test datasets
