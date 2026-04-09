# Configuration

All DataRaum configuration lives in the `config/` directory as YAML files. Configuration is organized by concern: pipeline orchestration, per-phase settings, entropy thresholds, LLM providers, and vertical-specific domain knowledge.

## Directory Structure

```
config/
├── pipeline.yaml              # Orchestrator: active phases, parallelism, retry
├── null_values.yaml           # NULL value patterns for type inference
│
├── phases/                    # Per-phase settings
│   ├── import.yaml
│   ├── typing.yaml
│   ├── statistics.yaml
│   ├── column_eligibility.yaml
│   ├── relationships.yaml
│   ├── semantic.yaml
│   ├── slicing.yaml
│   ├── temporal.yaml
│   ├── temporal_slice_analysis.yaml
│   ├── business_cycles.yaml
│   └── validation.yaml
│
├── entropy/
│   ├── contracts.yaml         # Use-case thresholds (6 built-in contracts)
│   ├── thresholds.yaml        # Per-dimension entropy thresholds
│   └── network.yaml           # Bayesian network configuration
│
├── filters/                   # System-level quality filters (shared by all verticals)
│   ├── role_based.yaml        # Filters by semantic role (key, measure, timestamp)
│   ├── type_based.yaml        # Filters by data type (DOUBLE, DATE, VARCHAR)
│   ├── pattern_based.yaml     # Filters by column name patterns (email, URL, phone)
│   └── consistency.yaml       # Cross-column checks (date ordering, statistical quality)
│
├── llm/
│   ├── config.yaml            # Provider, model tiers, privacy settings
│   └── prompts/               # LLM prompt templates (13 files)
│       ├── semantic_analysis.yaml
│       ├── column_annotation.yaml
│       ├── slicing_analysis.yaml
│       ├── enrichment_analysis.yaml
│       ├── business_cycles.yaml
│       ├── validation_sql.yaml
│       ├── graph_sql_generation.yaml
│       ├── config_fix.yaml
│       ├── query_analysis.yaml
│       └── sql_repair.yaml
│
└── verticals/                 # Industry-specific domain knowledge
    └── finance/
        ├── ontology.yaml      # Concepts, indicators, temporal behavior
        ├── cycles.yaml        # Business process definitions
        ├── filters/           # Domain-specific filters only
        │   └── consistency.yaml  # Debit/credit consistency check
        ├── metrics/           # Computable business metrics
        │   ├── working_capital/
        │   │   ├── dso.yaml
        │   │   ├── dpo.yaml
        │   │   └── cash_conversion_cycle.yaml
        │   ├── liquidity/
        │   │   └── current_ratio.yaml
        │   └── profitability/
        │       ├── gross_profit.yaml
        │       ├── operating_income.yaml
        │       ├── ebitda.yaml
        │       ├── net_income.yaml
        │       ├── gross_margin.yaml
        │       ├── operating_margin.yaml
        │       ├── ebitda_margin.yaml
        │       └── net_margin.yaml
        └── validations/       # Domain validation rules
            ├── double_entry.yaml
            ├── trial_balance.yaml
            ├── sign_conventions.yaml
            └── fiscal_period.yaml
```

## Pipeline Configuration

`config/pipeline.yaml` controls which phases run and how the orchestrator behaves.

```yaml
# Active phases (execution order determined by dependencies)
phases:
  - import
  - typing
  - statistics
  # ... comment out a phase to disable it

pipeline:
  max_parallel: 4       # ThreadPoolExecutor workers
  fail_fast: true        # Stop on first phase failure
  skip_completed: true   # Skip phases with completed checkpoints
  retry:
    max_retries: 2
    backoff_base: 2.0    # Exponential backoff seconds
```

Phases can be disabled by commenting them out. The orchestrator resolves dependencies automatically — if a phase's dependency is disabled, the phase is skipped.

## LLM Configuration

`config/llm/config.yaml` configures providers, model tiers, and privacy settings.

```yaml
providers:
  anthropic:
    api_key_env: ANTHROPIC_API_KEY
    default_model: claude-sonnet-4-5
    models:
      fast: claude-haiku-4-5
      balanced: claude-sonnet-4-5

active_provider: anthropic

features:
  semantic_analysis:
    enabled: true
    model_tier: balanced     # Use the "balanced" model
  column_annotation:
    enabled: true
    model_tier: fast         # Use the cheaper "fast" model
  enrichment_analysis:
    enabled: true
    model_tier: balanced
  # ... per-feature toggles (7 features total)

privacy:
  max_sample_values: 10
  sensitive_patterns:        # Columns matching these are redacted
    - ".*email.*"
    - ".*ssn.*"
    - ".*password.*"
```

Each LLM feature can be independently enabled/disabled and assigned a model tier. Set `ANTHROPIC_API_KEY` in your environment or pass it via MCP server config.

## Verticals (Domain Configuration)

Verticals encode industry-specific domain knowledge. Currently one vertical ships: `finance`.

### Ontology

Defines business concepts with pattern-matching indicators:

```yaml
name: financial_reporting
concepts:
  - name: revenue
    description: Income from sales or services
    indicators: [revenue, sales, income, turnover]
    exclude_patterns: [cost, expense]
    temporal_behavior: additive    # Can be summed over periods
    typical_role: measure
    unit_from_concept: currency
```

During semantic analysis, columns are matched to ontology concepts via their names, descriptions, and statistical properties.

### Metrics

Declarative computation graphs for business metrics:

```yaml
graph_id: "dso"
metadata:
  name: "Days Sales Outstanding"
  category: "working_capital"

dependencies:
  accounts_receivable:
    type: "extract"
    source:
      standard_field: "accounts_receivable"
      statement: "balance_sheet"
  revenue:
    type: "extract"
    source:
      standard_field: "revenue"
      statement: "income_statement"
  dso:
    type: "formula"
    expression: "(accounts_receivable / revenue) * days_in_period"
    output_step: true

interpretation:
  ranges:
    - { min: 0, max: 30, label: "EXCELLENT" }
    - { min: 31, max: 45, label: "GOOD" }
    - { min: 46, max: 60, label: "CONCERNING" }
```

The graph execution phase uses these definitions to compute metrics via LLM-generated SQL.

### Filters

Quality filters are split into two levels:

**System filters** (`config/filters/`) apply to all verticals automatically:
- **role_based.yaml** — Filters by semantic role (measure, dimension, key)
- **type_based.yaml** — Filters by data type (numeric, date, string)
- **pattern_based.yaml** — Filters by column name patterns (email, URL, phone)
- **consistency.yaml** — Cross-column checks (date ordering, statistical quality)

**Vertical filters** (`config/verticals/<name>/filters/`) contain domain-specific checks only:
- **consistency.yaml** — Finance-specific debit/credit consistency

When loading via `GraphLoader(vertical="finance")`, system filters load first, then vertical filters. Vertical filters can override system filters by using the same `graph_id`.

### Validations

Domain-specific business rules checked via LLM-generated SQL:

```yaml
validation_id: double_entry_balance
name: "Double Entry Balance"
category: accounting
severity: CRITICAL
check_type: balance
parameters:
  tolerance: 0.01
sql_hints: "Compare total debits vs total credits"
```

## Entropy Configuration

### Contracts (`config/entropy/contracts.yaml`)

Define acceptable entropy levels per use case. See [Entropy](entropy.md) for details on the 6 built-in contracts.

### Thresholds (`config/entropy/thresholds.yaml`)

Per-dimension score thresholds that determine low/medium/high classification.

### Network (`config/entropy/network.yaml`)

Bayesian network configuration for causal impact analysis between entropy dimensions.

## Prompt Templates

LLM prompts in `config/llm/prompts/` use YAML with template variables:

```yaml
system: |
  You are a data analyst examining a dataset...

user: |
  Analyze the following table schema:
  {schema_xml}

  Ontology context:
  {ontology_context}
```

Templates receive structured context (schema, statistics, semantic annotations) and produce structured output via tool-use schemas.

## Adding a New Vertical

To add domain knowledge for a new industry:

1. Create `config/verticals/{name}/ontology.yaml` with concepts and indicators
2. Add cycle definitions in `cycles.yaml` for business process detection
3. Add metric graphs in `metrics/` for computable KPIs
4. Add domain-specific filter definitions in `filters/` (system filters from `config/filters/` are inherited automatically)
5. Add validation specs in `validations/` for domain rules

The pipeline automatically discovers and applies vertical configuration based on semantic analysis results. System-level filters (role, type, pattern, and generic consistency checks) apply to all verticals without any per-vertical configuration.
