# Cycles Agent - Implementation Status

## Current State (2024-12-21)

### Completed

1. **Domain-Enhanced Cycles Agent**
   - Config: `config/cycles/cycle_vocabulary.yaml`
   - Loader: `src/dataraum_context/analysis/cycles/config.py`
   - Context injection: Domain vocabulary added to agent context
   - Agent accepts optional `domain` parameter

2. **Cycle Vocabulary Structure**
   - 8 core cycle types with stages, entities, completion indicators
   - Domain overlays (financial, retail, manufacturing)
   - Analysis hints for the LLM
   - Completion indicator patterns

3. **Cleanup**
   - Deleted `config/domains/` (old financial config)
   - Deleted `domains/financial/cycles/` (hardcoded cycle detection)
   - Deleted `domains/financial/orchestrator.py`

### Architecture

```
config/cycles/
└── cycle_vocabulary.yaml     # Domain vocabulary

src/dataraum_context/analysis/cycles/
├── __init__.py               # Exports
├── agent.py                  # BusinessCycleAgent (LLM with tools)
├── config.py                 # Config loader
├── context.py                # Context builder (injects vocabulary)
├── db_models.py              # SQLAlchemy persistence
├── models.py                 # Pydantic output models
└── tools.py                  # Agent tools (4 tools)
```

### How It Works

1. **Context Building** (`context.py`):
   - Loads semantic annotations from DB
   - Loads relationships from DB
   - Loads domain vocabulary from config
   - Formats everything for LLM prompt

2. **Agent Loop** (`agent.py`):
   - System prompt: Expert business analyst role
   - User prompt: Dataset context + task instructions
   - Tools: get_column_value_distribution, get_cycle_completion_metrics, etc.
   - Multi-turn conversation until final JSON output

3. **Output** (`models.py`):
   - `BusinessCycleAnalysis`: Full analysis with cycles, summary, recommendations
   - `DetectedCycle`: Individual cycle with stages, entity flows, metrics
   - `CycleStage`, `EntityFlow`: Nested structures

---

## Pending Items

### A3: Domain-Specific Cycle Types in Output Schema

**Current state**: `cycle_type` is a free-form string field in `DetectedCycle`.

**Goal**: Validate/map cycle types to known vocabulary.

**Proposed Solution** (Simple):

Add fields to `DetectedCycle` model:

```python
class DetectedCycle(BaseModel):
    # ... existing fields ...
    cycle_type: str  # What LLM returned (e.g., "ar_cycle", "revenue_cycle")

    # NEW FIELDS:
    canonical_type: str | None = None  # Mapped to vocabulary (e.g., "accounts_receivable")
    is_known_type: bool = False        # True if matches vocabulary
```

Add mapping function in `config.py`:

```python
def map_to_canonical_type(cycle_type: str) -> tuple[str | None, bool]:
    """Map LLM cycle_type to canonical vocabulary type.

    Returns (canonical_type, is_known).
    Handles aliases like "ar_cycle" -> "accounts_receivable".
    """
    cycle_types = get_cycle_types()

    # Direct match
    if cycle_type in cycle_types:
        return cycle_type, True

    # Check aliases
    for canonical, config in cycle_types.items():
        aliases = config.get("aliases", [])
        if cycle_type in aliases or cycle_type.lower() in [a.lower() for a in aliases]:
            return canonical, True

    return None, False
```

Call in `_parse_response()` after parsing each cycle.

**Effort**: ~30 mins

---

### A4: Domain-Specific Anomaly Detection Post-Processing

**Current state**: No post-processing. Agent returns raw analysis.

**Goal**: Flag anomalies based on domain expectations.

**Analysis of the Problem**:

The old `config/domains/financial.yaml` had these anomaly concepts:
- `expected_cycles`: Cycles we expect to see (missing = anomaly)
- `quality_thresholds`: Penalty scores for issues
- `anomaly_rules`: Specific conditions to flag

**Option 1: Post-Processing Function**

Add `analyze_cycle_anomalies()` function:

```python
@dataclass
class CycleAnomaly:
    anomaly_type: str  # "missing_expected", "low_completion", "unclassified", "excessive"
    severity: str      # "high", "medium", "low"
    description: str
    affected_cycles: list[str]
    recommendation: str

def analyze_cycle_anomalies(
    analysis: BusinessCycleAnalysis,
    domain: str | None = None,
) -> list[CycleAnomaly]:
    """Post-process analysis to detect anomalies."""
    anomalies = []
    domain_config = get_domain_config(domain) if domain else {}
    expected = domain_config.get("expected_cycles", [])

    # Missing expected cycles
    detected_types = {c.canonical_type for c in analysis.cycles if c.canonical_type}
    for expected_type in expected:
        if expected_type not in detected_types:
            anomalies.append(CycleAnomaly(
                anomaly_type="missing_expected",
                severity="medium",
                description=f"Expected {expected_type} cycle not detected",
                affected_cycles=[],
                recommendation=f"Verify data contains {expected_type} transactions",
            ))

    # Low completion rates
    for cycle in analysis.cycles:
        if cycle.completion_rate and cycle.completion_rate < 0.5:
            anomalies.append(CycleAnomaly(
                anomaly_type="low_completion",
                severity="high" if cycle.business_value == "high" else "medium",
                description=f"{cycle.cycle_name} has {cycle.completion_rate:.0%} completion",
                affected_cycles=[cycle.cycle_id],
                recommendation="Investigate incomplete transactions",
            ))

    # Unclassified cycles
    unclassified = [c for c in analysis.cycles if not c.is_known_type]
    if unclassified:
        anomalies.append(CycleAnomaly(
            anomaly_type="unclassified",
            severity="low",
            description=f"{len(unclassified)} cycles don't match known types",
            affected_cycles=[c.cycle_id for c in unclassified],
            recommendation="Review if these are domain-specific cycles",
        ))

    return anomalies
```

**Option 2: Extend BusinessCycleAnalysis**

Add anomalies directly to the analysis model:

```python
class BusinessCycleAnalysis(BaseModel):
    # ... existing fields ...

    # NEW:
    anomalies: list[CycleAnomaly] = Field(default_factory=list)
    anomaly_score: float = 0.0  # 0 = no issues, 1 = severe issues
```

Agent's `_parse_response()` would call `analyze_cycle_anomalies()` and populate these fields.

**Option 3: Add to Agent Prompt**

Include anomaly detection in the LLM's task:

```
## Anomaly Detection

Also identify these issues:
- Missing expected cycles for {domain}: {expected_cycles}
- Cycles with <50% completion rate
- Cycles that don't match known types

Include in your response:
"anomalies": [
  {"type": "missing_expected", "description": "...", "severity": "medium"}
]
```

**Recommendation**: Option 1 + Option 2 combined:
- Post-processing function for deterministic checks
- Add anomalies to the analysis model
- Keep agent focused on detection, not validation

**Effort**: ~1-2 hours

---

### Prompt Externalization

**Current state**: Prompts hardcoded in `agent.py` (lines 48-147).

**Goal**: Move to `config/prompts/` for consistency.

**Existing Pattern** (from `semantic_analysis.yaml`):

```yaml
name: cycles_detection
version: "1.0.0"
description: Detect business cycles in dataset
temperature: 0.0

prompt: |
  You are an expert business analyst...

  ## Dataset Context
  {context}

  ## Domain Knowledge
  {domain_vocabulary}

  ## Your Task
  ...

inputs:
  context:
    description: Formatted dataset context
    required: true
  domain_vocabulary:
    description: Cycle type vocabulary
    required: false
    default: ""

output_schema:
  type: object
  required:
    - cycles
  properties:
    cycles:
      type: array

validation:
  cycle_type_values:
    - order_to_cash
    - accounts_receivable
    - procure_to_pay
    # ... from vocabulary
```

**Challenge**: The cycles agent uses **tools** (multi-turn conversation), while other agents use single-turn prompts. The prompt template system doesn't currently support:
- System vs user prompt separation
- Tool definitions
- Multi-turn conversations

**Options**:

1. **Partial externalization**: Move only the text content to YAML, keep tool logic in Python
   ```yaml
   system_prompt: |
     You are an expert business analyst...

   user_prompt_template: |
     Analyze this dataset for business cycles.
     {context}
     ...
   ```

2. **Full externalization**: Extend `PromptTemplate` to support system/user separation
   ```yaml
   system: |
     You are an expert...

   user: |
     {context}
     ...

   tools:
     - get_column_value_distribution
     - get_cycle_completion_metrics
   ```

3. **Hybrid**: Keep current structure but load prompt text from files
   ```python
   SYSTEM_PROMPT = load_prompt_text("cycles_detection", "system")
   USER_PROMPT = load_prompt_text("cycles_detection", "user")
   ```

**Recommendation**: Option 1 (partial externalization) is simplest and maintains consistency without requiring infrastructure changes.

**Effort**: ~1 hour

---

## Implementation Priority

| Item | Effort | Value | Priority |
|------|--------|-------|----------|
| A3: Canonical type mapping | 30 min | Medium | 1 |
| Prompt externalization | 1 hour | Medium | 2 |
| A4: Anomaly detection | 1-2 hours | High | 3 |

Suggested order: A3 → Prompts → A4

---

## Usage Example

```python
from dataraum_context.analysis.cycles import BusinessCycleAgent
from dataraum_context.llm.providers.anthropic import AnthropicProvider

provider = AnthropicProvider(api_key="...")
agent = BusinessCycleAgent(provider)

result = await agent.analyze(
    session=async_session,
    duckdb_conn=conn,
    table_ids=["table1", "table2"],
    domain="financial",  # Optional: adds financial vocabulary
)

if result.success:
    analysis = result.value
    for cycle in analysis.cycles:
        print(f"{cycle.cycle_name}: {cycle.completion_rate:.1%}")
```

---

## Related Modules

- `domains/financial/checks.py` - Accounting validation (separate concern)
- `analysis/semantic/` - Provides semantic annotations used by cycles
- `analysis/relationships/` - Provides relationship graph used by cycles
