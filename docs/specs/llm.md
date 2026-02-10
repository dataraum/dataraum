# LLM Module Specification

## Reasoning & Summary

The `llm/` module provides shared infrastructure for all LLM-powered features in the system. It is a **cross-cutting module** — not a pipeline phase itself, but consumed by 7+ agents across the codebase.

**Problem it solves:** Centralizes LLM provider configuration, prompt template rendering, privacy-safe data sampling, and a common base class for LLM features. This avoids each agent implementing its own LLM boilerplate.

**Design principle:** Agents are co-located with their domain modules (e.g., `analysis/semantic/agent.py`), not in the `llm/` module. The `llm/` module only contains shared infrastructure.

## Architecture

```
llm/
├── __init__.py          # Public API: LLMConfig, load_llm_config, PromptRenderer, create_provider
├── config.py            # LLMConfig Pydantic model, load from config/system/llm.yaml
├── prompts.py           # PromptRenderer: YAML template loading + Jinja2 rendering
├── privacy.py           # DataSampler: privacy-safe sample preparation for prompts
├── providers/
│   ├── __init__.py      # create_provider() factory
│   ├── base.py          # LLMProvider ABC, request/response models, tool use models
│   └── anthropic.py     # AnthropicProvider: Claude API integration
└── features/
    ├── __init__.py
    └── _base.py         # LLMFeature base class (config + provider + renderer)
```

**824 LOC total** (after cleanup removing ~496 LOC of dead code).

## Key Components

### LLMConfig (`config.py`)

Pydantic model loaded from `config/system/llm.yaml`. Contains:
- `active_provider`: Which provider to use (currently only `"anthropic"`)
- `providers`: Provider-specific config (API keys, model mappings)
- `features`: Per-feature enable/disable + model tier settings
- `privacy`: Data sampling and PII controls
- `limits`: Token limits, cache TTL

### LLMProvider (`providers/base.py`)

Abstract base with two methods:
- `converse(ConversationRequest) → Result[ConversationResponse]`: Tool-use conversation
- `get_model_for_tier(tier: str) → str`: Map tier names ("fast", "balanced") to model IDs

Supporting models:
- `ToolDefinition`, `ToolCall`, `ToolResult`: Structured tool use
- `Message`, `ConversationRequest`, `ConversationResponse`: Conversation protocol
- `LLMRequest`, `LLMResponse`: Legacy simple request/response (kept for compatibility)

### PromptRenderer (`prompts.py`)

Loads YAML prompt templates from `config/system/prompts/*.yaml` and renders them with Jinja2 context variables. Supports split rendering into `(system_prompt, user_prompt, temperature)` tuples.

### DataSampler (`privacy.py`)

Prepares sample data from column profiles for LLM prompts with privacy controls (PII masking, sample size limits).

### LLMFeature (`features/_base.py`)

Base class for all agents. Stores `config`, `provider`, `renderer` — agents extend this and implement domain-specific logic.

## Consumers

| Agent | Module | Purpose |
|-------|--------|---------|
| `SemanticAgent` | `analysis/semantic/` | Column/table semantic analysis |
| `SlicingAgent` | `analysis/slicing/` | Data slicing recommendations |
| `ValidationAgent` | `analysis/validation/` | Data validation SQL generation |
| `QualitySummaryAgent` | `analysis/quality_summary/` | Quality report synthesis |
| `EntropyInterpreter` | `entropy/` | Entropy interpretation |
| `EntropySummaryAgent` | `entropy/` | Entropy summary generation |
| `GraphAgent` | `graphs/` | Metric SQL generation |
| `QueryAgent` | `query/` | Natural language query → SQL |

## Configuration

**File:** `config/system/llm.yaml`

Key sections:
- `active_provider`: `"anthropic"`
- `providers.anthropic.api_key`: API key (or env var)
- `providers.anthropic.models`: Tier-to-model mapping
- `features.<feature>.enabled`: Feature toggle
- `features.<feature>.model_tier`: Which model tier to use
- `privacy.max_sample_rows`: Maximum samples per column

## Cleanup History

**Removed in earlier refactor:**
- `cache.py` + `db_models.py` (297 LOC): LLM response cache — redundant with pipeline phase checkpointing via `should_skip()`
- `providers/openai.py` (48 LOC): Stub raising `NotImplementedError`
- `providers/local.py` (51 LOC): Stub raising `NotImplementedError`
- `complete()` method on `LLMProvider` (~75 LOC): Unused simple completion — all agents use `converse()` with tool use
- `cache` parameter on `LLMFeature.__init__()` and all 7 agent constructors
- `LLMCache()` instantiation from 9 pipeline phase files

**Removed in streamline refactor:**
- `LLMRequest`/`LLMResponse` from `providers/__init__.py` exports (never imported outside module)
- Commented-out SDV example code in `privacy.py` (~40 lines)
- Extracted hardcoded redacted sample count to `LLMPrivacy.redacted_sample_count`

## Roadmap

- **Provider extensibility**: Add OpenAI/Ollama providers when needed (not as stubs — implement when there's a use case)
- **Token tracking**: Aggregate LLM token usage across pipeline runs for cost monitoring
- **Prompt versioning**: Track prompt template versions to detect when re-runs are needed due to prompt changes
