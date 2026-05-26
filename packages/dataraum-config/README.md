# dataraum-config

Configuration **data** for the DataRaum stack — entropy contracts, LLM provider
config + prompt templates, per-phase pipeline config, and vertical (domain)
definitions. This is data, not code: no Python, no TypeScript, no build step.

It lives as a standalone top-level package so it can be mounted into any
container that needs it (engine today; cockpit for vertical-YAML UX hints later)
and, eventually, swapped for a remote backend (e.g. S3) without touching call
sites.

## Layout

```
dataraum-config/
├── entropy/                 # detector contracts, thresholds, network topology
│   ├── contracts.yaml
│   ├── thresholds.yaml
│   └── network.yaml
├── llm/
│   ├── config.yaml          # provider + model selection
│   └── prompts/             # one YAML per prompt template
├── phases/                  # per-phase pipeline config (phases/<name>.yaml)
├── verticals/               # domain definitions
│   └── finance/             # ontology, metrics, validations, cycles
├── null_values.yaml         # global null-token vocabulary
└── pipeline.yaml            # active phases, parallelism, retry config
```

## How it's consumed

### Engine (Python)

The engine resolves this directory through `dataraum.core.config` — never via
relative paths. Resolution priority (highest first):

1. `set_config_root()` test override
2. Active-workspace overlay (filesystem; set at server bootstrap)
3. **`DATARAUM_CONFIG_PATH`** env var — the production path
4. Auto-detection of the sibling `packages/dataraum-config/` (dev / CLI / tests)

In containers, `DATARAUM_CONFIG_PATH=/opt/dataraum/config` and this directory is
**bind-mounted** to that path (it is no longer baked into the engine image). On
the host, the auto-detection fallback finds this package next to
`packages/engine/`, so `uv run` / pytest work without setting the env var.

### Cockpit (TypeScript)

Mounted read-only at the same path for future vertical-YAML UX reads (e.g.
"which concept did the agent bind to?"). No consumers yet — the mount is
plumbing.

## Conventions

- One concept per file; group by directory.
- YAML only. Parsed via `yaml.safe_load` (engine) — no anchors/tags beyond
  plain data.
- Adding a phase config: drop `phases/<phase_name>.yaml`; the loader picks it up
  by convention (`load_phase_config`).
