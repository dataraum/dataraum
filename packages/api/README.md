# packages/api

OpenAPI contract for the DataRaum engine REST surface. `openapi.yaml` is generated from the engine's FastAPI app via `packages/engine/scripts/export_openapi.py` and consumed by `packages/cockpit` via `openapi-typescript` codegen.

Regenerate:

```bash
(cd packages/engine && uv run python scripts/export_openapi.py) > packages/api/openapi.yaml
```

CI publishes it on diff (the engine CI step lands as a follow-up — currently manual).
