# ADR-0019 — Observability: OpenTelemetry-native, OTLP as the vendor seam, LGTM for dev

- **Status:** Accepted
- **Date:** 2026-07-07
- **Ticket:** DAT-704 (epic), phases DAT-705/706/707
- **Design doc:** — (this ADR is the design record)

## Context

Observability today is hand-rolled structured log lines on both stacks: structlog to stderr in
the engine, `console.info` chat-middlewares in the cockpit, deliberately converging on a shared
snake_case `llm_call` schema so `grep llm_call | jq` aggregates across both. There are no traces
and no metrics backend; pipeline phase durations are measured (`phases/base.py`) but exist only
as log lines; the Temporal worker runs with no interceptors. Meanwhile the installed stack
already ships OpenTelemetry surfaces we leave unused: `temporalio.contrib.opentelemetry`
(worker-side tracing interceptor, behind the `opentelemetry` extra),
`@temporalio/interceptors-opentelemetry` (same 1.19.x line as our installed client SDK),
`@tanstack/ai`'s `otelMiddleware` (GenAI semantic conventions: token-usage/duration histograms,
content capture off by default), and drizzle-orm's tracing behind its `@opentelemetry/api` peer
dep. The system's defining seam — cockpit client → Temporal → Python worker → SQL — is exactly
where log-line correlation fails and distributed tracing works, and the open LLM-latency epic
(DAT-599) needs per-call latency/token data that today only exists as greppable lines.

## Decision

Instrument both stacks with the **OpenTelemetry SDKs directly** — no vendor wrapper (Sentry,
Logfire, Traceloop rejected per the no-wrapper-deps rule) — leaning on the native integrations
the stack already ships: the Temporal tracing interceptors on both the Python worker and the TS
client (trace context propagates through Temporal headers, yielding one trace per user action
across the whole pipeline), `@tanstack/ai`'s `otelMiddleware` for cockpit LLM calls, GenAI
semantic-convention spans for engine LLM calls, spans at the pipeline phase boundary, and a
structlog→OTel adapter so existing log lines carry trace ids and ship over OTLP.

The **vendor seam is OTLP itself**: one `OTEL_EXPORTER_OTLP_ENDPOINT` env var, no custom
abstraction layer. The dev backend is the **`grafana/otel-lgtm`** all-in-one container
(collector + Tempo + Loki + Prometheus + Grafana) added to the compose stack. The production
backend is **deliberately deferred**: there is no hosted deployment yet, and because everything
speaks OTLP the later choice (Grafana Cloud, self-hosted LGTM, other) is an endpoint-and-creds
swap made with real usage data. Also rejected: dogfooding DataRaum as its own telemetry store —
it creates a circular ops dependency on the product while the product is least stable, and
telemetry streams are not the product's domain (relational business data).

## Consequences

- **One trace from cockpit click → workflow → activities → SQL.** The Temporal interceptors are
  the load-bearing piece; nothing hand-rolled can propagate context across the language seam.
- **Retires grep-as-aggregation.** `llmTelemetryMiddleware` is a strict subset of
  `otelMiddleware` and is deleted; the cross-stack snake_case `llm_call` parity convention
  retires with it (trace semantics replace it); `PhaseResult.duration_seconds` becomes a span
  attribute instead of a log-only orphan. Logging itself stays — what goes is log-grepping as
  the only analysis path.
- **Content capture stays off.** `otelMiddleware` defaults `captureContent: false`; prompt and
  completion text must not land on spans while PII handling (DAT-554) is open.
- **Dependencies:** engine adds `temporalio[opentelemetry]` + the OTel SDK/exporter packages;
  cockpit adds `@opentelemetry/api` + a Node SDK setup (turning the currently-dormant peer deps
  of `@tanstack/ai` and drizzle-orm live).
- **`grafana/otel-lgtm` is dev-grade by design** (single-node, no retention story). That is
  acceptable precisely because the prod decision is deferred; revisit when a hosted environment
  exists.
- **DAT-599 gains its instrument:** LLM latency questions get per-call waterfalls inside the
  pipeline trace that caused them, instead of `grep llm_call | jq`.
- Risk: `@tanstack/ai` floats on `latest` per the cockpit dependency convention, so the
  `otelMiddleware` contract is guarded the same way as the rest of that surface — contract
  tests + tsc, not pins.
