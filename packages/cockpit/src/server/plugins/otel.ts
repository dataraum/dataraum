// Boot seam (ADR-0019 / DAT-705): bootstrap OpenTelemetry ONCE when the cockpit
// server process starts, ordered BEFORE the orchestration-worker plugin (the
// `plugins` array in vite.config.ts) so every span source — the shared Temporal
// client and the activity-only worker's interceptors — finds the global
// provider already registered from its first use.

import { definePlugin } from "nitro";
import { getOtel } from "#/otel";

export default definePlugin(() => {
	// Idempotent; resolves to null (nothing constructed) when
	// OTEL_EXPORTER_OTLP_ENDPOINT is unset — config.ts is the only gate.
	getOtel();
});
