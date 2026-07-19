import tailwindcss from "@tailwindcss/vite";
import { devtools } from "@tanstack/devtools-vite";

import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";
import { nitro } from "nitro/vite";
import { defineConfig } from "vite";

const config = defineConfig({
	resolve: { tsconfigPaths: true },
	plugins: [
		devtools(),
		tailwindcss(),
		tanstackStart(),
		// Keep DuckDB's NATIVE binary out of the server bundle. `@duckdb/node-api`
		// → `@duckdb/node-bindings` loads a platform-specific
		// `@duckdb/node-bindings-<plat>/duckdb.node` via a runtime `require`; if
		// Nitro/Rolldown tries to bundle that `.node`, the build dies with
		// `UNLOADABLE_DEPENDENCY … stream did not contain valid UTF-8` (it can't read
		// the binary as a module — duckdb/duckdb-node-neo#231). We externalize ONLY
		// the platform binding packages (`@duckdb/node-bindings-*`): the wrapper +
		// `node-bindings` JS (and `detect-libc`) still get bundled, and the `.node`
		// is `require`d at runtime. The runner image must therefore carry those
		// binding packages in node_modules — see packages/cockpit/Dockerfile.
		// preset "bun": the runner serves with `bun .output/server/index.mjs`, so
		// build the Bun-native server (Bun.serve via srvx) instead of the node
		// default — the sanctioned shape for a Bun deployment (DAT-451).
		// The co-located activity-only worker (DAT-529, slimmed DAT-708) boots
		// here: a Nitro plugin runs once at server startup and starts the
		// singleton worker. The OTel bootstrap (ADR-0019/DAT-705) is a plugin
		// too and MUST run first — the worker's interceptors and the Temporal
		// client resolve the global tracer provider it registers.
		nitro({
			preset: "bun",
			plugins: [
				"./src/server/plugins/otel.ts",
				"./src/server/plugins/orchestration-worker.ts",
				// Workspace-registry boot seed (DAT-819): the membership gate
				// fronts every request, so the (idempotent) seed that creates
				// the dev login user must not depend on a request arriving.
				"./src/server/plugins/registry-seed.ts",
			],
			rollupConfig: {
				external: [
					/^@duckdb\/node-bindings-/,
					// The Temporal WORKER side is native (Rust core-bridge `.node`) and
					// its package tree drags in a workflow bundler (webpack + @swc/core
					// native); bundling it dies like duckdb's binary. Externalize the
					// whole @temporalio scope + @swc so they `require` from node_modules
					// at runtime (the runner image copies @temporalio — see Dockerfile).
					// The worker is ACTIVITY-ONLY (DAT-708): no workflow code, no vm
					// sandbox — the runtime uses only the slim worker core + core-bridge.
					/^@temporalio\//,
					/^@swc\//,
					// Rolldown's CJS→ESM interop breaks @opentelemetry's class
					// inheritance across package boundaries (OTLPTraceExporter extends
					// OTLPExporterBase → "Cannot call a class constructor without new"
					// at boot — DAT-705, same failure family as the duckdb binary).
					// Externalize the scope; the runner's prod node_modules carries it,
					// and @temporalio/interceptors-opentelemetry (already external)
					// resolves the SAME copies, so no split module instances either.
					/^@opentelemetry\//,
				],
			},
		}),
		viteReact(),
	],
});

export default config;
