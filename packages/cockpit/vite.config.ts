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
		nitro({
			preset: "bun",
			rollupConfig: { external: [/^@duckdb\/node-bindings-/] },
		}),
		viteReact(),
	],
});

export default config;
