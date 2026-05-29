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
		nitro({ rollupConfig: { external: [/^@duckdb\/node-bindings-/] } }),
		viteReact(),
	],
	server: {
		proxy: {
			// Dev-only: same-origin /api requests on :3000 proxy to the Python
			// engine REST at :8000. In production the cockpit talks cross-origin
			// (CORS is configured on the engine side); the proxy is just to make
			// dev hot-reload nicer.
			"/api": {
				target: "http://localhost:8000",
				changeOrigin: true,
			},
		},
	},
});

export default config;
