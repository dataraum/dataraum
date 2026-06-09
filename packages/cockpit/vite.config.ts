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
		// Same deal for polyglot (@polyglot-sql/sdk, DAT-485): its WASM parser loads
		// a sibling `polyglot_sql.wasm` via a runtime `readFileSync(file:…)` relative
		// to its own dist file. Bundling the JS into `.output/server/_libs/` leaves a
		// dangling reference to a `.wasm` that was never copied → the server 500s at
		// boot with `ENOENT … polyglot_sql.wasm`. Externalize the package so it
		// resolves from node_modules (where the `.wasm` sibling actually is); the
		// runner image carries it via the same node_modules copy as DuckDB.
		// preset "bun": the runner serves with `bun .output/server/index.mjs`, so
		// build the Bun-native server (Bun.serve via srvx) instead of the node
		// default — the sanctioned shape for a Bun deployment (DAT-451).
		nitro({
			preset: "bun",
			rollupConfig: {
				external: [/^@duckdb\/node-bindings-/, /^@polyglot-sql\/sdk/],
			},
		}),
		viteReact(),
	],
});

export default config;
