// Test-only config — vitest picks this up in preference to vite.config.ts.
//
// Two deliberate choices:
//  1. Does NOT load the dev/build plugin stack from vite.config.ts
//     (tanstackStart, nitro, devtools, tailwind). Nitro boots an SSR server that
//     never tears down, so under vitest it hangs the run ("Vite server won't
//     exit") and balloons the module graph. Unit tests only need React (JSX) +
//     the tsconfig path aliases.
//  2. `vitest run` is PURE unit tests only — no DB, no containers, no network.
//     The one real-Postgres test (teach.integration) needs the compose stack and
//     is run separately (`bun run test:integration`), not gated by a leak-prone
//     env flag inside the unit run.

import { fileURLToPath } from "node:url";
import viteReact from "@vitejs/plugin-react";
import { configDefaults, defineConfig } from "vitest/config";

const src = fileURLToPath(new URL("./src", import.meta.url));

export default defineConfig({
	plugins: [viteReact()],
	resolve: {
		// Mirror tsconfig paths (#/* and @/* → src/*) explicitly, so alias
		// resolution doesn't depend on the excluded plugins.
		alias: [
			{ find: /^#\//, replacement: `${src}/` },
			{ find: /^@\//, replacement: `${src}/` },
		],
	},
	test: {
		exclude: [...configDefaults.exclude, "**/*.integration.test.*"],
	},
});
