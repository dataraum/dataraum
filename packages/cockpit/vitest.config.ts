// Test-only config — vitest picks this up in preference to vite.config.ts.
//
// Two deliberate choices:
//  1. Does NOT load the dev/build plugin stack from vite.config.ts
//     (tanstackStart, nitro, devtools, tailwind). Nitro boots an SSR server that
//     never tears down, so under vitest it hangs the run ("Vite server won't
//     exit") and balloons the module graph. Unit tests only need React (JSX) +
//     the tsconfig path aliases.
//  2. Two projects split unit from integration:
//       - `unit` (default `vitest run` / `bun run test`): PURE units — no DB, no
//         containers, no network. Tools import a live postgres() client at module
//         load, so any unit test that pulls a tool MUST mock `#/config` +
//         `#/db/metadata/client` (see registry.test.ts / chat.test.ts).
//       - `integration` (`bun run test:integration`, only with the compose stack
//         up): the real-Postgres tests (`*.integration.test.*`). Each self-skips
//         when METADATA_DATABASE_URL is unset, but keeping them in a separate
//         project means the default run never loads them at all.

import { fileURLToPath } from "node:url";
import viteReact from "@vitejs/plugin-react";
import { configDefaults, defineConfig } from "vitest/config";

const src = fileURLToPath(new URL("./src", import.meta.url));

const INTEGRATION_GLOB = "**/*.integration.test.*";

// jsdom polyfills (matchMedia / document.fonts / ResizeObserver) for the DOM
// tests; a no-op under the node environment. Loaded by both projects.
const setupFiles = ["./src/test-setup.ts"];

// Shared across both projects: the lean React-only plugin set + tsconfig path
// aliases (#/* and @/* → src/*), so alias resolution doesn't depend on the
// excluded dev/build plugins.
const shared = {
	plugins: [viteReact()],
	resolve: {
		alias: [
			{ find: /^#\//, replacement: `${src}/` },
			{ find: /^@\//, replacement: `${src}/` },
		],
	},
};

export default defineConfig({
	test: {
		projects: [
			{
				...shared,
				test: {
					name: "unit",
					setupFiles,
					exclude: [...configDefaults.exclude, INTEGRATION_GLOB],
				},
			},
			{
				...shared,
				test: {
					name: "integration",
					setupFiles,
					include: [INTEGRATION_GLOB],
				},
			},
		],
	},
});
