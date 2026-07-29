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
//       - `integration` (`bun run test:integration`): the real-infrastructure
//         tests (`*.integration.test.*`). Keeping them in a separate project
//         means the default run never loads them at all. Two flavours live
//         here:
//           · fixture-backed — a throwaway Postgres carrying the ENGINE's
//             generated schema, booted once per run by ./src/test/global-setup.ts.
//             Needs only a docker daemon; skips loudly without one.
//           · legacy stack-gated — older suites written against a seeded
//             compose stack, gated on `providedByEnvironment(...)` and skipped
//             unless the environment supplies real DSNs.
//
//         This project MUST run under `bun --bun` (see package.json): the
//         engine metadata client imports `SQL` from "bun", which cannot load
//         under Node at all.

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
					// Boots the fixture workspace once per run (a throwaway Postgres
					// carrying the engine's generated schema) and provides its DSNs.
					// Suites that don't need it are unaffected; suites that do skip
					// loudly when docker is unavailable. See ./src/test/global-setup.ts.
					globalSetup: ["./src/test/global-setup.ts"],
					// The fixture boot + cockpit_db migrations dominate a cold run.
					hookTimeout: 120_000,
					testTimeout: 60_000,
				},
			},
		],
	},
});
