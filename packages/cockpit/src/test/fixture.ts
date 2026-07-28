// What a fixture-backed suite imports: the injected DSNs, plus the one call
// that points `config` at them.
//
// Usage — the ORDER matters, because config.ts parses at module eval:
//
//   const fx = attachFixtureWorkspace();
//   describe.skipIf(!fx.available)(fx.describeName("my suite"), () => {
//     let subject: typeof import("../thing");
//     beforeAll(async () => { subject = await import("../thing"); });
//   });
//
// `attachFixtureWorkspace()` runs at module top level and applies the env
// overrides immediately, so the dynamic import inside `beforeAll` sees a
// `config` already pointed at the fixture.

import { inject } from "vitest";

import {
	applyIntegrationEnv,
	suiteTitle,
	TEST_WORKSPACE_ID,
} from "./integration-env";

export interface FixtureContext {
	available: boolean;
	/** Non-null when `available`. */
	metadataUrl: string | null;
	cockpitUrl: string | null;
	/** Why the fixture is unavailable, for a loud skip. */
	skipReason: string | null;
	/**
	 * Suite title carrying the skip reason, so a skipped run SAYS why in the
	 * reporter instead of showing a bare, easily-ignored "skipped".
	 */
	describeName(title: string): string;
}

/**
 * Read the fixture DSNs and point `config` at them.
 *
 * Call at test-module top level, before importing the module under test.
 */
export function attachFixtureWorkspace(): FixtureContext {
	const handle = inject("fixtureWorkspace");
	const skipReason = inject("fixtureSkipReason");

	if (handle) {
		applyIntegrationEnv({
			// Both metadata roles point at the same fixture DB. Production splits
			// them by ROLE search_path (reader → ws_<id>_read, writer → ws_<id>);
			// the fixture puts the read views in `public` and the raw tables in
			// `engine`, matching what the Drizzle mirror was introspected from
			// (pull-metadata.sh). Reads therefore resolve unqualified to the views
			// exactly as in production; test writes name `engine.<table>`.
			METADATA_DATABASE_URL: handle.metadataUrl,
			METADATA_WRITER_DATABASE_URL: handle.metadataUrl,
			COCKPIT_DATABASE_URL: handle.cockpitUrl,
			DATARAUM_WORKSPACE_ID: TEST_WORKSPACE_ID,
		});
	}

	return {
		available: !!handle,
		metadataUrl: handle?.metadataUrl ?? null,
		cockpitUrl: handle?.cockpitUrl ?? null,
		skipReason: skipReason ?? null,
		describeName: (title: string) =>
			suiteTitle(title, handle ? null : (skipReason ?? "no fixture")),
	};
}
