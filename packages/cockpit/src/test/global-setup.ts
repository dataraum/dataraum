// Boots ONE fixture workspace for the whole integration run and hands its
// DSNs to the suites through vitest's provide/inject channel.
//
// provide/inject rather than process.env: globalSetup runs in the vitest main
// process, and mutations there do not reach the worker processes that execute
// test files. Each suite injects the DSNs and passes them to
// applyIntegrationEnv() as overrides, BEFORE dynamically importing the module
// under test — config.ts parses at module eval, so the order is load-bearing.
//
// One container, not one per file: a per-file container would multiply a ~6s
// boot across every suite, and the fixture is read-mostly with per-suite
// workspace/run ids keeping writers apart.
//
// Docker missing is a SKIP, never a silent pass and never a hard failure: the
// reason is provided alongside, and every fixture-backed suite prints it. A
// harness that quietly reports green without its infrastructure would be
// worse than no harness — that is the exact failure mode this lane exists to
// remove.

import type { TestProject } from "vitest/node";

import {
	applyCockpitMigrations,
	dockerUnavailableReason,
	type FixtureWorkspace,
	startFixtureWorkspace,
	stopFixtureWorkspace,
} from "./fixture-workspace";
import { TEST_WORKSPACE_ID } from "./integration-env";
import {
	catalogSeedSql,
	graphSnippetSeedSql,
	metricArtifactSeedSql,
} from "./seed-catalog";

export interface FixtureHandle {
	metadataUrl: string;
	cockpitUrl: string;
}

declare module "vitest" {
	interface ProvidedContext {
		fixtureWorkspace: FixtureHandle | null;
		fixtureSkipReason: string | null;
	}
}

export default async function setup({ provide }: TestProject) {
	const unavailable = dockerUnavailableReason();
	if (unavailable) {
		provide("fixtureWorkspace", null);
		provide(
			"fixtureSkipReason",
			`fixture workspace unavailable — ${unavailable}`,
		);
		console.warn(
			`\n[fixture-workspace] SKIPPING all fixture-backed integration suites: ${unavailable}\n`,
		);
		return () => {};
	}

	let fixture: FixtureWorkspace;
	try {
		fixture = startFixtureWorkspace();
		applyCockpitMigrations(fixture.cockpitUrl);
		// One canonical, head-promoted catalog for every fixture-backed suite.
		fixture.psql(catalogSeedSql());
		fixture.psql(metricArtifactSeedSql());
		// Snippets are workspace-scoped by schema_mapping_id — it must match the
		// boot identity the suites run under or the loader reads zero rows.
		fixture.psql(graphSnippetSeedSql(TEST_WORKSPACE_ID));
	} catch (err) {
		// A fixture that fails to BUILD is a real failure, not a skip: docker is
		// present, so this is our seeding going wrong (e.g. engine schema.sql no
		// longer applies) and must be loud.
		throw new Error(
			`fixture workspace failed to build: ${(err as Error).message}`,
		);
	}

	provide("fixtureWorkspace", {
		metadataUrl: fixture.metadataUrl,
		cockpitUrl: fixture.cockpitUrl,
	});
	provide("fixtureSkipReason", null);

	return () => {
		stopFixtureWorkspace(fixture.containerId);
	};
}
