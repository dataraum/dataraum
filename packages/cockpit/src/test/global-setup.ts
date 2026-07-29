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

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { TestProject } from "vitest/node";

import {
	applyCockpitMigrations,
	dockerUnavailableReason,
	type FixtureWorkspace,
	startFixtureWorkspace,
	stopFixtureWorkspace,
} from "./fixture-workspace";
import { TEST_WORKSPACE_ID } from "./integration-env";
import { buildJourneyLake, findCorpusDir } from "./journey-lake";
import {
	catalogSeedSql,
	graphSnippetSeedSql,
	metricArtifactSeedSql,
} from "./seed-catalog";
import { JOURNEY_DB, journeySeedSql } from "./seed-journey";

export interface FixtureHandle {
	metadataUrl: string;
	cockpitUrl: string;
}

/** The journey workspace: its own metadata database AND its own real DuckLake
 *  lake, both inside the shared fixture container. Null when the corpus is not
 *  checked out — the journeys assert figures derived from it, so there is
 *  nothing honest to run without it. */
export interface JourneyHandle {
	metadataUrl: string;
	lakeCatalogUrl: string;
	lakeDataPath: string;
}

declare module "vitest" {
	interface ProvidedContext {
		fixtureWorkspace: FixtureHandle | null;
		fixtureSkipReason: string | null;
		journeyWorkspace: JourneyHandle | null;
		journeySkipReason: string | null;
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
		provide("journeyWorkspace", null);
		provide(
			"journeySkipReason",
			`journey workspace needs docker — ${unavailable}`,
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

	// --- the journey workspace: real catalog + real lake over the real corpus ---
	//
	// A MISSING CORPUS IS A SKIP, a failing BUILD is not. The journeys assert
	// figures derived from `dataraum-testdata`, which is a sibling repo and may
	// simply not be checked out; running them against anything else would assert
	// nothing. But once the corpus IS present, a failure to build the lake or
	// seed the catalog is OUR bug and must be loud — the same posture the shared
	// fixture takes.
	let lakeDir: string | null = null;
	const corpusDir = findCorpusDir();
	if (!corpusDir) {
		provide("journeyWorkspace", null);
		provide(
			"journeySkipReason",
			"dataraum-testdata corpus not found — set DATARAUM_TESTDATA_PATH or check " +
				"out the sibling repo (the journeys assert figures derived from it)",
		);
		console.warn(
			"\n[journey-workspace] SKIPPING the J1-J8 journey suite: corpus not found.\n",
		);
	} else {
		try {
			fixture.psql(journeySeedSql(), JOURNEY_DB);
			lakeDir = mkdtempSync(join(tmpdir(), "dataraum-journey-lake-"));
			await buildJourneyLake({
				catalogLibpq: fixture.journeyLakeCatalogLibpq,
				dataPath: join(lakeDir, "data"),
				corpusDir,
			});
		} catch (err) {
			throw new Error(
				`journey workspace failed to build: ${(err as Error).message}`,
			);
		}
		provide("journeyWorkspace", {
			metadataUrl: fixture.journeyUrl,
			lakeCatalogUrl: fixture.journeyLakeCatalogUrl,
			lakeDataPath: join(lakeDir, "data"),
		});
		provide("journeySkipReason", null);
	}

	return () => {
		stopFixtureWorkspace(fixture.containerId);
		if (lakeDir) rmSync(lakeDir, { recursive: true, force: true });
	};
}
