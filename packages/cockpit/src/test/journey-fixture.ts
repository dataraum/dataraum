// What the journey suite imports: the journey workspace's DSNs, and the one
// call that points `config` at them — catalog AND lake together.
//
// Same ordering contract as `fixture.ts`: call at module top level, then
// dynamic-import the routes inside `beforeAll`. `config.ts` parses its Zod
// schema at module eval, so a static import would read the environment before
// these overrides are installed and the suite would run against placeholders.
//
// The lake overrides are the point. `DUCKLAKE_CATALOG_URL` + `DATARAUM_LAKE_PATH`
// are all it takes to aim the cockpit's REAL `lake.ts` bootstrap at the fixture
// lake — no module is mocked, so the ATTACH, the extension load and the engine
// scope (`USE lake.typed`) all execute as they do in production.

import { inject } from "vitest";

import { applyIntegrationEnv, suiteTitle } from "./integration-env";
import { JOURNEY_WORKSPACE_ID } from "./seed-journey";

export interface JourneyContext {
	available: boolean;
	skipReason: string | null;
	describeName(title: string): string;
}

export function attachJourneyWorkspace(): JourneyContext {
	const handle = inject("journeyWorkspace");
	const skipReason = inject("journeySkipReason");
	const fixture = inject("fixtureWorkspace");

	if (handle) {
		applyIntegrationEnv({
			// The journey catalog lives in its OWN database; both metadata roles
			// point at it (the fixture does not model the reader/writer split).
			METADATA_DATABASE_URL: handle.metadataUrl,
			METADATA_WRITER_DATABASE_URL: handle.metadataUrl,
			// cockpit_db is shared and unused by these journeys, but config
			// requires it to parse.
			...(fixture ? { COCKPIT_DATABASE_URL: fixture.cockpitUrl } : {}),
			// Identity ties the triple together: it selects the snippets'
			// schema_mapping_id AND the lake's DuckLake METADATA_SCHEMA.
			DATARAUM_WORKSPACE_ID: JOURNEY_WORKSPACE_ID,
			// The real lake. A local DATA_PATH rather than `s3://`, so the S3
			// secret registered during bootstrap is scoped to a bucket nothing
			// reads and stays inert.
			DUCKLAKE_CATALOG_URL: handle.lakeCatalogUrl,
			DATARAUM_LAKE_PATH: handle.lakeDataPath,
		});
	}

	return {
		available: !!handle,
		skipReason: skipReason ?? null,
		describeName: (title: string) =>
			suiteTitle(title, handle ? null : (skipReason ?? "no journey workspace")),
	};
}
