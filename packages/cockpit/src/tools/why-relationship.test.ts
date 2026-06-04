// Unit tests for why_relationship's pure data assembly (DAT-409). No DB, no LLM —
// the Drizzle reads + the Anthropic synthesis are smoke-covered; here we pin the
// per-intent driver pass-through, evidence dimension_path + detail rendering,
// signal_count, and the found/analyzed distinctions.

import { describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the metadata client + the anthropic
// adapter; mock the env-dependent ones (sets no process.env — see registry.test.ts).
vi.mock("#/config", () => ({ config: { anthropicApiKey: "test" } }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
	projectWhyRelationship,
	type RelEndpoints,
	type WhyRelEvidenceRow,
	type WhyRelReadinessRow,
} from "./why-relationship";

const FROM = "c_from";
const TO = "c_to";

const endpoints: RelEndpoints = {
	fromTableName: "payments",
	fromColumnName: "invoice_id",
	toTableName: "invoices",
	toColumnName: "invoice_id",
};

function readiness(
	overrides: Partial<WhyRelReadinessRow> = {},
): WhyRelReadinessRow {
	return {
		band: "investigate",
		worstIntentRisk: 0.42,
		intents: [
			{
				intent: "aggregation_intent",
				band: "investigate",
				risk: 0.42,
				drivers: [
					{
						node: "referential_integrity",
						dimension_path: "structural.relations.referential_integrity",
						label: "Referential Integrity",
						state: "high",
						impact_delta: 0.3,
					},
				],
			},
		],
		...overrides,
	};
}

function evidenceRow(
	overrides: Partial<WhyRelEvidenceRow> = {},
): WhyRelEvidenceRow {
	return {
		layer: "structural",
		dimension: "relations",
		subDimension: "referential_integrity",
		score: 0.45,
		detectorId: "relationship_entropy",
		evidence: { left_referential_integrity: 80.06, orphan_count: 200 },
		...overrides,
	};
}

describe("projectWhyRelationship (DAT-409)", () => {
	it("assembles pair, endpoints, per-intent drivers, and correlated evidence", () => {
		const out = projectWhyRelationship(
			FROM,
			TO,
			endpoints,
			readiness(),
			[evidenceRow()],
			0,
		);
		expect(out.from_column_id).toBe(FROM);
		expect(out.to_column_id).toBe(TO);
		expect(out.from_table_name).toBe("payments");
		expect(out.to_table_name).toBe("invoices");
		expect(out.found).toBe(true);
		expect(out.band).toBe("investigate");
		expect(out.analyzed).toBe(true);
		// why_relationship KEEPS the full per-intent drivers (look_relationships drops them).
		expect(out.intents[0].drivers).toHaveLength(1);
		expect(out.intents[0].drivers[0].label).toBe("Referential Integrity");
		// Evidence: dimension_path composed layer.dimension.subDimension; detail = JSON.
		expect(out.evidence[0].dimension_path).toBe(
			"structural.relations.referential_integrity",
		);
		expect(out.evidence[0].detector_id).toBe("relationship_entropy");
		expect(out.evidence[0].detail).toContain("orphan_count");
		expect(out.signal_count).toBe(1);
	});

	it("treats a missing readiness row with evidence as found-but-unanalyzed", () => {
		const out = projectWhyRelationship(
			FROM,
			TO,
			endpoints,
			null,
			[evidenceRow()],
			0,
		);
		expect(out.found).toBe(true); // evidence exists
		expect(out.analyzed).toBe(false); // but no band
		expect(out.band).toBeNull();
		expect(out.intents).toEqual([]);
		expect(out.signal_count).toBe(1);
	});

	it("treats no readiness and no evidence as not-found", () => {
		const out = projectWhyRelationship(FROM, TO, endpoints, null, [], 0);
		expect(out.found).toBe(false);
		expect(out.analyzed).toBe(false);
		expect(out.evidence).toEqual([]);
	});

	it("degrades a malformed intents blob to empty rather than throwing", () => {
		const out = projectWhyRelationship(
			FROM,
			TO,
			endpoints,
			readiness({ intents: "garbage" }),
			[],
			2,
		);
		expect(out.intents).toEqual([]);
		expect(out.band).toBe("investigate"); // scalar still comes through
		expect(out.pending_teaches).toBe(2);
	});
});
