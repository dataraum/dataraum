// Unit tests for look_relationships' pure row→shape projection (DAT-409) + the
// catalog-facts union (DAT-478). No DB — the Drizzle reads are smoke-covered; here
// we pin the target→pair parsing, the endpoint-name resolution (and its
// degrade-to-null miss), the JSONB parsing, the top-driver cap, the
// non-relationship-target guard, and the full-outer union of bands ⟗ catalog facts
// (matched, bands-only, catalog-only).

import { describe, expect, it, vi } from "vitest";

// Importing the tool transitively pulls config.ts + the metadata client. Mock
// both so this pure-helper test needs no env and opens no connection (sets no
// process.env — see registry.test.ts).
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
	type ColumnNameLookup,
	projectRelationshipReadiness,
	type RelationshipCatalogRow,
	type RelationshipReadinessRow,
	unionRelationships,
} from "./look-relationships";

const FROM = "c_from";
const TO = "c_to";

function catalogRow(
	overrides: Partial<RelationshipCatalogRow> = {},
): RelationshipCatalogRow {
	return {
		fromColumnId: FROM,
		toColumnId: TO,
		relationshipType: "foreign_key",
		cardinality: "many_to_one",
		confidence: 0.91,
		// Real `detection_method` values are `candidate | llm | manual | keeper`
		// (engine `relationships/db_models.py`). A confirmed FK is `llm`.
		detectionMethod: "llm",
		isConfirmed: true,
		...overrides,
	};
}

function row(
	overrides: Partial<RelationshipReadinessRow> = {},
): RelationshipReadinessRow {
	return {
		target: `relationship:${FROM}::${TO}`,
		band: "investigate",
		worstIntentRisk: 0.42,
		intents: [
			{ intent: "query_intent", band: "ready", risk: 0.1, drivers: [] },
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
		topDrivers: [
			{
				node: "referential_integrity",
				dimension_path: "structural.relations.referential_integrity",
				label: "Referential Integrity",
				state: "high",
				impact_delta: 0.3,
			},
		],
		...overrides,
	};
}

function names(): ColumnNameLookup {
	return new Map([
		[FROM, { columnName: "invoice_id", tableName: "payments" }],
		[TO, { columnName: "invoice_id", tableName: "invoices" }],
	]);
}

describe("projectRelationshipReadiness (DAT-409)", () => {
	it("projects the pair, endpoint names, per-intent bands, and top drivers", () => {
		const out = projectRelationshipReadiness(row(), names());
		expect(out).not.toBeNull();
		if (!out) return;
		expect(out.from_column_id).toBe(FROM);
		expect(out.to_column_id).toBe(TO);
		expect(out.from_table_name).toBe("payments");
		expect(out.from_column_name).toBe("invoice_id");
		expect(out.to_table_name).toBe("invoices");
		expect(out.to_column_name).toBe("invoice_id");
		expect(out.band).toBe("investigate");
		expect(out.worst_intent_risk).toBe(0.42);
		// Per-intent overview carries band + risk only (drivers are why_relationship).
		expect(out.intents).toEqual([
			{ intent: "query_intent", band: "ready", risk: 0.1 },
			{ intent: "aggregation_intent", band: "investigate", risk: 0.42 },
		]);
		expect(out.top_drivers).toEqual([
			{ label: "Referential Integrity", state: "high", impact_delta: 0.3 },
		]);
		// No catalog row passed — the facts default to null (a bands-only row).
		expect(out.relationship_type).toBeNull();
		expect(out.cardinality).toBeNull();
		expect(out.confidence).toBeNull();
		expect(out.detection_method).toBeNull();
		expect(out.is_confirmed).toBeNull();
	});

	it("joins catalog facts when a matching catalog row is supplied (DAT-478)", () => {
		const out = projectRelationshipReadiness(row(), names(), catalogRow());
		expect(out).not.toBeNull();
		if (!out) return;
		// Bands unchanged…
		expect(out.band).toBe("investigate");
		expect(out.intents).toHaveLength(2);
		// …and the catalog facts ride alongside.
		expect(out.relationship_type).toBe("foreign_key");
		expect(out.cardinality).toBe("many_to_one");
		expect(out.confidence).toBe(0.91);
		expect(out.detection_method).toBe("llm");
		expect(out.is_confirmed).toBe(true);
	});

	it("returns null for a non-relationship target (defensive guard)", () => {
		expect(
			projectRelationshipReadiness(row({ target: "table:t1" }), names()),
		).toBeNull();
	});

	it("degrades a missing endpoint name to null rather than dropping the row", () => {
		const out = projectRelationshipReadiness(row(), new Map());
		expect(out).not.toBeNull();
		if (!out) return;
		expect(out.from_table_name).toBeNull();
		expect(out.from_column_name).toBeNull();
		expect(out.to_table_name).toBeNull();
		// The pair + band still come through — the relationship is real.
		expect(out.from_column_id).toBe(FROM);
		expect(out.band).toBe("investigate");
	});

	it("caps top drivers at 3", () => {
		const many = Array.from({ length: 6 }, (_, i) => ({
			node: `n${i}`,
			dimension_path: `p.${i}`,
			label: `L${i}`,
			state: "high",
			impact_delta: 0.5 - i * 0.01,
		}));
		const out = projectRelationshipReadiness(
			row({ topDrivers: many }),
			names(),
		);
		expect(out?.top_drivers).toHaveLength(3);
		expect(out?.top_drivers.map((d) => d.label)).toEqual(["L0", "L1", "L2"]);
	});

	it("degrades a malformed JSONB blob to empty rather than throwing", () => {
		const out = projectRelationshipReadiness(
			row({ intents: { not: "an array" }, topDrivers: "garbage" }),
			names(),
		);
		expect(out?.intents).toEqual([]);
		expect(out?.top_drivers).toEqual([]);
		expect(out?.band).toBe("investigate");
	});

	it("strips the content-keyed `src_<digest>__` prefix from endpoint table names (DAT-431)", () => {
		// This result goes back to the agent — never the hash form. The drill-down
		// round-trip (why_relationship) keys on the column ids, which pass through raw.
		const lookup: ColumnNameLookup = new Map([
			[
				FROM,
				{
					columnName: "invoice_id",
					tableName: "src_204bc8e118543a6c35654c1f68c43539a2e226f2__payments",
				},
			],
			[
				TO,
				{
					columnName: "invoice_id",
					tableName: "src_3cb4f3325aa757324f32b2dbe30b4ca5a55a8b50__invoices",
				},
			],
		]);
		const out = projectRelationshipReadiness(row(), lookup);
		expect(out?.from_table_name).toBe("payments");
		expect(out?.to_table_name).toBe("invoices");
		expect(out?.from_column_id).toBe(FROM);
		expect(out?.to_column_id).toBe(TO);
	});
});

describe("unionRelationships (DAT-478)", () => {
	const OTHER_FROM = "c_other_from";
	const OTHER_TO = "c_other_to";

	function wideNames(): ColumnNameLookup {
		return new Map([
			[FROM, { columnName: "invoice_id", tableName: "payments" }],
			[TO, { columnName: "invoice_id", tableName: "invoices" }],
			[OTHER_FROM, { columnName: "customer_id", tableName: "orders" }],
			[OTHER_TO, { columnName: "customer_id", tableName: "customers" }],
		]);
	}

	it("matches a readiness row to its catalog row by the directional pair", () => {
		const out = unionRelationships([row()], [catalogRow()], wideNames());
		expect(out).toHaveLength(1);
		const rel = out[0];
		// Bands…
		expect(rel.band).toBe("investigate");
		expect(rel.intents).toHaveLength(2);
		// …and catalog facts on the SAME row.
		expect(rel.relationship_type).toBe("foreign_key");
		expect(rel.cardinality).toBe("many_to_one");
		expect(rel.confidence).toBe(0.91);
		expect(rel.is_confirmed).toBe(true);
	});

	it("surfaces a bands-only readiness row (no catalog match) with null facts", () => {
		// Catalog covers a DIFFERENT pair — the readiness row must still surface.
		const out = unionRelationships(
			[row()],
			[catalogRow({ fromColumnId: OTHER_FROM, toColumnId: OTHER_TO })],
			wideNames(),
		);
		const bands = out.find((r) => r.from_column_id === FROM);
		expect(bands).toBeDefined();
		expect(bands?.band).toBe("investigate");
		expect(bands?.relationship_type).toBeNull();
		expect(bands?.cardinality).toBeNull();
		expect(bands?.confidence).toBeNull();
		expect(bands?.is_confirmed).toBeNull();
	});

	it("surfaces a catalog-only relationship (no readiness row) with null bands", () => {
		// Readiness covers a DIFFERENT pair — the catalog relationship must still surface.
		const out = unionRelationships(
			[row({ target: `relationship:${OTHER_FROM}::${OTHER_TO}` })],
			[catalogRow()],
			wideNames(),
		);
		const catalogOnly = out.find((r) => r.from_column_id === FROM);
		expect(catalogOnly).toBeDefined();
		// Facts present…
		expect(catalogOnly?.relationship_type).toBe("foreign_key");
		expect(catalogOnly?.is_confirmed).toBe(true);
		// …bands/intents null — never dropped.
		expect(catalogOnly?.band).toBeNull();
		expect(catalogOnly?.worst_intent_risk).toBeNull();
		expect(catalogOnly?.intents).toEqual([]);
		expect(catalogOnly?.top_drivers).toEqual([]);
		// Endpoints still resolved for the catalog-only side.
		expect(catalogOnly?.from_table_name).toBe("payments");
		expect(catalogOnly?.to_column_name).toBe("invoice_id");
	});

	it("keeps both sides — readiness order first, catalog-only appended", () => {
		const out = unionRelationships(
			[row()],
			[
				catalogRow(),
				catalogRow({ fromColumnId: OTHER_FROM, toColumnId: OTHER_TO }),
			],
			wideNames(),
		);
		expect(out).toHaveLength(2);
		// The matched readiness row leads (its query order is preserved)…
		expect(out[0].from_column_id).toBe(FROM);
		expect(out[0].band).toBe("investigate");
		// …then the catalog-only relationship the readiness pass didn't cover.
		expect(out[1].from_column_id).toBe(OTHER_FROM);
		expect(out[1].band).toBeNull();
		expect(out[1].relationship_type).toBe("foreign_key");
	});

	it("skips a catalog row with a missing endpoint id (no stable pair key)", () => {
		const out = unionRelationships(
			[],
			[catalogRow({ toColumnId: null })],
			wideNames(),
		);
		expect(out).toEqual([]);
	});

	it("drops a readiness row whose target isn't a relationship key", () => {
		const out = unionRelationships(
			[row({ target: "table:t1" })],
			[catalogRow({ detectionMethod: "llm" })],
			wideNames(),
		);
		// The non-relationship readiness row is dropped, but its (llm) catalog match
		// still surfaces catalog-only (the pair is valid + defined in the catalog).
		expect(out).toHaveLength(1);
		expect(out[0].from_column_id).toBe(FROM);
		expect(out[0].band).toBeNull();
		expect(out[0].relationship_type).toBe("foreign_key");
	});

	it("picks the llm/confirmed row when a pair has BOTH a candidate and an llm row (deterministic winner)", () => {
		// One promoted run carries multiple rows per directional pair — a structural
		// `candidate` (is_confirmed=false) AND the `llm` row the selector confirmed
		// (uniqueness includes detection_method). The higher-precedence row (llm beats
		// candidate) must win regardless of input order, so the surfaced facts are
		// deterministic — and confidence does NOT flip it (the candidate's is higher).
		const candidate = catalogRow({
			detectionMethod: "candidate",
			isConfirmed: false,
			confidence: 0.99, // higher confidence must NOT beat the higher-precedence method
			relationshipType: "structural_candidate",
			cardinality: "unknown",
		});
		const llm = catalogRow({
			detectionMethod: "llm",
			isConfirmed: true,
			confidence: 0.8,
			relationshipType: "foreign_key",
			cardinality: "many_to_one",
		});
		// Both orderings resolve to the same winner.
		for (const catalog of [
			[candidate, llm],
			[llm, candidate],
		]) {
			const out = unionRelationships([row()], catalog, wideNames());
			expect(out).toHaveLength(1);
			expect(out[0].detection_method).toBe("llm");
			expect(out[0].is_confirmed).toBe(true);
			expect(out[0].relationship_type).toBe("foreign_key");
			expect(out[0].cardinality).toBe("many_to_one");
			expect(out[0].confidence).toBe(0.8);
		}
	});

	it("picks `manual` over `llm` regardless of confidence — precedence dominates (both orderings)", () => {
		// The engine's readiness pass picks the representative by METHOD PRECEDENCE
		// (`manual > keeper > llm > candidate`), NOT confidence — so the facts we join
		// onto that band must come from the same row. `manual(0.7)` must beat `llm(0.9)`
		// even though the llm confidence is higher, or the facts contradict the band.
		const manual = catalogRow({
			detectionMethod: "manual",
			confidence: 0.7,
			relationshipType: "foreign_key",
			cardinality: "one_to_one",
		});
		const llm = catalogRow({
			detectionMethod: "llm",
			confidence: 0.9, // higher confidence must NOT beat the higher-precedence method
			relationshipType: "association",
			cardinality: "many_to_one",
		});
		for (const catalog of [
			[manual, llm],
			[llm, manual],
		]) {
			const out = unionRelationships([row()], catalog, wideNames());
			expect(out).toHaveLength(1);
			expect(out[0].detection_method).toBe("manual");
			expect(out[0].confidence).toBe(0.7);
			expect(out[0].relationship_type).toBe("foreign_key");
			expect(out[0].cardinality).toBe("one_to_one");
		}
	});

	it("picks `keeper` over `llm` regardless of confidence — precedence dominates (both orderings)", () => {
		// keeper outranks llm in the engine's precedence map, so a `keeper(0.6)` row
		// wins over an `llm(0.95)` row — confidence never crosses the method boundary.
		const keeper = catalogRow({
			detectionMethod: "keeper",
			confidence: 0.6,
			relationshipType: "foreign_key",
			cardinality: "one_to_one",
		});
		const llm = catalogRow({
			detectionMethod: "llm",
			confidence: 0.95, // higher confidence must NOT beat the higher-precedence method
			relationshipType: "association",
			cardinality: "many_to_one",
		});
		for (const catalog of [
			[keeper, llm],
			[llm, keeper],
		]) {
			const out = unionRelationships([row()], catalog, wideNames());
			expect(out).toHaveLength(1);
			expect(out[0].detection_method).toBe("keeper");
			expect(out[0].confidence).toBe(0.6);
			expect(out[0].relationship_type).toBe("foreign_key");
			expect(out[0].cardinality).toBe("one_to_one");
		}
	});

	it("prefers the higher-confidence row among same-method rows (intra-method tiebreak ONLY)", () => {
		// Confidence is the tiebreak only WITHIN one detection_method — two `llm` rows
		// on the same pair resolve to the higher-confidence one. (Across methods,
		// precedence dominates — see the manual/keeper-over-llm tests above.)
		const lo = catalogRow({ detectionMethod: "llm", confidence: 0.6 });
		const hi = catalogRow({
			detectionMethod: "llm",
			confidence: 0.95,
			cardinality: "one_to_one",
		});
		const out = unionRelationships([row()], [lo, hi], wideNames());
		expect(out).toHaveLength(1);
		expect(out[0].detection_method).toBe("llm");
		expect(out[0].confidence).toBe(0.95);
		expect(out[0].cardinality).toBe("one_to_one");
	});

	it("excludes a candidate-only pair with no readiness row (readiness contract)", () => {
		// A bare structural `candidate` the LLM never confirmed, with no band row, is
		// NOT a catalog relationship — the engine scores `detection_method != 'candidate'`,
		// so it must not surface as a catalog-only relationship.
		const out = unionRelationships(
			[],
			[catalogRow({ detectionMethod: "candidate", isConfirmed: false })],
			wideNames(),
		);
		expect(out).toEqual([]);
	});

	it("surfaces a candidate pair that DOES have a readiness band row (bands lead, candidate facts ride)", () => {
		// The exclusion is candidate-only-AND-no-band. When a readiness row exists for
		// the pair, the (candidate) facts still ride on it — the row is real.
		const out = unionRelationships(
			[row()],
			[catalogRow({ detectionMethod: "candidate", isConfirmed: false })],
			wideNames(),
		);
		expect(out).toHaveLength(1);
		expect(out[0].band).toBe("investigate");
		expect(out[0].detection_method).toBe("candidate");
	});
});
