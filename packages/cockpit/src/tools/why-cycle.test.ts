// Unit tests for the why_cycle projection (DAT-465). Pure — no DB; the live read
// path is covered by the operating_model integration smoke.
//
// What this guards: the found discriminant, the verbatim (digest-sanitized)
// reason/name/description pass-through, the structural completion fields, and
// unknown-shape JSON (grounded_against / stages / entity_flows / evidence)
// rendering through the shared evidence sanitizer — never assumed.

import { describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the Postgres metadata client; mock both
// (with the `#/` alias — relative specifiers silently don't intercept) so the
// pure projection runs with no env and no connection.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
	projectWhyCycle,
	type WhyCycleArtifactRow,
	type WhyCycleDetectionRow,
} from "./why-cycle";

const D1 = "204bc8e118543a6c35654c1f68c43539a2e226f2";

const executedArtifact: WhyCycleArtifactRow = {
	state: "executed",
	stateReason: null,
	strictness: 0.8,
	groundedAgainst: { detect: "run-7", typing: "run-3" },
};

const executedDetection: WhyCycleDetectionRow = {
	cycleName: "Order-to-Cash Cycle",
	isKnownType: true,
	family: null,
	direction: null,
	businessValue: "high",
	confidence: 0.92,
	description: "Revenue cycle from order through collection.",
	completionRate: 0.82,
	completedCycles: 41,
	totalRecords: 50,
	statusTable: "invoices",
	statusColumn: "status",
	completionValue: "paid",
	stages: [{ name: "Order Placed", order: 1 }],
	entityFlows: [{ entity: "customer" }],
	tablesInvolved: ["invoices", "payments"],
	evidence: { signal: "status column present" },
};

describe("projectWhyCycle (DAT-465)", () => {
	it("assembles the executed drill-down: state + completion + detection detail", () => {
		const projected = projectWhyCycle(
			"order_to_cash",
			executedArtifact,
			executedDetection,
			2,
		);

		expect(projected).toEqual({
			canonical_type: "order_to_cash",
			found: true,
			cycle_name: "Order-to-Cash Cycle",
			state: "executed",
			state_reason: null,
			strictness: 0.8,
			grounded_against: JSON.stringify({ detect: "run-7", typing: "run-3" }),
			is_known_type: true,
			family: null,
			direction: null,
			business_value: "high",
			confidence: 0.92,
			description: "Revenue cycle from order through collection.",
			completion_rate: 0.82,
			completed_cycles: 41,
			total_records: 50,
			status_table: "invoices",
			status_column: "status",
			completion_value: "paid",
			stages: JSON.stringify([{ name: "Order Placed", order: 1 }]),
			entity_flows: JSON.stringify([{ entity: "customer" }]),
			tables_involved: JSON.stringify(["invoices", "payments"]),
			evidence: JSON.stringify({ signal: "status column present" }),
			pending_teaches: 2,
		});
	});

	it("surfaces a family cycle's undetermined direction verbatim (DAT-856)", () => {
		const projected = projectWhyCycle(
			"settlement",
			{
				state: "executed",
				stateReason: null,
				strictness: null,
				groundedAgainst: null,
			},
			{
				...executedDetection,
				cycleName: "Settlement Cycle",
				family: "settlement",
				direction: "undetermined",
			},
			0,
		);
		// The detected-but-undirected state is surfaced as exactly that, never coerced.
		expect(projected.family).toBe("settlement");
		expect(projected.direction).toBe("undetermined");
	});

	it("found=false for an unknown cycle — all fields null/empty, nothing invented", () => {
		const projected = projectWhyCycle("nope", null, null, 0);

		expect(projected.found).toBe(false);
		expect(projected.state).toBeNull();
		expect(projected.state_reason).toBeNull();
		expect(projected.cycle_name).toBeNull();
		expect(projected.completion_rate).toBeNull();
		expect(projected.status_table).toBeNull();
		expect(projected.grounded_against).toBe("");
		expect(projected.stages).toBe("");
		expect(projected.entity_flows).toBe("");
		expect(projected.tables_involved).toBe("");
		expect(projected.evidence).toBe("");
	});

	it("a declared-but-not-detected cycle is found, with the reason verbatim", () => {
		const projected = projectWhyCycle(
			"subscription_renewal",
			{
				state: "declared",
				stateReason: "not detected in this workspace",
				strictness: null,
				groundedAgainst: null,
			},
			null,
			0,
		);

		expect(projected.found).toBe(true);
		expect(projected.state).toBe("declared");
		expect(projected.state_reason).toBe("not detected in this workspace");
		// Never grounded → empty grounding render; no detection fields invented.
		expect(projected.grounded_against).toBe("");
		expect(projected.cycle_name).toBeNull();
		expect(projected.completion_rate).toBeNull();
	});

	it("renders narrow names in reason/name/description/status; stays digest-free (DAT-639)", () => {
		const projected = projectWhyCycle(
			"inventory_cycle",
			{
				state: "executed",
				stateReason: `measured on inventory`,
				strictness: null,
				// `_`-prefixed engine plumbing keys are dropped by the sanitizer.
				groundedAgainst: { _detect: `src_${D1}`, detect: "run-9" },
			},
			{
				cycleName: `Flow over inventory`,
				isKnownType: false,
				family: null,
				direction: null,
				businessValue: "low",
				confidence: 0.5,
				description: `Cycle through inventory`,
				completionRate: 0.6,
				completedCycles: 6,
				totalRecords: 10,
				statusTable: `inventory`,
				statusColumn: "state",
				completionValue: "shipped",
				stages: [{ table: `inventory` }],
				entityFlows: null,
				tablesInvolved: [`inventory`],
				evidence: { table: `inventory` },
			},
			0,
		);

		expect(JSON.stringify(projected)).not.toMatch(/src_[0-9a-f]{40}/);
		expect(projected.state_reason).toBe("measured on inventory");
		expect(projected.cycle_name).toBe("Flow over inventory");
		expect(projected.description).toBe("Cycle through inventory");
		expect(projected.status_table).toBe("inventory");
		expect(projected.grounded_against).toBe(
			JSON.stringify({ detect: "run-9" }),
		);
		expect(projected.tables_involved).toBe(JSON.stringify(["inventory"]));
	});
});
