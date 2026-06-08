// Unit tests for the look_cycle projection (DAT-465). Pure — no DB; the live
// read path (head check + current_* views) is covered by the operating_model
// integration smoke (scripts/smoke-operating-model.ts).
//
// What this guards: the artifact↔detection join surfaces the engine's persisted
// state/reason/completion VERBATIM (sanitized only for content-keyed digests),
// and a declared-but-not-detected cycle keeps its reason first-class (the
// "visibly impossible" case) with null detection fields.

import { describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the Postgres metadata client; mock both
// (with the `#/` alias — relative specifiers silently don't intercept) so the
// pure projection runs with no env and no connection.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import type { LifecycleArtifactRow } from "../db/metadata/lifecycle-artifacts";
import { type CycleDetectionRow, projectCycleOverview } from "./look-cycle";

const D1 = "204bc8e118543a6c35654c1f68c43539a2e226f2";

describe("projectCycleOverview (DAT-465)", () => {
	it("joins an executed artifact with its detection row — values verbatim", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "order_to_cash",
			state: "executed",
			stateReason: null,
		};
		const detected: CycleDetectionRow = {
			cycleName: "Order-to-Cash Cycle",
			businessValue: "high",
			isKnownType: true,
			confidence: 0.92,
			completionRate: 0.82,
			completedCycles: 41,
			totalRecords: 50,
		};

		expect(projectCycleOverview(artifact, detected)).toEqual({
			canonical_type: "order_to_cash",
			cycle_name: "Order-to-Cash Cycle",
			state: "executed",
			state_reason: null,
			business_value: "high",
			is_known_type: true,
			confidence: 0.92,
			completion_rate: 0.82,
			completed_cycles: 41,
			total_records: 50,
		});
	});

	it("keeps a not-detected cycle's state_reason first-class (visibly impossible)", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "subscription_renewal",
			state: "declared",
			stateReason: "not detected in this workspace",
		};

		const projected = projectCycleOverview(artifact, undefined);
		expect(projected.state).toBe("declared");
		expect(projected.state_reason).toBe("not detected in this workspace");
		// No detection row joined → detection fields are null, not invented.
		expect(projected.cycle_name).toBeNull();
		expect(projected.business_value).toBeNull();
		expect(projected.completion_rate).toBeNull();
		expect(projected.completed_cycles).toBeNull();
		expect(projected.total_records).toBeNull();
		expect(projected.is_known_type).toBeNull();
	});

	it("renders the grounded-but-unmeasured case (completion_rate null, reason set)", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "bank_reconciliation",
			state: "grounded",
			stateReason: "detected but no completion measurement could be derived",
		};
		const detected: CycleDetectionRow = {
			cycleName: "Bank Reconciliation",
			businessValue: "high",
			isKnownType: true,
			confidence: 0.7,
			completionRate: null,
			completedCycles: null,
			totalRecords: null,
		};

		const projected = projectCycleOverview(artifact, detected);
		expect(projected.state).toBe("grounded");
		expect(projected.state_reason).toBe(
			"detected but no completion measurement could be derived",
		);
		// Detected → name/value present, but completion stays null.
		expect(projected.cycle_name).toBe("Bank Reconciliation");
		expect(projected.completion_rate).toBeNull();
	});

	it("strips content-keyed src_<digest> names from engine-built free text", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "inventory_cycle",
			state: "declared",
			stateReason: `not detected: src_${D1}__inventory missing`,
		};
		const detected: CycleDetectionRow = {
			cycleName: `Flow over src_${D1}__inventory`,
			businessValue: "medium",
			isKnownType: false,
			confidence: 0.4,
			completionRate: 0.3,
			completedCycles: 3,
			totalRecords: 10,
		};

		const projected = projectCycleOverview(artifact, detected);
		expect(projected.state_reason).toBe("not detected: inventory missing");
		expect(projected.cycle_name).toBe("Flow over inventory");
		expect(JSON.stringify(projected)).not.toMatch(/src_[0-9a-f]{40}/);
	});

	it("coalesces a null state at the edge — never invents a lifecycle state", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "k",
			state: null,
			stateReason: null,
		};
		expect(projectCycleOverview(artifact, undefined).state).toBe("");
	});
});
