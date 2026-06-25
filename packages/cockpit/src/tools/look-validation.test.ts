// Unit tests for the look_validation projection (DAT-440). Pure — no DB; the
// live read path (head check + current_* views) is covered by the
// operating_model integration smoke (scripts/smoke-operating-model.ts).
//
// What this guards: the artifact↔result join surfaces the engine's persisted
// state/reason/message VERBATIM (sanitized only for content-keyed digests),
// and a declared-but-blocked validation keeps its reason first-class.

import { describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the Postgres metadata client; mock both
// (with the `#/` alias — relative specifiers silently don't intercept) so the
// pure projection runs with no env and no connection.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
	type LifecycleArtifactRow,
	projectValidationOverview,
	type ValidationResultRow,
} from "./look-validation";

describe("projectValidationOverview (DAT-440)", () => {
	it("joins an executed artifact with its result row — values verbatim", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "gl_invoice_match",
			state: "executed",
			stateReason: null,
		};
		const result: ValidationResultRow = {
			status: "executed",
			severity: "error",
			passed: false,
			message: "12 invoices have no matching journal entry",
			columnsUsed: ["invoices.amount", "journal_lines.debit"],
		};

		expect(projectValidationOverview(artifact, result)).toEqual({
			validation_id: "gl_invoice_match",
			state: "executed",
			state_reason: null,
			severity: "error",
			status: "executed",
			passed: false,
			message: "12 invoices have no matching journal entry",
			columns_used: ["invoices.amount", "journal_lines.debit"],
		});
	});

	it("keeps a blocked validation's state_reason first-class (visibly impossible)", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "gl_invoice_match",
			state: "declared",
			stateReason:
				"Missing required tables: journal_entries and journal_lines are not in the session",
		};

		const projected = projectValidationOverview(artifact, undefined);
		expect(projected.state).toBe("declared");
		expect(projected.state_reason).toBe(
			"Missing required tables: journal_entries and journal_lines are not in the session",
		);
		// No result row joined → result fields are null, not invented.
		expect(projected.status).toBeNull();
		expect(projected.passed).toBeNull();
		expect(projected.message).toBeNull();
		expect(projected.severity).toBeNull();
	});

	it("renders narrow names in engine-built free text; stays digest-free (DAT-639)", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "balance_check",
			state: "declared",
			stateReason: `Missing required tables: orders`,
		};
		const result: ValidationResultRow = {
			status: "executed",
			severity: null,
			passed: true,
			message: `checked orders rows`,
			columnsUsed: [`orders.amount`],
		};

		const projected = projectValidationOverview(artifact, result);
		expect(projected.state_reason).toBe("Missing required tables: orders");
		expect(projected.message).toBe("checked orders rows");
		expect(projected.columns_used).toEqual(["orders.amount"]);
		expect(JSON.stringify(projected)).not.toMatch(/src_[0-9a-f]{40}/);
	});

	it("coalesces a null state at the edge — never invents a lifecycle state", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "k",
			state: null,
			stateReason: null,
		};
		expect(projectValidationOverview(artifact, undefined).state).toBe("");
	});
});
