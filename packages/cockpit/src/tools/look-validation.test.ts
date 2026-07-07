// Unit tests for the look_validation projection (DAT-440 / docs/architecture/grounding.md). Pure — no
// DB; the live read + on-demand verdict path is covered by the operating_model
// integration smoke (scripts/smoke-operating-model.ts).
//
// What this guards: the projection surfaces the artifact's lifecycle
// state/reason verbatim (digest-sanitized), and folds the RECOMPUTED verdict
// (status/passed/message) + the declared severity over it — never a stored
// verdict (docs/architecture/grounding.md).

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
import type { Verdict } from "./validation-verdict";
import type { ValidationParams } from "./validation-verdict-runner";

describe("projectValidationOverview (DAT-440 / docs/architecture/grounding.md)", () => {
	it("folds the recomputed verdict + declared severity over the artifact", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "gl_invoice_match",
			state: "executed",
			stateReason: null,
		};
		const result: ValidationResultRow = {
			sqlUsed: "SELECT 12 AS deviation, 100 AS magnitude",
			columnsUsed: ["invoices.amount", "journal_lines.debit"],
		};
		const verdict: Verdict = {
			status: "failed",
			passed: false,
			deviation: 12,
			magnitude: 100,
			message: "validation: deviation 12 (tolerance 0.01)",
		};
		const params: ValidationParams = { tolerance: 0.01, severity: "error" };

		expect(
			projectValidationOverview(artifact, result, verdict, params),
		).toEqual({
			validation_id: "gl_invoice_match",
			state: "executed",
			state_reason: null,
			severity: "error",
			status: "failed",
			passed: false,
			message: "validation: deviation 12 (tolerance 0.01)",
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

		// No result row + no verdict → verdict fields are null, not invented.
		const projected = projectValidationOverview(
			artifact,
			undefined,
			undefined,
			undefined,
		);
		expect(projected.state).toBe("declared");
		expect(projected.state_reason).toBe(
			"Missing required tables: journal_entries and journal_lines are not in the session",
		);
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
			sqlUsed: "SELECT 0 AS deviation, 1 AS magnitude",
			columnsUsed: [`orders.amount`],
		};
		const verdict: Verdict = {
			status: "passed",
			passed: true,
			deviation: 0,
			magnitude: 1,
			message: "validation: deviation 0 (tolerance 0.01)",
		};
		const params: ValidationParams = { tolerance: 0.01, severity: null };

		const projected = projectValidationOverview(
			artifact,
			result,
			verdict,
			params,
		);
		expect(projected.state_reason).toBe("Missing required tables: orders");
		expect(projected.passed).toBe(true);
		expect(projected.columns_used).toEqual(["orders.amount"]);
		expect(JSON.stringify(projected)).not.toMatch(/src_[0-9a-f]{40}/);
	});

	it("coalesces a null state at the edge — never invents a lifecycle state", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "k",
			state: null,
			stateReason: null,
		};
		expect(
			projectValidationOverview(artifact, undefined, undefined, undefined)
				.state,
		).toBe("");
	});
});
