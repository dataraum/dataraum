// Unit tests for the look_metric projection (DAT-466). Pure — no DB; the live
// read path (head check + lifecycle reader + snippet count) is covered by the
// operating_model integration smoke.
//
// What this guards: the projection surfaces the engine's persisted state/reason
// VERBATIM (sanitized only for content-keyed digests) + the snippet_count
// signal, and a declared-but-ungroundable metric keeps its reason first-class.

import { describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the Postgres metadata client; mock both
// (with the `#/` alias — relative specifiers silently don't intercept) so the
// pure projection runs with no env and no connection.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import type { LifecycleArtifactRow } from "../db/metadata/lifecycle-artifacts";
import { metricSnippetSource, projectMetricOverview } from "./look-metric";

const D1 = "204bc8e118543a6c35654c1f68c43539a2e226f2";

describe("metricSnippetSource (DAT-466)", () => {
	it("builds the graph:<id> provenance link", () => {
		expect(metricSnippetSource("ebitda")).toBe("graph:ebitda");
	});
});

describe("projectMetricOverview (DAT-466)", () => {
	it("projects an executed metric with its snippet count — values verbatim", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "ebitda",
			state: "executed",
			stateReason: null,
		};
		expect(projectMetricOverview(artifact, 6)).toEqual({
			graph_id: "ebitda",
			state: "executed",
			state_reason: null,
			snippet_count: 6,
		});
	});

	it("keeps an ungroundable metric's state_reason first-class (visibly impossible)", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "dso",
			state: "declared",
			stateReason:
				"ungroundable: required field mappings missing (missing: accounts_receivable)",
		};

		const projected = projectMetricOverview(artifact, 0);
		expect(projected.state).toBe("declared");
		expect(projected.state_reason).toBe(
			"ungroundable: required field mappings missing (missing: accounts_receivable)",
		);
		// Never composed → zero persisted SQL steps.
		expect(projected.snippet_count).toBe(0);
	});

	it("renders the composed-but-unexecutable case (grounded + reason, snippets exist)", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "current_ratio",
			state: "grounded",
			stateReason: "composed but execution failed: division by zero",
		};
		const projected = projectMetricOverview(artifact, 3);
		expect(projected.state).toBe("grounded");
		expect(projected.state_reason).toContain("composed but execution failed");
		expect(projected.snippet_count).toBe(3);
	});

	it("strips content-keyed src_<digest> names from engine-built free text", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "gross_margin",
			state: "declared",
			stateReason: `ungroundable: src_${D1}__income missing`,
		};
		const projected = projectMetricOverview(artifact, 0);
		expect(projected.state_reason).toBe("ungroundable: income missing");
		expect(JSON.stringify(projected)).not.toMatch(/src_[0-9a-f]{40}/);
	});

	it("coalesces a null state at the edge — never invents a lifecycle state", () => {
		const artifact: LifecycleArtifactRow = {
			artifactKey: "k",
			state: null,
			stateReason: null,
		};
		expect(projectMetricOverview(artifact, 0).state).toBe("");
	});
});
