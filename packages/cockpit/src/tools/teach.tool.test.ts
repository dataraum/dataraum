// Error-surface tests for the teach tool's server handler (review-batch).
//
// `runTeachTool` turns a recoverable TeachValidationError into a structured
// `{error}` (so the agent can read it and retry) while letting any other error
// propagate. Importing teach.ts transitively pulls config.ts + the Postgres
// metadata client, so we mock both (same approach as registry.test.ts) — the
// validation path never touches the DB, and the rethrow path relies on the
// empty metadataDb stub throwing when teach() reaches the insert.

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import { runTeachTool } from "./teach";

describe("runTeachTool error surface (review-batch)", () => {
	it("returns a structured {error} on a TeachValidationError (malformed payload)", async () => {
		// `concept` requires a vertical — omitting it throws TeachValidationError
		// inside validateTeach, before any DB call.
		const out = await runTeachTool({
			type: "concept",
			payload: { name: "revenue" },
		});
		expect(out).toHaveProperty("error");
		expect((out as { error: string }).error).toMatch(/vertical/i);
	});

	it("rethrows a non-validation error (e.g. DB failure) instead of masking it", async () => {
		// Valid payload passes validateTeach, then teach() hits the empty
		// metadataDb stub (`{}.insert` is not a function) → a non-TeachValidationError
		// that must propagate, not be swallowed into {error}.
		await expect(
			runTeachTool({
				type: "concept",
				payload: { vertical: "_adhoc", name: "revenue" },
			}),
		).rejects.toThrow();
	});
});
