// Unit tests for the upload route handler (DAT-386).
//
// Importing the route transitively boots config.ts and @aws-lite (via
// upload/s3-upload), plus the cockpit_db registry (DAT-505) which pulls in the
// bun-only client (`import { SQL } from "bun"`). We MOCK all three — `#/config`
// for the bucket, the s3 PUT, and `#/db/cockpit/registry` to break the
// transitive bun import under node (mirrors trigger-add-source.test.ts) — so the
// test asserts the gates + the locked handle shape with no SeaweedFS, no DB, and
// no real client. Mocks use the `#/` alias (a relative mock does not intercept).

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: { s3Bucket: "dataraum-lake" } }));
const { putMock } = vi.hoisted(() => ({ putMock: vi.fn() }));
vi.mock("#/upload/s3-upload", () => ({ putObject: putMock }));
// The route resolves the active workspace via the registry, which transitively
// imports the bun-only cockpit_db client; mock it so the units project (node)
// can load the import graph. handleUpload takes an injected workspaceId, so the
// return value is irrelevant to these tests — only breaking the import matters.
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspaceRow: vi.fn(async () => ({
		id: "00000000-0000-0000-0000-000000000001",
		taskQueue: "dataraum-pipeline",
		vertical: "_adhoc",
	})),
}));

import { handleUpload } from "./upload";

// Deterministic digest so the asserted key/handle is stable; `listPrefix`
// returns no existing object by default (so each test stages a fresh upload).
const FIXED_DIGEST = "deadbeefcafe";
// The route resolves this from the registry (DAT-505) and passes it in; the
// handler stages under `<ws>/uploads/...`. The unit test injects it directly.
const WS = "00000000-0000-0000-0000-000000000001";
const deps = (existing: string[] = []) => ({
	bucket: "dataraum-lake",
	workspaceId: WS,
	put: putMock,
	digest: async () => FIXED_DIGEST,
	listPrefix: async () => existing,
});

function formRequest(file?: File): Request {
	const form = new FormData();
	if (file) form.append("file", file);
	return new Request("http://x/api/upload", { method: "POST", body: form });
}

describe("handleUpload (DAT-386)", () => {
	it("PUTs to <ws>/uploads/<digest>/<name> and returns the locked s3:// handle", async () => {
		putMock.mockReset();
		putMock.mockResolvedValue(undefined);
		const file = new File(["id,name\n1,Ada\n"], "people.csv", {
			type: "text/csv",
		});

		const res = await handleUpload(formRequest(file), deps());

		expect(res.status).toBe(200);
		const body = (await res.json()) as { path: string; deduped: boolean };
		expect(body.path).toBe(
			`s3://dataraum-lake/${WS}/uploads/${FIXED_DIGEST}/people.csv`,
		);
		expect(body.deduped).toBe(false);

		expect(putMock).toHaveBeenCalledTimes(1);
		const [bucket, key, payload, contentType] = putMock.mock.calls[0];
		expect(bucket).toBe("dataraum-lake");
		expect(key).toBe(`${WS}/uploads/${FIXED_DIGEST}/people.csv`);
		expect(Buffer.isBuffer(payload)).toBe(true);
		expect((payload as Buffer).toString()).toBe("id,name\n1,Ada\n");
		expect(contentType).toBe("text/csv");
	});

	it("dedups identical content: skips the PUT and returns the existing handle", async () => {
		putMock.mockReset();
		const file = new File(["id,name\n1,Ada\n"], "people-again.csv", {
			type: "text/csv",
		});
		const existingKey = `${WS}/uploads/${FIXED_DIGEST}/people.csv`;

		const res = await handleUpload(formRequest(file), deps([existingKey]));

		expect(res.status).toBe(200);
		const body = (await res.json()) as { path: string; deduped: boolean };
		// The EXISTING object's handle (the first-staged filename), not the new one.
		expect(body.path).toBe(`s3://dataraum-lake/${existingKey}`);
		expect(body.deduped).toBe(true);
		expect(putMock).not.toHaveBeenCalled();
	});

	it("sanitizes a path-bearing filename into a single safe leaf", async () => {
		putMock.mockReset();
		putMock.mockResolvedValue(undefined);
		const file = new File(["a"], "../../etc/evil.csv", { type: "text/csv" });

		const res = await handleUpload(formRequest(file), deps());

		const body = (await res.json()) as { path: string };
		expect(body.path).toBe(
			`s3://dataraum-lake/${WS}/uploads/${FIXED_DIGEST}/evil.csv`,
		);
	});

	it("rejects a missing file with 400", async () => {
		putMock.mockReset();
		const res = await handleUpload(formRequest(), deps());
		expect(res.status).toBe(400);
		expect(putMock).not.toHaveBeenCalled();
	});

	it("rejects an unsupported extension with 415 before touching the bucket", async () => {
		putMock.mockReset();
		const file = new File(["x"], "sheet.xlsx", {
			type: "application/vnd.ms-excel",
		});
		const res = await handleUpload(formRequest(file), deps());
		expect(res.status).toBe(415);
		expect(putMock).not.toHaveBeenCalled();
	});

	it("rejects an oversized file with 413 before touching the bucket", async () => {
		putMock.mockReset();
		// Lie about size via a real over-cap body: File.size reflects the bytes,
		// so a >100MiB body trips the size gate. Build a sparse-ish large string.
		const huge = "x".repeat(100 * 1024 * 1024 + 1);
		const file = new File([huge], "big.csv", { type: "text/csv" });
		const res = await handleUpload(formRequest(file), deps());
		expect(res.status).toBe(413);
		expect(putMock).not.toHaveBeenCalled();
	});

	it("maps a failed PUT to 502", async () => {
		putMock.mockReset();
		putMock.mockRejectedValue(new Error("seaweedfs down"));
		const file = new File(["id\n1\n"], "x.csv", { type: "text/csv" });
		const res = await handleUpload(formRequest(file), deps());
		expect(res.status).toBe(502);
	});
});
