// Unit tests for the upload route handler (DAT-386).
//
// Importing the route transitively boots config.ts and @aws-lite (via
// upload/s3-upload). We MOCK both — `#/config` for the bucket and the s3 PUT —
// so the test asserts the gates + the locked handle shape with no SeaweedFS and
// no real client. Mocks use the `#/` alias (a relative mock does not intercept).

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: { s3Bucket: "dataraum-lake" } }));
const { putMock } = vi.hoisted(() => ({ putMock: vi.fn() }));
vi.mock("#/upload/s3-upload", () => ({ putObject: putMock }));

import { handleUpload } from "./upload";

// Deterministic uuid so the asserted key/handle is stable.
const FIXED_UUID = "uuuuuuuu-uuuu-uuuu-uuuu-uuuuuuuuuuuu";
const deps = () => ({
	bucket: "dataraum-lake",
	put: putMock,
	uuid: () => FIXED_UUID,
});

function formRequest(file?: File): Request {
	const form = new FormData();
	if (file) form.append("file", file);
	return new Request("http://x/api/upload", { method: "POST", body: form });
}

describe("handleUpload (DAT-386)", () => {
	it("PUTs to uploads/<uuid>/<name> and returns the locked s3:// handle", async () => {
		putMock.mockReset();
		putMock.mockResolvedValue(undefined);
		const file = new File(["id,name\n1,Ada\n"], "people.csv", {
			type: "text/csv",
		});

		const res = await handleUpload(formRequest(file), deps());

		expect(res.status).toBe(200);
		const body = (await res.json()) as { path: string };
		expect(body.path).toBe(
			`s3://dataraum-lake/uploads/${FIXED_UUID}/people.csv`,
		);

		expect(putMock).toHaveBeenCalledTimes(1);
		const [bucket, key, payload, contentType] = putMock.mock.calls[0];
		expect(bucket).toBe("dataraum-lake");
		expect(key).toBe(`uploads/${FIXED_UUID}/people.csv`);
		expect(Buffer.isBuffer(payload)).toBe(true);
		expect((payload as Buffer).toString()).toBe("id,name\n1,Ada\n");
		expect(contentType).toBe("text/csv");
	});

	it("sanitizes a path-bearing filename into a single safe leaf", async () => {
		putMock.mockReset();
		putMock.mockResolvedValue(undefined);
		const file = new File(["a"], "../../etc/evil.csv", { type: "text/csv" });

		const res = await handleUpload(formRequest(file), deps());

		const body = (await res.json()) as { path: string };
		expect(body.path).toBe(`s3://dataraum-lake/uploads/${FIXED_UUID}/evil.csv`);
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
