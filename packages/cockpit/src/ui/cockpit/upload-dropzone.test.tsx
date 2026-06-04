// @vitest-environment jsdom
//
// Unit test for the upload entry-mode dropzone (DAT-386; multi-file DAT-391).
// Mocks `fetch` for the /api/upload POST and asserts: a single pick hands a
// one-element list to `onUploaded`; a valid multi-pick uploads each and hands
// the ordered list; the client-side batch gate (cap MAX_UPLOAD_FILES, same-kind)
// blocks before any upload; a route error surfaces and aborts the batch. The PUT
// are covered by the route unit test + connect.integration.

import { MantineProvider } from "@mantine/core";
import {
	cleanup,
	fireEvent,
	render,
	screen,
	waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { UploadDropzone } from "#/ui/cockpit/upload-dropzone";
import { MAX_UPLOAD_FILES } from "#/upload/policy";

function renderDropzone(onUploaded: (paths: string[]) => void) {
	return render(
		<MantineProvider env="test">
			<UploadDropzone onUploaded={onUploaded} />
		</MantineProvider>,
	);
}

function pick(names: string[]) {
	const input = screen.getByTestId("upload-input") as HTMLInputElement;
	const files = names.map(
		(n) => new File(["id\n1\n"], n, { type: "text/csv" }),
	);
	fireEvent.change(input, { target: { files } });
}

/** fetch mock that echoes each uploaded filename back as its staged s3:// path. */
function stubUploadOk() {
	const fetchMock = vi.fn(async (_url: string, init: { body: FormData }) => {
		const name = (init.body.get("file") as File).name;
		return new Response(
			JSON.stringify({ path: `s3://dataraum-lake/uploads/u/${name}` }),
			{ status: 200, headers: { "Content-Type": "application/json" } },
		);
	});
	vi.stubGlobal("fetch", fetchMock);
	return fetchMock;
}

describe("UploadDropzone (DAT-386 / DAT-391)", () => {
	beforeEach(() => vi.restoreAllMocks());
	afterEach(() => cleanup());

	it("uploads a single picked file and hands a one-element list to onUploaded", async () => {
		stubUploadOk();
		const onUploaded = vi.fn();
		renderDropzone(onUploaded);
		pick(["people.csv"]);
		await waitFor(() =>
			expect(onUploaded).toHaveBeenCalledWith([
				"s3://dataraum-lake/uploads/u/people.csv",
			]),
		);
	});

	it("uploads several files and hands the ordered s3:// list to onUploaded", async () => {
		const fetchMock = stubUploadOk();
		const onUploaded = vi.fn();
		renderDropzone(onUploaded);
		pick(["a.csv", "b.csv", "c.tsv"]);
		await waitFor(() =>
			expect(onUploaded).toHaveBeenCalledWith([
				"s3://dataraum-lake/uploads/u/a.csv",
				"s3://dataraum-lake/uploads/u/b.csv",
				"s3://dataraum-lake/uploads/u/c.tsv",
			]),
		);
		expect(fetchMock).toHaveBeenCalledTimes(3);
	});

	it("blocks more than MAX_UPLOAD_FILES at the client gate — no upload, no onUploaded", async () => {
		const fetchMock = stubUploadOk();
		const onUploaded = vi.fn();
		renderDropzone(onUploaded);
		pick(Array.from({ length: MAX_UPLOAD_FILES + 1 }, (_, i) => `f${i}.csv`));
		await waitFor(() =>
			expect(screen.getByTestId("upload-error").textContent).toMatch(
				new RegExp(`Up to ${MAX_UPLOAD_FILES}`),
			),
		);
		expect(fetchMock).not.toHaveBeenCalled();
		expect(onUploaded).not.toHaveBeenCalled();
	});

	it("blocks a mixed-kind batch (csv + parquet) before uploading", async () => {
		const fetchMock = stubUploadOk();
		const onUploaded = vi.fn();
		renderDropzone(onUploaded);
		pick(["a.csv", "b.parquet"]);
		await waitFor(() =>
			expect(screen.getByTestId("upload-error").textContent).toMatch(
				/same kind/i,
			),
		);
		expect(fetchMock).not.toHaveBeenCalled();
		expect(onUploaded).not.toHaveBeenCalled();
	});

	it("surfaces a route error and aborts the batch (no onUploaded)", async () => {
		const fetchMock = vi.fn().mockResolvedValue(
			new Response(JSON.stringify({ error: "Upload failed (500)." }), {
				status: 500,
				headers: { "Content-Type": "application/json" },
			}),
		);
		vi.stubGlobal("fetch", fetchMock);
		const onUploaded = vi.fn();
		renderDropzone(onUploaded);
		pick(["a.csv", "b.csv"]);
		await waitFor(() =>
			expect(screen.getByTestId("upload-error").textContent).toMatch(
				/failed to upload/i,
			),
		);
		expect(onUploaded).not.toHaveBeenCalled();
	});

	it("is inert while the agent is busy (disabled): no upload, no onUploaded", async () => {
		const fetchMock = stubUploadOk();
		const onUploaded = vi.fn();
		render(
			<MantineProvider env="test">
				<UploadDropzone onUploaded={onUploaded} disabled />
			</MantineProvider>,
		);
		const input = screen.getByTestId("upload-input") as HTMLInputElement;
		expect(input.disabled).toBe(true);
		pick(["a.csv"]);
		await Promise.resolve();
		expect(fetchMock).not.toHaveBeenCalled();
		expect(onUploaded).not.toHaveBeenCalled();
	});
});
