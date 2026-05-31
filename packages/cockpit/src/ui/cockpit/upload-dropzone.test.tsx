// @vitest-environment happy-dom
//
// Unit test for the upload entry-mode dropzone (DAT-386). Mocks `fetch` for the
// /api/upload POST and asserts the dropzone uploads the picked file, hands the
// returned `s3://` handle to `onConnect` (which drives the existing connect
// tool), and surfaces an error response. The real PUT + sniff are covered by the
// route unit test and connect.integration.

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

function renderDropzone(onConnect: (p: string) => void) {
	return render(
		<MantineProvider env="test">
			<UploadDropzone onConnect={onConnect} />
		</MantineProvider>,
	);
}

function pickFile(name: string, content = "id\n1\n") {
	const input = screen.getByTestId("upload-input") as HTMLInputElement;
	const file = new File([content], name, { type: "text/csv" });
	fireEvent.change(input, { target: { files: [file] } });
}

describe("UploadDropzone (DAT-386)", () => {
	beforeEach(() => {
		vi.restoreAllMocks();
	});
	afterEach(() => cleanup());

	it("uploads the picked file and hands the s3:// handle to onConnect", async () => {
		const fetchMock = vi
			.fn()
			.mockResolvedValue(
				new Response(
					JSON.stringify({ path: "s3://dataraum-lake/uploads/u/people.csv" }),
					{ status: 200, headers: { "Content-Type": "application/json" } },
				),
			);
		vi.stubGlobal("fetch", fetchMock);
		const onConnect = vi.fn();

		renderDropzone(onConnect);
		pickFile("people.csv");

		await waitFor(() =>
			expect(onConnect).toHaveBeenCalledWith(
				"s3://dataraum-lake/uploads/u/people.csv",
			),
		);

		// POSTs multipart to the route.
		const [url, init] = fetchMock.mock.calls[0];
		expect(url).toBe("/api/upload");
		expect(init.method).toBe("POST");
		expect(init.body).toBeInstanceOf(FormData);
		expect((init.body as FormData).get("file")).toBeInstanceOf(File);
	});

	it("surfaces a route error and does not drive connect", async () => {
		const fetchMock = vi.fn().mockResolvedValue(
			new Response(JSON.stringify({ error: "Unsupported file type." }), {
				status: 415,
				headers: { "Content-Type": "application/json" },
			}),
		);
		vi.stubGlobal("fetch", fetchMock);
		const onConnect = vi.fn();

		renderDropzone(onConnect);
		pickFile("sheet.csv");

		await waitFor(() =>
			expect(screen.getByTestId("upload-error").textContent).toContain(
				"Unsupported file type.",
			),
		);
		expect(onConnect).not.toHaveBeenCalled();
	});
});
