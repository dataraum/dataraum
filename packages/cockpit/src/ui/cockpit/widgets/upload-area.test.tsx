// @vitest-environment jsdom

import { MantineProvider } from "@mantine/core";
import {
	cleanup,
	fireEvent,
	render,
	screen,
	waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { UploadAreaWidget } from "#/ui/cockpit/widgets/upload-area";

// The widget drives the connect flow via the stable actions context; mock it so
// the test needs no CockpitProvider and can observe the dispatched request.
const sendMessage = vi.fn();
vi.mock("#/ui/cockpit/cockpit-state", () => ({
	useCockpitActions: () => ({ sendMessage }),
	useCockpitState: () => ({ isLoading: false }),
}));

function renderWidget() {
	render(
		<MantineProvider env="test">
			<UploadAreaWidget state={{ kind: "upload-area" }} />
		</MantineProvider>,
	);
}

describe("UploadAreaWidget", () => {
	afterEach(() => {
		cleanup();
		sendMessage.mockClear();
	});

	it("renders the upload area with the dropzone", () => {
		renderWidget();
		expect(screen.getByTestId("canvas-upload-area")).toBeTruthy();
		expect(screen.getByTestId("upload-dropzone")).toBeTruthy();
	});

	it("hands a clean bubble + a model-only refs part — no s3:// in the bubble (DAT-423)", async () => {
		const fetchMock = vi
			.fn()
			.mockResolvedValue(
				new Response(
					JSON.stringify({ path: "s3://dataraum-lake/uploads/u/people.csv" }),
					{ status: 200, headers: { "Content-Type": "application/json" } },
				),
			);
		vi.stubGlobal("fetch", fetchMock);

		renderWidget();
		const input = screen.getByTestId("upload-input") as HTMLInputElement;
		fireEvent.change(input, {
			target: {
				files: [new File(["id\n1\n"], "people.csv", { type: "text/csv" })],
			},
		});

		await waitFor(() => expect(sendMessage).toHaveBeenCalled());
		// The upload turn is a CLEAN bubble (filenames, no path) + model-only refs
		// via forwardedProps (the s3:// uris) — never in the visible message.
		const [bubble, opts] = sendMessage.mock.calls[0] as [
			string,
			{ refs?: string },
		];
		expect(bubble).toContain("people.csv");
		expect(bubble).not.toContain("s3://");
		expect(opts.refs).toContain("s3://dataraum-lake/uploads/u/people.csv");

		vi.unstubAllGlobals();
	});
});
