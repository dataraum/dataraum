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

	it("drives connect over the staged s3:// handle after an upload", async () => {
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
		// The connect-driving message references the staged s3:// path so the agent
		// runs the existing connect tool against it.
		expect(sendMessage.mock.calls[0][0]).toContain(
			"s3://dataraum-lake/uploads/u/people.csv",
		);

		vi.unstubAllGlobals();
	});
});
