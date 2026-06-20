// @vitest-environment jsdom

// Behavior tests for the Frame / Vertical modal (DAT-594). The server fns are
// stubbed (env/RPC, verified elsewhere); what we assert is the modal's own logic:
// the frame path sends the staged set + vertical name, the adopt path sends the
// picked vertical, both call onModelDeclared on success, and the empty-set guard.

import { MantineProvider } from "@mantine/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
	cleanup,
	fireEvent,
	render,
	screen,
	waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const frameStagingSetMock = vi.fn();
const adoptVerticalForStagingMock = vi.fn();
const listAdoptableVerticalsMock = vi.fn();
vi.mock("#/server/stage-frame", () => ({
	frameStagingSet: (args: unknown) => frameStagingSetMock(args),
	adoptVerticalForStaging: (args: unknown) => adoptVerticalForStagingMock(args),
	listAdoptableVerticals: () => listAdoptableVerticalsMock(),
}));

import {
	ModelModal,
	type StagedForFrame,
} from "#/ui/cockpit/widgets/model-modal";
import { theme } from "#/ui/theme";

function renderModal(importSet: StagedForFrame[], onModelDeclared = vi.fn()) {
	const qc = new QueryClient({
		defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
	});
	render(
		<QueryClientProvider client={qc}>
			<MantineProvider theme={theme} env="test">
				<ModelModal
					opened
					onClose={vi.fn()}
					importSet={importSet}
					onModelDeclared={onModelDeclared}
				/>
			</MantineProvider>
		</QueryClientProvider>,
	);
	return { onModelDeclared };
}

const STAGED: StagedForFrame[] = [
	{
		kind: "query",
		source_name: "wwi_orders",
		credential_source: "wwi",
		backend: "mssql",
		sql: "SELECT * FROM Sales.Orders",
	},
	{ kind: "file", file_uri: "s3://b/ws/uploads/aaa/orders.csv" },
];

beforeEach(() => {
	listAdoptableVerticalsMock.mockResolvedValue([
		{
			name: "finance",
			kind: "builtin",
			description: "Ledgers",
			concept_count: 12,
		},
	]);
});

afterEach(() => {
	cleanup();
	vi.clearAllMocks();
});

describe("ModelModal frame path (DAT-594)", () => {
	it("frames over the staged set + vertical name and signals on success", async () => {
		frameStagingSetMock.mockResolvedValue({
			vertical: "sales",
			concept_count: 4,
			validation_count: 1,
			cycle_count: 0,
			metric_count: 0,
		});
		const { onModelDeclared } = renderModal(STAGED);

		fireEvent.change(screen.getByTestId("model-vertical-name"), {
			target: { value: "sales" },
		});
		fireEvent.click(screen.getByTestId("model-frame-run"));

		await waitFor(() => expect(frameStagingSetMock).toHaveBeenCalledTimes(1));
		expect(frameStagingSetMock).toHaveBeenCalledWith({
			data: {
				queries: [
					{
						source_name: "wwi_orders",
						credential_source: "wwi",
						backend: "mssql",
						sql: "SELECT * FROM Sales.Orders",
					},
				],
				files: [{ file_uri: "s3://b/ws/uploads/aaa/orders.csv" }],
				vertical_name: "sales",
			},
		});
		await waitFor(() => expect(onModelDeclared).toHaveBeenCalledTimes(1));
	});

	it("blocks frame on an empty staged set", () => {
		renderModal([]);
		expect(screen.getByTestId("model-empty-set")).toBeTruthy();
		expect(
			(screen.getByTestId("model-frame-run") as HTMLButtonElement).disabled,
		).toBe(true);
	});

	it("surfaces a frame error", async () => {
		frameStagingSetMock.mockRejectedValue(new Error("induction returned none"));
		renderModal(STAGED);
		fireEvent.click(screen.getByTestId("model-frame-run"));
		const err = await screen.findByTestId("model-error");
		expect(err.textContent).toContain("induction returned none");
	});
});

describe("ModelModal adopt path (DAT-594)", () => {
	it("adopts the picked builtin vertical and signals on success", async () => {
		adoptVerticalForStagingMock.mockResolvedValue({
			vertical: "finance",
			kind: "builtin",
		});
		const { onModelDeclared } = renderModal(STAGED);

		fireEvent.click(screen.getByTestId("model-mode-adopt"));
		// The vertical list loads from the stubbed server fn.
		await screen.findByTestId("model-vertical-list");
		fireEvent.click(screen.getByLabelText(/finance/));
		fireEvent.click(screen.getByTestId("model-adopt-run"));

		await waitFor(() =>
			expect(adoptVerticalForStagingMock).toHaveBeenCalledWith({
				data: { name: "finance" },
			}),
		);
		await waitFor(() => expect(onModelDeclared).toHaveBeenCalledTimes(1));
	});
});
