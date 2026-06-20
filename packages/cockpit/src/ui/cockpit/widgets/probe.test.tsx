// @vitest-environment jsdom

// Behavior tests for ProbeWidget → STAGING HUB (DAT-576 + DAT-592 + DAT-594). The
// boundaries are stubbed — they are external systems verified elsewhere: the server
// fns (env/RPC), the CodeMirror editor (a DOM widget, smoke-verified), the streaming
// grid (network I/O), the progress widget (Temporal polling), the upload dropzone,
// the model modal, and the router. What we assert is the widget's OWN logic: the
// no-sources state, run-gating, the probe-sql request body, the staging add flow,
// the MOVED gate (Add is free; Start gates on framed + non-empty set), and that
// Start calls importSources with the staged set + conversationId then shows progress.

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

const getConfiguredDatabasesMock = vi.fn();
vi.mock("#/server/configured-databases", () => ({
	getConfiguredDatabases: () => getConfiguredDatabasesMock(),
}));
const getActiveVerticalStatusMock = vi.fn();
vi.mock("#/server/active-vertical", () => ({
	getActiveVerticalStatus: () => getActiveVerticalStatusMock(),
}));
const importSourcesMock = vi.fn();
vi.mock("#/server/import-sources", () => ({
	importSources: (args: unknown) => importSourcesMock(args),
}));
// The model modal pulls #/server/stage-frame; stub the modal itself (its own
// behavior isn't under test here) but expose its props so we can drive the
// model-declared callback that flips the gate.
let modelModalProps: {
	opened: boolean;
	onModelDeclared: () => void;
} | null = null;
vi.mock("#/ui/cockpit/widgets/model-modal", () => ({
	ModelModal: (props: { opened: boolean; onModelDeclared: () => void }) => {
		modelModalProps = props;
		return props.opened ? <div data-testid="model-modal" /> : null;
	},
}));
vi.mock("#/ui/cockpit/upload-dropzone", () => ({
	UploadDropzone: ({
		onUploaded,
	}: {
		onUploaded: (uris: string[]) => void;
	}) => (
		<button
			type="button"
			data-testid="upload-dropzone"
			onClick={() =>
				onUploaded(["s3://dataraum-lake/ws/uploads/aaa111/orders.csv"])
			}
		>
			upload
		</button>
	),
}));
// Route param the widget reads to record the run against the chat.
vi.mock("@tanstack/react-router", () => ({
	useParams: () => ({ conversationId: "conv-1" }),
}));
vi.mock("#/ui/cockpit/widgets/sql-editor", () => ({
	SqlEditor: ({
		value,
		onChange,
	}: {
		value: string;
		onChange: (s: string) => void;
	}) => (
		<textarea
			data-testid="sql-editor"
			value={value}
			onChange={(e) => onChange(e.target.value)}
		/>
	),
}));
vi.mock("#/ui/cockpit/widgets/result-grid", () => ({
	StreamingGrid: ({
		endpoint,
		body,
	}: {
		endpoint: string;
		body: Record<string, unknown>;
	}) => (
		<div
			data-testid="streaming-grid"
			data-endpoint={endpoint}
			data-body={JSON.stringify(body)}
		/>
	),
}));
vi.mock("#/ui/cockpit/widgets/measure-progress", () => ({
	MeasureProgressWidget: ({
		state,
	}: {
		state: { workflowId: string; runId: string };
	}) => <div data-testid="measure-progress" data-workflow={state.workflowId} />,
}));

import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { ProbeWidget } from "#/ui/cockpit/widgets/probe";
import { theme } from "#/ui/theme";

function renderProbe(
	state: Extract<CanvasState, { kind: "probe" }> = { kind: "probe" },
) {
	const qc = new QueryClient({
		defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
	});
	return render(
		<QueryClientProvider client={qc}>
			<MantineProvider theme={theme} env="test">
				<ProbeWidget state={state} />
			</MantineProvider>
		</QueryClientProvider>,
	);
}

const runBtn = () => screen.getByTestId("probe-run") as HTMLButtonElement;
const addBtn = () =>
	screen.getByTestId("probe-add-to-set") as HTMLButtonElement;
const startBtn = () => screen.getByTestId("probe-start") as HTMLButtonElement;
const seededState: Extract<CanvasState, { kind: "probe" }> = {
	kind: "probe",
	source: { name: "wwi", backend: "mssql" },
	sql: "SELECT * FROM Sales.Orders",
};

// Default: a framed workspace, so the Start gate is open. Tests that exercise the
// unframed gate override this per-test (a later mockResolvedValue wins).
beforeEach(() => {
	modelModalProps = null;
	getActiveVerticalStatusMock.mockResolvedValue({
		vertical: "retail",
		framed: true,
	});
});

describe("ProbeWidget (DAT-576)", () => {
	afterEach(() => {
		cleanup();
		vi.clearAllMocks();
	});

	it("shows the no-sources alert and disables run when none are configured", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([]);
		renderProbe();
		await waitFor(() =>
			expect(screen.getByTestId("probe-no-sources")).toBeTruthy(),
		);
		expect(runBtn().disabled).toBe(true);
		expect(screen.queryByTestId("streaming-grid")).toBeNull();
	});

	it("gates run on a source AND non-empty SQL", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		renderProbe({ kind: "probe", source: { name: "wwi", backend: "mssql" } });
		await waitFor(() => expect(getConfiguredDatabasesMock).toHaveBeenCalled());
		expect(runBtn().disabled).toBe(true);

		fireEvent.change(screen.getByTestId("sql-editor"), {
			target: { value: "SELECT 1" },
		});
		await waitFor(() => expect(runBtn().disabled).toBe(false));
	});

	it("streams /api/probe-sql with the seeded source + sql on Run", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		renderProbe({
			kind: "probe",
			source: { name: "wwi", backend: "mssql" },
			sql: "SELECT TOP 10 * FROM Sales.Orders",
		});
		await waitFor(() => expect(runBtn().disabled).toBe(false));
		fireEvent.click(runBtn());

		const grid = await screen.findByTestId("streaming-grid");
		expect(grid.getAttribute("data-endpoint")).toBe("/api/probe-sql");
		expect(JSON.parse(grid.getAttribute("data-body") ?? "{}")).toEqual({
			source_name: "wwi",
			backend: "mssql",
			sql: "SELECT TOP 10 * FROM Sales.Orders",
		});
	});
});

describe("ProbeWidget staging hub (DAT-592 + DAT-594)", () => {
	afterEach(() => {
		cleanup();
		vi.clearAllMocks();
	});

	it("gates Add to import set on a source, non-empty SQL, and a valid name — NOT on framing", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		// Unframed: Add must STILL be reachable (the gate moved to Start, DAT-594).
		getActiveVerticalStatusMock.mockResolvedValue({
			vertical: "_adhoc",
			framed: false,
		});
		renderProbe(seededState);
		await waitFor(() => expect(getConfiguredDatabasesMock).toHaveBeenCalled());
		// Source + sql are seeded, but no name yet → disabled.
		expect(addBtn().disabled).toBe(true);

		// An invalid name (uppercase) stays disabled.
		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "Bad Name" },
		});
		expect(addBtn().disabled).toBe(true);

		// A valid name enables it EVEN THOUGH the workspace is unframed.
		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "wwi_orders" },
		});
		await waitFor(() => expect(addBtn().disabled).toBe(false));
	});

	it("gates START on a framed workspace (the moved gate) and shows why when blocked", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		getActiveVerticalStatusMock.mockResolvedValue({
			vertical: "_adhoc",
			framed: false,
		});
		renderProbe(seededState);
		await waitFor(() =>
			expect(screen.getByTestId("probe-unframed")).toBeTruthy(),
		);

		// Stage a query — Start is still blocked because the workspace is unframed.
		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "wwi_orders" },
		});
		await waitFor(() => expect(addBtn().disabled).toBe(false));
		fireEvent.click(addBtn());

		expect(startBtn().disabled).toBe(true);
		expect(screen.getByTestId("probe-start-blocked").textContent).toContain(
			"business model",
		);

		// The model modal flipping framed → true opens the Start gate (the
		// invalidation path is exercised by re-resolving the status query).
		getActiveVerticalStatusMock.mockResolvedValue({
			vertical: "retail",
			framed: true,
		});
		modelModalProps?.onModelDeclared();
		await waitFor(() => expect(startBtn().disabled).toBe(false));
	});

	it("stages a query, starts the import, and shows progress", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		importSourcesMock.mockResolvedValue({
			workflow_id: "addsource-ws",
			run_id: "addsource-ws",
			sources: ["sid-1"],
			source_names: ["wwi_orders"],
		});
		renderProbe(seededState);
		await waitFor(() => expect(getConfiguredDatabasesMock).toHaveBeenCalled());

		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "wwi_orders" },
		});
		await waitFor(() => expect(addBtn().disabled).toBe(false));
		fireEvent.click(addBtn());

		// The set lives behind a count symbol; the framed workspace opens Start.
		const indicator = await screen.findByTestId("probe-import-indicator");
		expect(indicator.textContent).toContain("1");
		await waitFor(() => expect(startBtn().disabled).toBe(false));
		fireEvent.click(startBtn());

		// importSources is called with the staged query + the conversationId, in the
		// heterogeneous { queries, files } shape (DAT-594).
		await waitFor(() => expect(importSourcesMock).toHaveBeenCalledTimes(1));
		expect(importSourcesMock).toHaveBeenCalledWith({
			data: {
				queries: [
					{
						source_name: "wwi_orders",
						credential_source: "wwi",
						backend: "mssql",
						sql: "SELECT * FROM Sales.Orders",
					},
				],
				files: [],
				conversationId: "conv-1",
			},
		});

		// On success the set clears and the inline progress widget shows.
		await waitFor(() =>
			expect(screen.getByTestId("probe-import-progress")).toBeTruthy(),
		);
		expect(
			screen.getByTestId("measure-progress").getAttribute("data-workflow"),
		).toBe("addsource-ws");
		expect(screen.queryByTestId("probe-import-indicator")).toBeNull();
	});

	it("stages an uploaded FILE into the set (mixed with queries)", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		importSourcesMock.mockResolvedValue({
			workflow_id: "addsource-ws",
			run_id: "addsource-ws",
			sources: ["sid-1", "sid-2"],
			source_names: ["wwi_orders", "src_aaa111"],
		});
		renderProbe(seededState);
		await waitFor(() => expect(getConfiguredDatabasesMock).toHaveBeenCalled());

		// Stage a query.
		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "wwi_orders" },
		});
		await waitFor(() => expect(addBtn().disabled).toBe(false));
		fireEvent.click(addBtn());

		// Open the upload modal and "upload" a file (the stub fires onUploaded).
		fireEvent.click(screen.getByTestId("probe-upload-open"));
		fireEvent.click(await screen.findByTestId("upload-dropzone"));

		// Set now holds 2 items (a query + a file).
		await waitFor(() =>
			expect(
				screen.getByTestId("probe-import-indicator").textContent,
			).toContain("2"),
		);
		await waitFor(() => expect(startBtn().disabled).toBe(false));
		fireEvent.click(startBtn());

		await waitFor(() => expect(importSourcesMock).toHaveBeenCalledTimes(1));
		expect(importSourcesMock).toHaveBeenCalledWith({
			data: {
				queries: [
					{
						source_name: "wwi_orders",
						credential_source: "wwi",
						backend: "mssql",
						sql: "SELECT * FROM Sales.Orders",
					},
				],
				files: [
					{ file_uri: "s3://dataraum-lake/ws/uploads/aaa111/orders.csv" },
				],
				conversationId: "conv-1",
			},
		});
	});

	it("re-adding a staged name updates its SQL rather than duplicating", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		renderProbe(seededState);
		await waitFor(() => expect(getConfiguredDatabasesMock).toHaveBeenCalled());

		// Stage wwi_orders with the seeded SQL.
		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "wwi_orders" },
		});
		await waitFor(() => expect(addBtn().disabled).toBe(false));
		fireEvent.click(addBtn());

		// Edit the SQL, re-stage under the SAME name (the button now reads "Update").
		fireEvent.change(screen.getByTestId("sql-editor"), {
			target: { value: "SELECT 99 AS x" },
		});
		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "wwi_orders" },
		});
		await waitFor(() => expect(addBtn().textContent).toContain("Update"));
		expect(addBtn().disabled).toBe(false);
		fireEvent.click(addBtn());

		// Still one staged source (the symbol reads 1); open the modal to verify SQL.
		const indicator = screen.getByTestId("probe-import-indicator");
		expect(indicator.textContent).toContain("1");
		fireEvent.click(indicator);
		const setPanel = await screen.findByTestId("probe-import-set");
		expect(setPanel.textContent).toContain("SELECT 99 AS x");
		expect(setPanel.textContent).not.toContain("SELECT * FROM Sales.Orders");
		expect(screen.getAllByText("wwi_orders")).toHaveLength(1);
	});

	it("surfaces an import error and preserves the set for retry", async () => {
		getConfiguredDatabasesMock.mockResolvedValue([
			{ name: "wwi", backend: "mssql" },
		]);
		importSourcesMock.mockRejectedValue(new Error("Temporal not configured"));
		renderProbe(seededState);
		await waitFor(() => expect(getConfiguredDatabasesMock).toHaveBeenCalled());

		fireEvent.change(screen.getByTestId("probe-import-name"), {
			target: { value: "wwi_orders" },
		});
		await waitFor(() => expect(addBtn().disabled).toBe(false));
		fireEvent.click(addBtn());
		await waitFor(() => expect(startBtn().disabled).toBe(false));
		fireEvent.click(startBtn());

		// The error surfaces and the set is NOT cleared — the symbol persists for retry.
		const err = await screen.findByTestId("probe-import-error");
		expect(err.textContent).toContain("Temporal not configured");
		expect(screen.getByTestId("probe-import-indicator")).toBeTruthy();
		expect(screen.queryByTestId("probe-import-progress")).toBeNull();
	});
});
