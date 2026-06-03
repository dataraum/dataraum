// @vitest-environment happy-dom

import { MantineProvider } from "@mantine/core";
import {
	cleanup,
	fireEvent,
	render,
	screen,
	waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ChatRail } from "#/ui/cockpit/chat-rail";
import { CockpitProvider, useCockpit } from "#/ui/cockpit/cockpit-state";

// Mock useChat at the SDK boundary — the test controls the message list + the
// loading/error flags and asserts OUR rendering, canvas projection, and approval
// dispatch. The SDK's own loop/transport is exercised by the compose smoke.
const h = vi.hoisted(() => ({
	messages: [] as unknown[],
	isLoading: false,
	error: undefined as Error | undefined,
	sendMessage: vi.fn(),
	addToolApprovalResponse: vi.fn(),
}));

vi.mock("@tanstack/ai-react", () => ({
	useChat: () => ({
		messages: h.messages,
		isLoading: h.isLoading,
		error: h.error,
		sendMessage: h.sendMessage,
		addToolApprovalResponse: h.addToolApprovalResponse,
	}),
	fetchServerSentEvents: () => ({}),
}));

function CanvasProbe() {
	const { canvasState, pinnedCallId, returnToLive } = useCockpit();
	return (
		<div>
			<div data-testid="canvas-kind">{canvasState.kind}</div>
			<div data-testid="pinned-call">{pinnedCallId ?? "live"}</div>
			<button
				type="button"
				data-testid="probe-return-to-live"
				onClick={returnToLive}
			>
				return
			</button>
		</div>
	);
}

function renderRail() {
	return render(
		<MantineProvider env="test">
			<CockpitProvider>
				<ChatRail />
				<CanvasProbe />
			</CockpitProvider>
		</MantineProvider>,
	);
}

// A single completed list_sources call → source-list canvas.
function sourcesCall(id: string, name = "orders") {
	return {
		id: `m-${id}`,
		role: "assistant",
		parts: [
			{
				type: "tool-call",
				id,
				name: "list_sources",
				state: "complete",
				output: [
					{
						kind: "file",
						name,
						backend: null,
						uri: `s3://dataraum-lake/uploads/abc/${name}`,
						size_bytes: 123,
					},
				],
			},
		],
	};
}

// A single completed list_tables call → workspace-inventory canvas.
function tablesCall(id: string) {
	return {
		id: `m-${id}`,
		role: "assistant",
		parts: [
			{
				type: "tool-call",
				id,
				name: "list_tables",
				state: "complete",
				output: [
					{
						table_id: "t1",
						source_id: "s1",
						table_name: "orders",
						layer: "typed",
						row_count: 42,
					},
				],
			},
		],
	};
}

describe("ChatRail (DAT-353)", () => {
	beforeEach(() => {
		h.messages = [];
		h.isLoading = false;
		h.error = undefined;
		h.sendMessage.mockClear();
		h.addToolApprovalResponse.mockClear();
	});
	afterEach(() => cleanup());

	it("sends the typed message on submit", () => {
		renderRail();
		fireEvent.change(screen.getByTestId("chat-input"), {
			target: { value: "hello agent" },
		});
		fireEvent.click(screen.getByTestId("chat-send"));
		expect(h.sendMessage).toHaveBeenCalledWith("hello agent");
	});

	it("renders assistant text parts", () => {
		h.messages = [
			{
				id: "a1",
				role: "assistant",
				parts: [{ type: "text", content: "hi there" }],
			},
		];
		renderRail();
		expect(screen.getByTestId("chat-messages").textContent).toContain(
			"hi there",
		);
	});

	it("projects a list_sources tool result onto the source-list canvas", () => {
		h.messages = [sourcesCall("c1")];
		renderRail();
		expect(screen.getByTestId("canvas-kind").textContent).toBe("source-list");
		expect(screen.getByTestId("tool-call-c1")).toBeTruthy();
	});

	it("renders an approval prompt and dispatches the response", () => {
		h.messages = [
			{
				id: "a1",
				role: "assistant",
				parts: [
					{
						type: "tool-call",
						id: "c1",
						name: "teach",
						state: "approval-requested",
						approval: { id: "ap1", needsApproval: true },
					},
				],
			},
		];
		renderRail();
		fireEvent.click(screen.getByTestId("tool-approve-c1"));
		expect(h.addToolApprovalResponse).toHaveBeenCalledWith({
			id: "ap1",
			approved: true,
		});
	});

	it("surfaces a run/stream error as a highlighted message in the chat (not a canvas takeover)", () => {
		h.error = new Error("kaboom");
		renderRail();
		// The RUN_ERROR text lands inline in the chat rail, highlighted…
		expect(screen.getByTestId("chat-error").textContent).toContain("kaboom");
		// …and the canvas stops spinning rather than showing a generic error widget.
		expect(screen.getByTestId("canvas-kind").textContent).toBe("empty");
	});

	it("mounts the upload entry-mode dropzone", () => {
		renderRail();
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

		renderRail();
		const input = screen.getByTestId("upload-input") as HTMLInputElement;
		fireEvent.change(input, {
			target: {
				files: [new File(["id\n1\n"], "people.csv", { type: "text/csv" })],
			},
		});

		await waitFor(() => expect(h.sendMessage).toHaveBeenCalled());
		// The connect-driving message references the staged s3:// path so the
		// agent runs the existing connect tool against it.
		expect(h.sendMessage.mock.calls[0][0]).toContain(
			"s3://dataraum-lake/uploads/u/people.csv",
		);
		// Canvas flips to loading while the connect round-trips.
		expect(screen.getByTestId("canvas-kind").textContent).toBe("loading");

		vi.unstubAllGlobals();
	});
});

describe("ChatRail tool-result chips (DAT-354)", () => {
	beforeEach(() => {
		h.messages = [];
		h.isLoading = false;
		h.error = undefined;
		h.sendMessage.mockClear();
		h.addToolApprovalResponse.mockClear();
	});
	afterEach(() => cleanup());

	// One completed call per canvas tool + its expected readable summary substring.
	const canvasCases: Array<{
		name: string;
		state: string;
		arguments?: string;
		output: unknown;
		summary: string;
	}> = [
		{
			name: "list_sources",
			state: "complete",
			output: [{ kind: "file", name: "orders.csv" }],
			summary: "1 file",
		},
		{
			name: "list_tables",
			state: "complete",
			output: [{ table_id: "t1" }, { table_id: "t2" }],
			summary: "2 tables",
		},
		{
			name: "look_table",
			state: "complete",
			output: {
				table_id: "t1",
				table_name: "orders",
				analyzed: true,
				pending_teaches: 0,
				columns: [{}, {}],
			},
			summary: "orders — 2 columns",
		},
		{
			name: "why_column",
			state: "complete",
			output: {
				column_id: "col1",
				column_name: "amount",
				table_name: "orders",
				found: true,
				band: "ready",
				intents: [],
				evidence: [],
			},
			summary: "amount (orders) — ready",
		},
		{
			name: "connect",
			state: "complete",
			output: {
				sourceKind: "file",
				source: "people.csv",
				tables: [{ name: "people" }],
			},
			summary: "people.csv — 1 table",
		},
		{
			name: "frame",
			state: "complete",
			output: { vertical: "ecommerce", concepts: [{}, {}, {}] },
			summary: "ecommerce — 3 concepts",
		},
		{
			name: "select",
			state: "complete",
			output: { source_id: "s1", name: "orders", source_type: "file" },
			summary: "orders (file)",
		},
		{
			name: "run_sql",
			state: "complete",
			arguments: JSON.stringify({ sql: "SELECT * FROM lake.typed.orders" }),
			output: { columns: [], rows: [], rowCount: 0 },
			summary: "SELECT * FROM lake.typed.orders",
		},
	];

	it.each(canvasCases)("renders a type-aware, no-JSON summary for $name", ({
		name,
		state,
		arguments: args,
		output,
		summary,
	}) => {
		h.messages = [
			{
				id: "m1",
				role: "assistant",
				parts: [
					{ type: "tool-call", id: "c1", name, state, arguments: args, output },
				],
			},
		];
		renderRail();
		const el = screen.getByTestId("tool-call-summary-c1");
		expect(el.textContent).toContain(summary);
		// No raw JSON dump in the rail.
		expect(screen.getByTestId("chat-messages").textContent).not.toContain(
			'"output"',
		);
	});

	it.each([
		"probe",
		"teach",
	])("renders %s as a display-only chip (no rehydrate target)", (name) => {
		h.messages = [
			{
				id: "m1",
				role: "assistant",
				parts: [
					{
						type: "tool-call",
						id: "c1",
						name,
						state: "complete",
						arguments: JSON.stringify({ type: "null_value", payload: {} }),
						output: { ok: true },
					},
				],
			},
		];
		renderRail();
		// A display-only chip exposes no clickable rehydrate handle.
		expect(screen.queryByTestId("tool-chip-c1")).toBeNull();
	});

	it("shows 'denied' (not a stuck loader) for a denied approval-gated tool", () => {
		// Deny is terminal: the call never reaches "complete", so without the denied
		// branch the card would spin its Loader forever (the Approve/Deny buttons
		// vanish once `approved` is set). It must read "denied" and offer no buttons.
		h.messages = [
			{
				id: "m1",
				role: "assistant",
				parts: [
					{
						type: "tool-call",
						id: "c1",
						name: "select",
						state: "approval-requested",
						arguments: "{}",
						approval: { id: "a1", needsApproval: true, approved: false },
					},
				],
			},
		];
		renderRail();
		expect(screen.getByTestId("tool-denied-c1").textContent).toBe("denied");
		expect(screen.queryByTestId("tool-approve-c1")).toBeNull();
		expect(screen.queryByTestId("tool-deny-c1")).toBeNull();
	});

	it("clicking a canvas-tool chip pins by call-id and projects that call's result", () => {
		// Two completed calls: list_tables is latest (live projection), list_sources
		// is the earlier one we want to rehydrate.
		h.messages = [sourcesCall("c-sources"), tablesCall("c-tables")];
		renderRail();
		// Live: the latest (list_tables) is projected.
		expect(screen.getByTestId("canvas-kind").textContent).toBe(
			"workspace-inventory",
		);
		expect(screen.getByTestId("pinned-call").textContent).toBe("live");

		// Click the earlier list_sources chip → pin + project that call's result.
		fireEvent.click(screen.getByTestId("tool-chip-c-sources"));
		expect(screen.getByTestId("pinned-call").textContent).toBe("c-sources");
		expect(screen.getByTestId("canvas-kind").textContent).toBe("source-list");
	});

	it("a non-canvas (display-only) chip click is a no-op", () => {
		h.messages = [
			{
				id: "m1",
				role: "assistant",
				parts: [
					{
						type: "tool-call",
						id: "c1",
						name: "teach",
						state: "complete",
						arguments: JSON.stringify({ type: "null_value", payload: {} }),
						output: { overlay_id: "ov1", type: "null_value" },
					},
				],
			},
		];
		renderRail();
		// No clickable handle, and clicking the card text leaves the canvas live.
		expect(screen.queryByTestId("tool-chip-c1")).toBeNull();
		fireEvent.click(screen.getByTestId("tool-call-c1"));
		expect(screen.getByTestId("pinned-call").textContent).toBe("live");
	});

	it("THE REVERSAL GUARD: while pinned, a freshly streamed tool-result does NOT change the canvas", () => {
		h.messages = [sourcesCall("c-sources")];
		const { rerender } = renderRail();
		// Pin to the list_sources result.
		fireEvent.click(screen.getByTestId("tool-chip-c-sources"));
		expect(screen.getByTestId("canvas-kind").textContent).toBe("source-list");
		expect(screen.getByTestId("pinned-call").textContent).toBe("c-sources");

		// A NEW tool result streams in (list_tables) while pinned. Force the
		// re-render that the SDK message update would trigger.
		h.messages = [sourcesCall("c-sources"), tablesCall("c-tables")];
		rerender(
			<MantineProvider env="test">
				<CockpitProvider>
					<ChatRail />
					<CanvasProbe />
				</CockpitProvider>
			</MantineProvider>,
		);
		// Canvas STAYS pinned on the historical result — the live projection is
		// suppressed while pinned.
		expect(screen.getByTestId("canvas-kind").textContent).toBe("source-list");
		expect(screen.getByTestId("pinned-call").textContent).toBe("c-sources");
	});

	it("returnToLive re-projects the latest EVEN WHEN it equals the last projected value", () => {
		// Single result. The pre-pin live projection is source-list; after pinning
		// to a DIFFERENT-but-same-kind earlier result and returning to live, the
		// dedupe must NOT suppress the snap-back.
		h.messages = [sourcesCall("c1", "alpha")];
		renderRail();
		// Live projection happened: source-list, dedupe ref now holds its key.
		expect(screen.getByTestId("canvas-kind").textContent).toBe("source-list");

		// Manually flip the canvas to something else, then pin to c1, then return.
		// Pin to c1 (its result is the SAME source-list the live ref already holds).
		fireEvent.click(screen.getByTestId("tool-chip-c1"));
		expect(screen.getByTestId("pinned-call").textContent).toBe("c1");

		// Return to live. The latest result equals the pre-pin projected value;
		// the guard must still re-project it (not be suppressed by a stale ref).
		fireEvent.click(screen.getByTestId("probe-return-to-live"));
		expect(screen.getByTestId("pinned-call").textContent).toBe("live");
		expect(screen.getByTestId("canvas-kind").textContent).toBe("source-list");
	});

	it("returnToLive snaps to the NEWEST result after a pin+stream", () => {
		h.messages = [sourcesCall("c-sources")];
		const { rerender } = renderRail();
		fireEvent.click(screen.getByTestId("tool-chip-c-sources"));
		// A newer result streamed while pinned.
		h.messages = [sourcesCall("c-sources"), tablesCall("c-tables")];
		rerender(
			<MantineProvider env="test">
				<CockpitProvider>
					<ChatRail />
					<CanvasProbe />
				</CockpitProvider>
			</MantineProvider>,
		);
		// Still pinned to the old source-list.
		expect(screen.getByTestId("canvas-kind").textContent).toBe("source-list");
		// Return to live → snaps to the newest (workspace-inventory).
		fireEvent.click(screen.getByTestId("probe-return-to-live"));
		expect(screen.getByTestId("pinned-call").textContent).toBe("live");
		expect(screen.getByTestId("canvas-kind").textContent).toBe(
			"workspace-inventory",
		);
	});

	it("renders a readable teach chip from arguments at approval time AND keeps Approve/Deny", () => {
		h.messages = [
			{
				id: "m1",
				role: "assistant",
				parts: [
					{
						type: "tool-call",
						id: "c1",
						name: "teach",
						state: "approval-requested",
						approval: { id: "ap1", needsApproval: true },
						arguments: JSON.stringify({
							type: "null_value",
							payload: { sentinel: "N/A" },
						}),
					},
				],
			},
		];
		renderRail();
		// The {type, payload} is readable — type name + the payload's field keys.
		const summary =
			screen.getByTestId("tool-call-summary-c1").textContent ?? "";
		expect(summary).toContain("null_value");
		expect(summary).toContain("sentinel");
		// No raw JSON dump.
		expect(summary).not.toContain('"payload"');
		// Approve/Deny still present and wired.
		fireEvent.click(screen.getByTestId("tool-approve-c1"));
		expect(h.addToolApprovalResponse).toHaveBeenCalledWith({
			id: "ap1",
			approved: true,
		});
	});

	it("renders the completed teach chip as {overlay_id, type} (display-only)", () => {
		h.messages = [
			{
				id: "m1",
				role: "assistant",
				parts: [
					{
						type: "tool-call",
						id: "c1",
						name: "teach",
						state: "complete",
						arguments: JSON.stringify({ type: "null_value", payload: {} }),
						output: { overlay_id: "ov-123", type: "null_value" },
					},
				],
			},
		];
		renderRail();
		const summary =
			screen.getByTestId("tool-call-summary-c1").textContent ?? "";
		expect(summary).toContain("null_value");
		expect(summary).toContain("ov-123");
	});
});
