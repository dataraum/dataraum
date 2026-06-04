// @vitest-environment jsdom
//
// The provider now OWNS the chat (useChat) and DERIVES the canvas from the
// message stream. We mock useChat at the SDK boundary to feed messages / loading
// and assert the derivation: canvas = pinned ?? override ?? live ?? loading/empty.

import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CockpitProvider, useCockpit } from "#/ui/cockpit/cockpit-state";

const h = vi.hoisted(() => ({
	messages: [] as unknown[],
	isLoading: false,
	error: undefined as Error | undefined,
	sendMessage: vi.fn(),
	stop: vi.fn(),
	addToolApprovalResponse: vi.fn(),
}));

vi.mock("@tanstack/ai-react", () => ({
	useChat: () => ({
		messages: h.messages,
		isLoading: h.isLoading,
		error: h.error,
		sendMessage: h.sendMessage,
		stop: h.stop,
		addToolApprovalResponse: h.addToolApprovalResponse,
	}),
	fetchServerSentEvents: () => ({}),
}));

function wrapper({ children }: { children: React.ReactNode }) {
	return <CockpitProvider>{children}</CockpitProvider>;
}

// A single completed list_sources call → source-list canvas (the simplest live
// projection the derivation can produce).
function sourcesCall(id: string) {
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
					{ kind: "file", name: "orders.csv", uri: "s3://x/orders.csv" },
				],
			},
		],
	};
}

describe("cockpit-state — view + chat (DAT-347 / DAT-353)", () => {
	beforeEach(() => {
		h.messages = [];
		h.isLoading = false;
		h.error = undefined;
		h.sendMessage.mockClear();
		h.stop.mockClear();
		h.addToolApprovalResponse.mockClear();
	});
	afterEach(() => cleanup());

	it("defaults to an empty canvas / live (no pin)", () => {
		const { result } = renderHook(() => useCockpit(), { wrapper });
		expect(result.current.canvas).toEqual({ kind: "empty" });
		expect(result.current.pinnedCallId).toBeNull();
	});

	it("throws when useCockpit is read outside a provider", () => {
		expect(() => renderHook(() => useCockpit())).toThrow(/CockpitProvider/);
	});

	it("DERIVES the canvas from the latest mappable tool result", () => {
		h.messages = [sourcesCall("c1")];
		const { result } = renderHook(() => useCockpit(), { wrapper });
		expect(result.current.canvas).toEqual({
			kind: "source-list",
			sources: [{ kind: "file", name: "orders.csv", uri: "s3://x/orders.csv" }],
		});
	});

	it("shows a loading canvas while a turn is in flight with nothing to show yet", () => {
		h.isLoading = true;
		const { result } = renderHook(() => useCockpit(), { wrapper });
		expect(result.current.canvas.kind).toBe("loading");
	});

	it("captions the loading canvas from the send label", () => {
		h.isLoading = true;
		const { result } = renderHook(() => useCockpit(), { wrapper });
		act(() =>
			result.current.sendMessage("explain", {
				label: "Explaining the column…",
			}),
		);
		expect(result.current.canvas).toEqual({
			kind: "loading",
			label: "Explaining the column…",
		});
	});

	it("a live result wins over the loading state", () => {
		h.isLoading = true;
		h.messages = [sourcesCall("c1")];
		const { result } = renderHook(() => useCockpit(), { wrapper });
		expect(result.current.canvas.kind).toBe("source-list");
	});

	it("sendMessage routes to the SDK and is callable from anywhere (no bridge)", () => {
		const { result } = renderHook(() => useCockpit(), { wrapper });
		act(() => result.current.sendMessage("hello"));
		expect(h.sendMessage).toHaveBeenCalledWith("hello");
	});

	it("stop forwards to the SDK so a turn can be aborted", () => {
		const { result } = renderHook(() => useCockpit(), { wrapper });
		act(() => result.current.stop());
		expect(h.stop).toHaveBeenCalledOnce();
	});

	it("pinCanvas pins by call-id and re-derives that call's result from the stream", () => {
		h.messages = [sourcesCall("c1")];
		const { result } = renderHook(() => useCockpit(), { wrapper });
		act(() => result.current.pinCanvas("c1"));
		expect(result.current.pinnedCallId).toBe("c1");
		expect(result.current.canvas.kind).toBe("source-list");
	});

	it("returnToLive clears the pin", () => {
		h.messages = [sourcesCall("c1")];
		const { result } = renderHook(() => useCockpit(), { wrapper });
		act(() => result.current.pinCanvas("c1"));
		act(() => result.current.returnToLive());
		expect(result.current.pinnedCallId).toBeNull();
	});

	it("showCanvas imperatively overrides the canvas; the next turn clears it", () => {
		const { result } = renderHook(() => useCockpit(), { wrapper });
		act(() =>
			result.current.showCanvas({
				kind: "add-source-progress",
				workflowId: "wf1",
				runId: "run1",
			}),
		);
		expect(result.current.canvas).toEqual({
			kind: "add-source-progress",
			workflowId: "wf1",
			runId: "run1",
		});
		// A new turn supersedes the imperative override.
		act(() => result.current.sendMessage("next"));
		expect(result.current.canvas.kind).not.toBe("add-source-progress");
	});

	it("a pin outranks an imperative override", () => {
		h.messages = [sourcesCall("c1")];
		const { result } = renderHook(() => useCockpit(), { wrapper });
		act(() =>
			result.current.showCanvas({
				kind: "add-source-progress",
				workflowId: "wf1",
				runId: "run1",
			}),
		);
		act(() => result.current.pinCanvas("c1"));
		expect(result.current.canvas.kind).toBe("source-list");
	});
});
