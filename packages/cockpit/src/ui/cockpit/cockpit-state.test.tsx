// @vitest-environment happy-dom

import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { CockpitProvider, useCockpit } from "#/ui/cockpit/cockpit-state";

function wrapper({ children }: { children: React.ReactNode }) {
	return <CockpitProvider>{children}</CockpitProvider>;
}

describe("cockpit-state (DAT-347)", () => {
	afterEach(() => cleanup());

	it("defaults to add_source / empty canvas", () => {
		const { result } = renderHook(() => useCockpit(), { wrapper });
		expect(result.current.activeStage).toBe("add_source");
		expect(result.current.canvasState).toEqual({ kind: "empty" });
	});

	it("setActiveStage updates the active stage", () => {
		const { result } = renderHook(() => useCockpit(), { wrapper });
		act(() => result.current.setActiveStage("connect"));
		expect(result.current.activeStage).toBe("connect");
	});

	it("setCanvasState swaps the canvas member", () => {
		const { result } = renderHook(() => useCockpit(), { wrapper });
		act(() => result.current.setCanvasState({ kind: "loading" }));
		expect(result.current.canvasState).toEqual({ kind: "loading" });
		act(() =>
			result.current.setCanvasState({ kind: "error", message: "boom" }),
		);
		expect(result.current.canvasState).toEqual({
			kind: "error",
			message: "boom",
		});
	});

	it("throws when useCockpit is read outside a provider", () => {
		expect(() => renderHook(() => useCockpit())).toThrow(/CockpitProvider/);
	});

	it("defaults to live (no pin)", () => {
		const { result } = renderHook(() => useCockpit(), { wrapper });
		expect(result.current.pinnedCallId).toBeNull();
	});

	it("pinCanvas sets the pin AND the canvas in one dispatch", () => {
		const { result } = renderHook(() => useCockpit(), { wrapper });
		act(() =>
			result.current.pinCanvas("call-7", { kind: "source-list", sources: [] }),
		);
		expect(result.current.pinnedCallId).toBe("call-7");
		expect(result.current.canvasState).toEqual({
			kind: "source-list",
			sources: [],
		});
	});

	it("returnToLive clears the pin (leaving the canvas for the rail to re-project)", () => {
		const { result } = renderHook(() => useCockpit(), { wrapper });
		act(() =>
			result.current.pinCanvas("call-7", { kind: "source-list", sources: [] }),
		);
		act(() => result.current.returnToLive());
		expect(result.current.pinnedCallId).toBeNull();
	});
});
