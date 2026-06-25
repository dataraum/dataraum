import { describe, expect, it } from "vitest";
import { canvasRegistry } from "#/ui/cockpit/canvas-registry";
import { WidgetRegistry } from "#/ui/cockpit/widget-registry";
import { EmptyWidget } from "#/ui/cockpit/widgets/empty";

describe("WidgetRegistry (DAT-347)", () => {
	it("registers and resolves a widget by kind", () => {
		const registry = new WidgetRegistry().register({
			kind: "empty",
			component: EmptyWidget,
		});
		const contract = registry.resolve("empty");
		expect(contract?.kind).toBe("empty");
		expect(contract?.component).toBe(EmptyWidget);
	});

	it("returns undefined for an unregistered kind", () => {
		const registry = new WidgetRegistry();
		expect(registry.resolve("empty")).toBeUndefined();
		expect(registry.has("empty")).toBe(false);
	});

	it("the shared canvas registry has all three baseline widgets", () => {
		for (const kind of ["empty", "loading", "error"] as const) {
			expect(canvasRegistry.has(kind)).toBe(true);
		}
	});

	it("the shared canvas registry has the probe widget (DAT-576, the staging-hub default)", () => {
		expect(canvasRegistry.has("probe")).toBe(true);
	});

	it("the shared canvas registry has the result-grid widget (DAT-385)", () => {
		expect(canvasRegistry.has("result-grid")).toBe(true);
	});

	it("the shared canvas registry has the table-readiness widget (DAT-350)", () => {
		expect(canvasRegistry.has("table-readiness")).toBe(true);
	});

	it("the shared canvas registry has the column-why widget (DAT-351)", () => {
		expect(canvasRegistry.has("column-why")).toBe(true);
	});

	it("the shared canvas registry has the column-profile widget (DAT-475)", () => {
		expect(canvasRegistry.has("column-profile")).toBe(true);
	});

	it("the shared canvas registry has the add-source-progress widget (DAT-352)", () => {
		expect(canvasRegistry.has("add-source-progress")).toBe(true);
	});

	it("the shared canvas registry has the session-progress widget (DAT-435)", () => {
		expect(canvasRegistry.has("session-progress")).toBe(true);
	});

	it("the shared canvas registry has the validation widgets (DAT-440)", () => {
		expect(canvasRegistry.has("validation-list")).toBe(true);
		expect(canvasRegistry.has("validation-why")).toBe(true);
	});
});
