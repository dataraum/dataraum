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

	it("the shared canvas registry has the schema-preview widget (DAT-381)", () => {
		expect(canvasRegistry.has("schema-preview")).toBe(true);
	});

	it("the shared canvas registry has the concept-frame widget (DAT-382)", () => {
		expect(canvasRegistry.has("concept-frame")).toBe(true);
	});

	it("the shared canvas registry has the result-grid widget (DAT-385)", () => {
		expect(canvasRegistry.has("result-grid")).toBe(true);
	});

	it("the shared canvas registry has the selected-source widget (DAT-398)", () => {
		expect(canvasRegistry.has("selected-source")).toBe(true);
	});

	it("the shared canvas registry has the table-readiness widget (DAT-350)", () => {
		expect(canvasRegistry.has("table-readiness")).toBe(true);
	});

	it("the shared canvas registry has the column-why widget (DAT-351)", () => {
		expect(canvasRegistry.has("column-why")).toBe(true);
	});

	it("the shared canvas registry has the add-source-progress widget (DAT-352)", () => {
		expect(canvasRegistry.has("add-source-progress")).toBe(true);
	});
});
