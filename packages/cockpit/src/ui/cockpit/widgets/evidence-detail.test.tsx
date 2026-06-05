// @vitest-environment jsdom
//
// Render tests for the shared EvidenceDetail component (DAT-437) — the
// hierarchical key→value rendering of a detector's sanitized evidence `detail`
// string. Shared by column-why today and the why_table / why_relationship
// widgets (DAT-434).
//
// Assertions read the COMPONENT node (`evidence-detail` testid), never the
// container — MantineProvider injects <style> tags whose CSS is full of the
// very JSON punctuation these tests forbid.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { EvidenceDetail } from "#/ui/cockpit/widgets/evidence-detail";
import { theme } from "#/ui/theme";

function renderDetail(detail: string): HTMLElement {
	render(
		<MantineProvider theme={theme} env="test">
			<EvidenceDetail detail={detail} />
		</MantineProvider>,
	);
	return screen.getByTestId("evidence-detail");
}

describe("EvidenceDetail (DAT-437)", () => {
	afterEach(() => cleanup());

	it("renders a flat object as key→value rows, not a JSON blob", () => {
		const el = renderDetail('{"metric":"undeclared_ratio","value":0.8}');
		const text = el.textContent ?? "";
		expect(text).toContain("metric:");
		expect(text).toContain("undeclared_ratio");
		expect(text).toContain("0.8");
		// No JSON punctuation reaches the DOM — this is the whole point.
		expect(text).not.toContain("{");
		expect(text).not.toContain('"');
	});

	it("renders an array as repeated groups, one per element", () => {
		const el = renderDetail(
			'[{"metric":"alpha_ratio","value":1},{"metric":"beta_ratio","value":2}]',
		);
		const text = el.textContent ?? "";
		// Both elements render their rows, no array punctuation.
		expect(text).toContain("alpha_ratio");
		expect(text).toContain("beta_ratio");
		expect(text).not.toContain("[");
	});

	it("indents a nested object under its key", () => {
		const el = renderDetail('{"outer":{"inner_key":"inner_value"},"flat":3}');
		const text = el.textContent ?? "";
		expect(text).toContain("outer:");
		expect(text).toContain("inner_key");
		expect(text).toContain("inner_value");
		expect(text).toContain("flat");
	});

	it("truncates a long value and carries the full text in title", () => {
		const long = "x".repeat(200);
		const el = renderDetail(`{"note":"${long}"}`);
		const text = el.textContent ?? "";
		// Truncated in the DOM…
		expect(text).toContain("…");
		expect(text).not.toContain(long);
		// …with the full value hover-reachable.
		expect(el.querySelector(`[title="${long}"]`)).toBeTruthy();
	});

	it("renders a plain-string (non-JSON) detail as-is — never blanks the cell", () => {
		const el = renderDetail("plain detector note");
		expect(el.textContent).toContain("plain detector note");
	});

	it("bounds the plain-string branch like the parsed branch (no unbounded cell)", () => {
		const el = renderDetail("x ".repeat(5000));
		expect(el.style.maxWidth).toBe("360px");
		expect(el.style.maxHeight).toBe("200px");
		expect(el.style.overflowY).toBe("auto");
	});

	it("caps a long array at 20 elements with a '+N more' tail", () => {
		const items = Array.from({ length: 25 }, (_, i) => ({
			metric: `metric_${i}`,
		}));
		const el = renderDetail(JSON.stringify(items));
		const text = el.textContent ?? "";
		expect(text).toContain("metric_0");
		expect(text).toContain("metric_19");
		// The 21st element never reaches the DOM — the tail counts it instead.
		expect(text).not.toContain("metric_20");
		expect(text).toContain("+5 more");
	});

	it("renders a dash for an empty detail", () => {
		const el = renderDetail("");
		expect(el.textContent).toContain("—");
	});

	it("renders null values, empty strings, and empty containers as dashes", () => {
		const el = renderDetail('{"a":null,"b":{},"c":[],"d":""}');
		const text = el.textContent ?? "";
		expect(text).toContain("a:");
		expect(text).toContain("d:");
		// All four render a dash rather than literal null/{}/[] or a hollow cell.
		expect(text).not.toContain("null");
		expect((text.match(/—/g) ?? []).length).toBeGreaterThanOrEqual(4);
	});
});
