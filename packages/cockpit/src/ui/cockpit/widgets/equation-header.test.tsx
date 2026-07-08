// @vitest-environment jsdom
//
// The live equation header (DAT-712): the role/sign walk that assigns the
// ledger ink, the accent map the grid shares, and the header's binding logic
// — totals on open, hover rebind, pin lock, the missing-operand sentence,
// and the many-operand elision.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { parseFormulaExpression } from "#/duckdb/metric-formula";
import { theme } from "#/ui/theme";

import {
	EquationHeader,
	type NodeShapeWire,
	operandAccents,
	operandRoles,
} from "./equation-header";

const rolesOf = (expression: string) => {
	const parsed = parseFormulaExpression(expression);
	if ("refusal" in parsed) throw new Error("unparseable test expression");
	return operandRoles(parsed.expr);
};

describe("operandRoles", () => {
	it("assigns added/subtracted/divisor from the tree walk", () => {
		const roles = rolesOf("(revenue - cost_of_goods_sold) / revenue * 100");
		// First occurrence wins: revenue enters as an added numerator term
		// before it reappears as the divisor.
		expect(roles.get("revenue")).toBe("added");
		expect(roles.get("cost_of_goods_sold")).toBe("subtracted");
	});

	it("marks a pure divisor", () => {
		const roles = rolesOf("current_assets / current_liabilities");
		expect(roles.get("current_assets")).toBe("added");
		expect(roles.get("current_liabilities")).toBe("divisor");
	});

	it("flips signs through negation and subtraction — a − (b − c)", () => {
		const roles = rolesOf("a - (b - c)");
		expect(roles.get("a")).toBe("added");
		expect(roles.get("b")).toBe("subtracted");
		expect(roles.get("c")).toBe("added"); // double flip
	});

	it("unary minus flips", () => {
		expect(rolesOf("-a + b").get("a")).toBe("subtracted");
		expect(rolesOf("-(a - b)").get("b")).toBe("added");
	});

	it("multiplication keeps the additive sign", () => {
		const roles = rolesOf("a * b - c * d");
		expect(roles.get("a")).toBe("added");
		expect(roles.get("b")).toBe("added");
		expect(roles.get("c")).toBe("subtracted");
		expect(roles.get("d")).toBe("subtracted");
	});
});

describe("operandAccents", () => {
	it("maps every operand to a color, keyed by the COLUMN name the grid uses", () => {
		const accents = operandAccents(
			shape("(revenue - cost_of_goods_sold) / revenue * 100"),
		);
		expect(Object.keys(accents).sort()).toEqual([
			"cost_of_goods_sold",
			"revenue",
		]);
		expect(accents.revenue).toContain("teal");
		expect(accents.cost_of_goods_sold).toContain("red");
	});

	it("is empty for a bare extract (no expression)", () => {
		expect(operandAccents({ ...shape("a + b"), expression: null })).toEqual({});
	});
});

function shape(
	expression: string,
	overrides: Partial<NodeShapeWire> = {},
): NodeShapeWire {
	const parsed = parseFormulaExpression(expression);
	const refs =
		"refusal" in parsed
			? []
			: [
					...new Set(
						(function walk(e): string[] {
							switch (e.kind) {
								case "ref":
									return [e.name];
								case "num":
									return [];
								case "neg":
									return walk(e.operand);
								case "bin":
									return [...walk(e.left), ...walk(e.right)];
							}
						})(parsed.expr),
					),
				];
	return {
		name: "gross_margin",
		unit: "percentage",
		targetStepId: "gross_margin",
		expression,
		additive: false,
		operands: refs.map((stepId) => ({
			stepId,
			kind: "extract" as const,
			value: null,
		})),
		...overrides,
	};
}

function renderHeader(props: Partial<Parameters<typeof EquationHeader>[0]>) {
	return render(
		<MantineProvider theme={theme} env="test">
			<EquationHeader
				shape={shape("(revenue - cost_of_goods_sold) / revenue * 100")}
				totals={{ revenue: 800, cost_of_goods_sold: 200, value: 75 }}
				hoverRow={null}
				lockedRow={null}
				scope="all data"
				{...props}
			/>
		</MantineProvider>,
	);
}

afterEach(cleanup);

describe("EquationHeader", () => {
	it("binds to the totals on open — terms, result, unit symbol", () => {
		renderHeader({});
		expect(screen.getByTestId("equation-result").textContent).toBe("75");
		// Unit renders as its symbol next to the result ("75 %", not a word).
		expect(screen.getByTestId("equation-body").textContent).toContain("%");
		// revenue appears TWICE (numerator + divisor) — both bind.
		for (const revenue of screen.getAllByTestId("equation-term-Revenue")) {
			expect(revenue.textContent).toContain("800");
		}
		expect(
			screen.getByTestId("equation-term-Cost Of Goods Sold").textContent,
		).toContain("200");
		expect(screen.queryByTestId("equation-missing")).toBeNull();
	});

	it("rebinds to the hover row; a term the row doesn't carry keeps its total", () => {
		renderHeader({
			hoverRow: { revenue: 100, cost_of_goods_sold: 40, value: 60 },
		});
		expect(screen.getByTestId("equation-result").textContent).toBe("60");
		expect(
			screen.getAllByTestId("equation-term-Revenue")[0]?.textContent,
		).toContain("100");
	});

	it("hover previews OVER a lock; without hover the lock binds", () => {
		renderHeader({
			hoverRow: { revenue: 100, cost_of_goods_sold: 40, value: 60 },
			lockedRow: { revenue: 50, cost_of_goods_sold: 10, value: 80 },
		});
		expect(screen.getByTestId("equation-result").textContent).toBe("60");
		cleanup();
		renderHeader({
			lockedRow: { revenue: 50, cost_of_goods_sold: 10, value: 80 },
		});
		expect(screen.getByTestId("equation-result").textContent).toBe("80");
	});

	it("an observed-NULL operand is the honest gap: dash + the sentence", () => {
		renderHeader({
			hoverRow: { revenue: null, cost_of_goods_sold: 40, value: null },
		});
		expect(screen.getByTestId("equation-result").textContent).toBe("—");
		expect(screen.getByTestId("equation-missing").textContent).toBe(
			"No Revenue booked in all data — Gross Margin needs every input.",
		);
	});

	it("a constant operand renders its declared value inline", () => {
		renderHeader({
			shape: shape("(accounts_receivable / revenue) * days_in_period", {
				name: "dso",
				unit: "days",
				targetStepId: "dso",
				operands: [
					{ stepId: "accounts_receivable", kind: "extract", value: null },
					{ stepId: "revenue", kind: "extract", value: null },
					{ stepId: "days_in_period", kind: "constant", value: "30" },
				],
			}),
			totals: { accounts_receivable: 180, revenue: 800, value: 6.75 },
		});
		expect(
			screen.getByTestId("equation-term-Days In Period").textContent,
		).toContain("30");
	});

	it("elides past the threshold: chips instead of nested structure", () => {
		renderHeader({
			shape: shape("a + b + c + d + e", { name: "many" }),
			totals: { a: 1, b: 2, c: 3, d: 4, e: 5, value: 15 },
		});
		expect(screen.getByTestId("equation-body").textContent).toContain("ƒ(");
		// Every operand is still present as a chip.
		for (const id of ["A", "B", "C", "D", "E"]) {
			expect(screen.getByTestId(`equation-term-${id}`)).toBeDefined();
		}
	});

	it("passes exact big-integer strings through instead of rounding via double", () => {
		renderHeader({
			totals: {
				revenue: "12345678901234567893",
				cost_of_goods_sold: 200,
				value: 75,
			},
		});
		expect(
			screen.getAllByTestId("equation-term-Revenue")[0]?.textContent,
		).toContain("12345678901234567893");
	});

	it("renders nothing without an expression (bare measures)", () => {
		const { container } = renderHeader({
			shape: shape("a + b", { expression: null }),
		});
		expect(container.querySelector("[data-testid=equation-header]")).toBeNull();
	});
});
