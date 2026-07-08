// The live equation header (DAT-712): the metric's own formula as the analyse
// surface's header — labeled operand terms with values, pretty-printed from
// the parsed DAG expression via the SAME closed grammar the composer trusts
// (metric-formula.ts, client-safe).
//
// This is the PARTS-CONTEXT layer above the generic DrillableGrid (the lead's
// layering constraint): it keys on "the result carries a structured formula
// shape", never on "this is a canvas metric node" — answer-agent results join
// by shipping the same shape. The grid stays generic; this layer OWNS the
// operand hue assignment and hands the identical map to the grid's
// `columnAccents`, so equation terms and component columns can never disagree.
//
// Binding: the equation binds to the unrestricted TOTALS on open, REBINDS to
// a row on hover/focus, and LOCKS to the pinned row on pin (hover still
// previews other rows; releasing the pointer falls back to the lock). A term
// whose column the bound row doesn't carry (additive nodes' grouped rows
// project no operand columns) keeps its total, dimmed. An operand observed
// as NULL is the honest gap — rendered as `—` plus one sentence naming it.

import { Group, Text } from "@mantine/core";
import { useReducedMotion } from "@mantine/hooks";
import type { ReactNode } from "react";

import {
	type FormulaExpr,
	parseFormulaExpression,
} from "#/duckdb/metric-formula";

/** The `/api/drill/node` open call's `node` block — the target's formula
 *  shape (parts.ts `NodeShape` + display metadata), narrowed at the fetch
 *  boundary by the caller. */
export interface NodeShapeWire {
	name: string | null;
	unit: string | null;
	targetStepId: string;
	expression: string | null;
	additive: boolean;
	operands: {
		stepId: string;
		kind: "extract" | "formula" | "constant";
		value: string | null;
	}[];
}

/** An operand's role in the formula — what the ledger ink encodes: added
 *  terms credit-green, subtracted terms debit-red, divisors (neither side of
 *  the ledger) indigo. First occurrence wins — ONE hue per operand is the
 *  point (unlike `flattenAdditive`, which deliberately counts every
 *  occurrence because it computes arithmetic, not ink). */
export type OperandRole = "added" | "subtracted" | "divisor";

export function operandRoles(expr: FormulaExpr): Map<string, OperandRole> {
	const roles = new Map<string, OperandRole>();
	const visit = (e: FormulaExpr, sign: 1 | -1, divisor: boolean): void => {
		switch (e.kind) {
			case "num":
				return;
			case "ref":
				if (!roles.has(e.name)) {
					roles.set(
						e.name,
						divisor ? "divisor" : sign === 1 ? "added" : "subtracted",
					);
				}
				return;
			case "neg":
				visit(e.operand, sign === 1 ? -1 : 1, divisor);
				return;
			case "bin": {
				visit(e.left, sign, divisor);
				const rightSign = e.op === "-" ? (sign === 1 ? -1 : 1) : sign;
				visit(e.right, rightSign, divisor || e.op === "/");
			}
		}
	};
	visit(expr, 1, false);
	return roles;
}

// Ledger ink, theme-aware via CSS `light-dark()` (Mantine sets color-scheme).
const ROLE_COLOR: Record<OperandRole, string> = {
	added: "light-dark(var(--mantine-color-teal-8), var(--mantine-color-teal-4))",
	subtracted:
		"light-dark(var(--mantine-color-red-8), var(--mantine-color-red-4))",
	divisor:
		"light-dark(var(--mantine-color-indigo-8), var(--mantine-color-indigo-4))",
};

/** The hue per operand COLUMN — computed once here and passed verbatim to the
 *  grid's `columnAccents`, the single assignment both surfaces render. */
export function operandAccents(shape: NodeShapeWire): Record<string, string> {
	if (!shape.expression) return {};
	const parsed = parseFormulaExpression(shape.expression);
	if ("refusal" in parsed) return {};
	const accents: Record<string, string> = {};
	for (const [name, role] of operandRoles(parsed.expr)) {
		accents[name] = ROLE_COLOR[role];
	}
	return accents;
}

/** Engine unit strings → display symbols. Only symbols users actually read as
 *  symbols get one; everything else shows its own word. */
export function unitSymbol(unit: string): string {
	return unit === "percentage" ? "%" : unit;
}

/** `cost_of_goods_sold` → `Cost Of Goods Sold`. */
const labelOf = (stepId: string): string =>
	stepId
		.split("_")
		.filter(Boolean)
		.map((w) => w.charAt(0).toUpperCase() + w.slice(1))
		.join(" ");

/** Deterministic number rendering (fixed locale — the modal is client-only,
 *  but hydration-safe formatting is the house rule). DECIMAL/HUGEINT arrive
 *  as STRINGS on this path; an integer string past Number's exact range
 *  passes through untouched — rounding it through a double would show wrong
 *  digits while the grid's formatCell keeps them exact. */
const formatNumber = (v: unknown): string | null => {
	if (v === null || v === undefined || v === "") return null;
	if (typeof v === "string" && /^-?\d{16,}$/.test(v)) return v;
	const n = typeof v === "number" ? v : Number(v);
	if (!Number.isFinite(n)) return null;
	return n.toLocaleString("en-US", { maximumFractionDigits: 2 });
};

const OP_SYMBOL: Record<"+" | "-" | "*" | "/", string> = {
	"+": "+",
	"-": "−",
	"*": "×",
	"/": "÷",
};

/** How many operand terms the nested equation carries before it elides to
 *  named chips (ebitda_margin-class formulas). */
const ELISION_THRESHOLD = 4;

function OperandTerm({
	label,
	value,
	color,
	dimmed,
	missing,
	transition,
}: {
	label: string;
	value: string | null;
	color: string | undefined;
	/** The bound row doesn't carry this term — its total shows, quieted. */
	dimmed: boolean;
	/** Observed-NULL: doctrine v2's honest gap. */
	missing: boolean;
	transition: boolean;
}) {
	return (
		<span
			data-testid={`equation-term-${label}`}
			style={{
				display: "inline-flex",
				flexDirection: "column",
				alignItems: "center",
				verticalAlign: "middle",
				padding: "0 2px",
				opacity: dimmed ? 0.55 : 1,
			}}
		>
			<Text component="span" size="xs" style={{ color }} fw={600}>
				{label}
			</Text>
			<Text
				component="span"
				size="sm"
				fw={600}
				style={{
					fontVariantNumeric: "tabular-nums",
					...(missing
						? {
								borderBottom: "2px dashed var(--mantine-color-default-border)",
							}
						: {}),
					...(transition ? { transition: "opacity 120ms ease" } : {}),
				}}
			>
				{value ?? "—"}
			</Text>
		</span>
	);
}

export function EquationHeader({
	shape,
	totals,
	hoverRow,
	lockedRow,
	scope,
}: {
	shape: NodeShapeWire;
	/** The unrestricted totals row (operand columns + `value`) — the open
	 *  binding and the fallback for terms a bound row doesn't carry. */
	totals: Record<string, unknown> | null;
	/** The grid row under the pointer/focus (rebind), or null. */
	hoverRow: Record<string, unknown> | null;
	/** The committed pin's row (lock), or null. */
	lockedRow: Record<string, unknown> | null;
	/** The drill scope in words — "all data", "January 2025" — for the
	 *  missing-operand sentence and the header's scope line. */
	scope: string;
}) {
	const reducedMotion = useReducedMotion();
	if (!shape.expression) return null;
	const parsed = parseFormulaExpression(shape.expression);
	if ("refusal" in parsed) return null;

	const roles = operandRoles(parsed.expr);
	const metricLabel = labelOf(shape.name ?? shape.targetStepId);
	const bound = hoverRow ?? lockedRow ?? null;
	const constantsById = new Map(
		shape.operands
			.filter((o) => o.kind === "constant")
			.map((o) => [o.stepId, o.value]),
	);

	/** Resolve one operand term's display state against the binding. */
	const termOf = (stepId: string) => {
		const constant = constantsById.get(stepId);
		if (constant !== undefined) {
			return {
				value: constant,
				dimmed: false,
				missing: false,
			};
		}
		const source =
			bound && stepId in bound
				? bound
				: totals && stepId in totals
					? totals
					: null;
		const raw = source?.[stepId];
		return {
			value: formatNumber(raw),
			// Bound to a row that doesn't decompose this term (additive grids
			// carry no operand columns) — the total shows, quieted.
			dimmed: bound !== null && !(stepId in bound),
			missing: source !== null && (raw === null || raw === undefined),
		};
	};

	const resultRaw = bound !== null ? bound.value : totals?.value;
	const resultText = formatNumber(resultRaw);
	const missingTerms = [...roles.keys()].filter((id) => termOf(id).missing);

	const term = (stepId: string): ReactNode => {
		const t = termOf(stepId);
		return (
			<OperandTerm
				key={`${stepId}-${String(t.value)}`}
				label={labelOf(stepId)}
				value={t.value}
				color={
					roles.get(stepId)
						? ROLE_COLOR[roles.get(stepId) as OperandRole]
						: undefined
				}
				dimmed={t.dimmed}
				missing={t.missing}
				transition={!reducedMotion}
			/>
		);
	};

	// Minimal-parens pretty-print: a child renders parenthesized only when its
	// operator binds looser than its parent's position requires.
	const PREC: Record<"+" | "-" | "*" | "/", number> = {
		"+": 1,
		"-": 1,
		"*": 2,
		"/": 2,
	};
	const renderExpr = (e: FormulaExpr, minPrec: number): ReactNode => {
		switch (e.kind) {
			case "num":
				return <span>{String(e.value)}</span>;
			case "ref":
				return term(e.name);
			case "neg": {
				const inner = <span>−{renderExpr(e.operand, 3)}</span>;
				// Inside anything tighter than a leftmost `+` chain, a bare unary
				// minus is ambiguous (`a − −b`) — parenthesize it.
				return minPrec > 1 ? (
					<span>
						<span style={{ opacity: 0.5 }}>(</span>
						{inner}
						<span style={{ opacity: 0.5 }}>)</span>
					</span>
				) : (
					inner
				);
			}
			case "bin": {
				const prec = PREC[e.op];
				const inner = (
					<>
						{renderExpr(e.left, prec)}
						<span style={{ padding: "0 6px", opacity: 0.7 }}>
							{OP_SYMBOL[e.op]}
						</span>
						{/* Right operand of − and ÷ must re-parenthesize equal
						    precedence: a − (b − c) ≠ a − b − c. */}
						{renderExpr(
							e.right,
							e.op === "-" || e.op === "/" ? prec + 1 : prec,
						)}
					</>
				);
				return prec < minPrec ? (
					<span>
						<span style={{ opacity: 0.5 }}>(</span>
						{inner}
						<span style={{ opacity: 0.5 }}>)</span>
					</span>
				) : (
					<span>{inner}</span>
				);
			}
		}
	};

	const elided = roles.size > ELISION_THRESHOLD;

	// The equation IS the whole header now — identity (name, kind, unit,
	// scope) lives in the modal's title row (DAT-712 iteration 2: the
	// upper-left corner was carrying the metric name twice).
	return (
		<div data-testid="equation-header">
			<Group gap={4} wrap="wrap" align="center" data-testid="equation-body">
				{elided ? (
					// Many-operand formulas elide to named chips — nesting stops
					// reading as an equation past a handful of terms.
					<>
						<span style={{ opacity: 0.7 }}>ƒ(</span>
						{[...roles.keys()].map((id) => term(id))}
						<span style={{ opacity: 0.7 }}>)</span>
					</>
				) : (
					renderExpr(parsed.expr, 0)
				)}
				<span style={{ padding: "0 6px", opacity: 0.7 }}>=</span>
				<Text
					component="span"
					size="lg"
					fw={700}
					style={{ fontVariantNumeric: "tabular-nums" }}
					data-testid="equation-result"
				>
					{resultText ?? "—"}
				</Text>
				{shape.unit && resultText !== null && (
					// Same size as the number — "49.91 %", not a fine-print word.
					<Text component="span" size="lg" c="dimmed">
						{unitSymbol(shape.unit)}
					</Text>
				)}
			</Group>
			{missingTerms.length > 0 && (
				<Text size="xs" c="dimmed" mt={2} data-testid="equation-missing">
					No {labelOf(missingTerms[0] ?? "")} booked in {scope} — {metricLabel}{" "}
					needs every input.
				</Text>
			)}
		</div>
	);
}
