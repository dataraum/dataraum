// SPIKE (not a test): compose EVERY metric from clause parts using
// @uwdata/mosaic-sql as the builder, on the live workspace.
//
// Per metric:
//   1. SCALAR: build the metric as mosaic CTEs (extract = parts, constant =
//      literal, formula = closed-grammar arithmetic over scalar subqueries) and
//      compare the value against the DAT-702 fused-string composer
//      (composeMetricNodeSql — itself parity-pinned to the engine).
//   2. GROUPED, per axis (union over the carriers' facts): each carrier gets
//      dim + GROUP BY, carriers join FULL … USING (dim) via mosaic join nodes,
//      the formula runs per row. Absence is MEASURE-LOCAL: COALESCE(ref, 0)
//      iff the referenced step is a SUM/COUNT extract; formula carriers stay
//      bare (NULL propagates — recorded, not hidden).
//      Checks: additive metrics (+/- only) must have Σ groups == scalar;
//      ratio metrics report their NULL (undefined) group counts.
//
// Run:  cd packages/cockpit && bun spikes/drill-metrics.spike.ts

import {
	Query,
	type SelectQuery,
	column,
	join,
	verbatim,
} from "@uwdata/mosaic-sql";
import { and, asc, desc, eq, like } from "drizzle-orm";

import { config } from "#/config";
import { metadataDb } from "#/db/metadata/client";
import {
	currentEnrichedViews,
	currentLifecycleArtifacts,
	currentSliceDefinitions,
	sqlSnippets,
} from "#/db/metadata/schema";
import { applyEngineScope, closeLake, withLakeConnection } from "#/duckdb/lake";
import {
	composeMetricNodeSql,
	type MetricDrillStep,
} from "#/duckdb/metric-compose";
import {
	type FormulaExpr,
	formulaRefs,
	parseFormulaExpression,
} from "#/duckdb/metric-formula";
import {
	type MetricStep,
	parseMetricDag,
} from "#/tools/operating-model-graph";

import { extractParts, num, run, type SnippetParts } from "./spike-lib";

// --- formula rendering over carriers (spike-local grouped renderer) ------------

const floatLit = (v: number): string =>
	Number.isInteger(v) ? `${v}.0` : String(v);

/** Render a formula expression; `carriers` are dim-carrying CTE deps (referenced
 *  as `"dep"."value"`), everything else stays an engine-style scalar subquery.
 *  `zeroAbsent` marks refs whose absence is a true zero (SUM/COUNT extracts). */
function render(
	expr: FormulaExpr,
	carriers: ReadonlySet<string>,
	zeroAbsent: ReadonlySet<string>,
): string {
	switch (expr.kind) {
		case "ref": {
			if (!carriers.has(expr.name)) return `(SELECT value FROM ${expr.name})`;
			const ref = `"${expr.name}"."value"`;
			return zeroAbsent.has(expr.name) ? `COALESCE(${ref}, 0)` : ref;
		}
		case "num":
			return floatLit(expr.value);
		case "neg":
			return `-${render(expr.operand, carriers, zeroAbsent)}`;
		case "bin": {
			const l = render(expr.left, carriers, zeroAbsent);
			const r = render(expr.right, carriers, zeroAbsent);
			const rhs = expr.op === "/" ? `NULLIF(${r}, 0)` : r;
			return `(${l} ${expr.op} ${rhs})`;
		}
	}
}

const opsOf = (expr: FormulaExpr): string[] => {
	switch (expr.kind) {
		case "bin":
			return [expr.op, ...opsOf(expr.left), ...opsOf(expr.right)];
		case "neg":
			return opsOf(expr.operand);
		default:
			return [];
	}
};

// --- per-metric assembly --------------------------------------------------------

interface StepCtx {
	step: MetricStep;
	parts: SnippetParts | null; // extracts only
	expr: FormulaExpr | null; // formulas only
}

/** Topological order of the output step's subtree via parsed refs. */
function orderSteps(
	steps: Map<string, StepCtx>,
	outputId: string,
): StepCtx[] | { fail: string } {
	const order: StepCtx[] = [];
	const done = new Set<string>();
	const visit = (id: string): string | null => {
		if (done.has(id)) return null;
		done.add(id);
		const ctx = steps.get(id);
		if (!ctx) return null; // not a step (engine treats as cache leaf)
		const refs =
			ctx.expr !== null
				? formulaRefs(ctx.expr)
				: ctx.step.kind === "extract"
					? []
					: [];
		for (const r of refs) {
			const err = visit(r);
			if (err) return err;
		}
		order.push(ctx);
		return null;
	};
	const err = visit(outputId);
	if (err) return { fail: err };
	return order;
}

function buildMetricQuery(
	order: StepCtx[],
	outputId: string,
	dim: string | null,
): { sql: string } | { fail: string } {
	const ctes: Record<string, SelectQuery> = {};
	const carriers = new Set<string>(); // dim-carrying CTEs (grouped mode)
	const zeroAbsent = new Set<string>();

	for (const { step, parts, expr } of order) {
		if (step.kind === "extract") {
			if (!parts) return { fail: `no parts for extract '${step.stepId}'` };
			// mosaic auto-aliases bare expressions with their own text — computed
			// items must be {alias: expr} objects (their idiom).
			let q = Query.from(parts.fromText);
			if (dim) {
				q = q
					.select(column(dim), { value: verbatim(parts.valueExpr) })
					.groupby(column(dim));
				carriers.add(step.stepId);
				if (step.aggregation === "sum" || step.aggregation === "count") {
					zeroAbsent.add(step.stepId);
				}
			} else {
				q = q.select({ value: verbatim(parts.valueExpr) });
			}
			if (parts.whereText) q = q.where(verbatim(parts.whereText));
			ctes[step.stepId] = q;
			continue;
		}
		if (step.kind === "constant") {
			const n = Number(step.value);
			if (!Number.isFinite(n))
				return { fail: `constant '${step.stepId}' not numeric` };
			ctes[step.stepId] = Query.select({ value: verbatim(String(n)) });
			continue;
		}
		if (!expr) return { fail: `formula '${step.stepId}' has no expression` };
		const deps = formulaRefs(expr).filter((r) => carriers.has(r));
		const rendered = render(expr, carriers, zeroAbsent);
		// Zero-absence propagates bottom-up through purely-additive formulas
		// (ops ⊆ {+,-} and every dim-carrying ref itself zero-absent): the
		// step's absence for a group is then a true zero, exactly like a SUM
		// extract's. Found by data: ebitda = operating_income + depreciation
		// lost the depreciation-only groups when operating_income stayed bare.
		if (
			opsOf(expr).every((o) => o === "+" || o === "-") &&
			deps.every((d) => zeroAbsent.has(d))
		) {
			zeroAbsent.add(step.stepId);
		}
		if (dim && deps.length > 0) {
			// FULL JOIN … USING (dim) spine over the dim-carrying deps.
			const fromArg =
				deps.length === 1
					? deps[0]
					: deps
							.slice(1)
							.reduce<Parameters<typeof join>[0]>(
								(acc, d) => join(acc, d, { type: "FULL", using: [dim] }),
								deps[0] as string,
							);
			ctes[step.stepId] = Query.from(fromArg as never).select(column(dim), {
				value: verbatim(rendered),
			});
			carriers.add(step.stepId);
		} else {
			ctes[step.stepId] = Query.select({ value: verbatim(rendered) });
		}
	}

	// No star: mosaic would alias it. Select the output CTE's columns explicitly
	// (grouped output carries the dim; scalar carries value only).
	const outCols =
		dim && carriers.has(outputId) ? [column(dim), column("value")] : [column("value")];
	const sqlText = String(Query.with(ctes).select(...outCols).from(outputId));
	return { sql: sqlText };
}

// --- main ------------------------------------------------------------------------

async function main(): Promise<void> {
	const metricRows = await metadataDb
		.select({
			key: currentLifecycleArtifacts.artifactKey,
			dag: currentLifecycleArtifacts.graphDefinition,
		})
		.from(currentLifecycleArtifacts)
		.where(eq(currentLifecycleArtifacts.artifactType, "metric"))
		.orderBy(asc(currentLifecycleArtifacts.artifactKey));

	const snippetRows = await metadataDb
		.select({
			standardField: sqlSnippets.standardField,
			sql: sqlSnippets.sql,
			failureCount: sqlSnippets.failureCount,
		})
		.from(sqlSnippets)
		.where(
			and(
				eq(sqlSnippets.schemaMappingId, config.dataraumWorkspaceId),
				like(sqlSnippets.source, "graph:%"),
				eq(sqlSnippets.snippetType, "extract"),
			),
		)
		.orderBy(desc(sqlSnippets.updatedAt));
	const sqlByField = new Map<string, string>();
	const decided = new Set<string>();
	for (const r of snippetRows) {
		if (!r.standardField || decided.has(r.standardField)) continue;
		decided.add(r.standardField);
		if ((r.failureCount ?? 0) === 0 && r.sql)
			sqlByField.set(r.standardField, r.sql);
	}

	const views = await metadataDb
		.select({
			viewName: currentEnrichedViews.viewName,
			factTableId: currentEnrichedViews.factTableId,
			dimensionColumns: currentEnrichedViews.dimensionColumns,
			isGrainVerified: currentEnrichedViews.isGrainVerified,
		})
		.from(currentEnrichedViews)
		.orderBy(asc(currentEnrichedViews.viewTableId));
	const viewByName = new Map(views.map((v) => [v.viewName ?? "", v]));
	const sliceRows = await metadataDb
		.select({
			tableId: currentSliceDefinitions.tableId,
			columnName: currentSliceDefinitions.columnName,
		})
		.from(currentSliceDefinitions)
		.orderBy(asc(currentSliceDefinitions.slicePriority));

	const report: string[] = [];

	await withLakeConnection(async (conn) => {
		await applyEngineScope(conn);

		// Parts per measure field, extracted once.
		const partsByField = new Map<string, SnippetParts | { miss: string }>();
		for (const [field, snippetSql] of sqlByField) {
			partsByField.set(field, await extractParts(conn, snippetSql));
		}

		for (const m of metricRows) {
			const dag = parseMetricDag(m.dag);
			const key = m.key ?? "?";
			if (!dag) {
				report.push(`\n## ${key} — SKIP (no parseable DAG)`);
				continue;
			}
			const outputStep =
				dag.steps.find((s) => s.outputStep) ??
				dag.steps.find(
					(s) => !dag.steps.some((o) => o.dependsOn.includes(s.stepId)),
				) ??
				dag.steps[0];
			if (!outputStep) continue;

			const steps = new Map<string, StepCtx>();
			let hole: string | null = null;
			for (const s of dag.steps) {
				let parts: SnippetParts | null = null;
				let expr: FormulaExpr | null = null;
				if (s.kind === "extract") {
					const p = s.standardField
						? partsByField.get(s.standardField)
						: undefined;
					if (p && !("miss" in p)) parts = p;
				} else if (s.kind === "formula" && s.expression) {
					const parsed = parseFormulaExpression(s.expression);
					if (!("refusal" in parsed)) expr = parsed.expr;
				}
				steps.set(s.stepId, { step: s, parts, expr });
			}

			const ordered = orderSteps(steps, outputStep.stepId);
			if ("fail" in ordered) {
				report.push(`\n## ${key} — FAIL order: ${ordered.fail}`);
				continue;
			}
			for (const ctx of ordered) {
				if (ctx.step.kind === "extract" && !ctx.parts) {
					hole = ctx.step.standardField ?? ctx.step.stepId;
					break;
				}
			}
			if (hole) {
				report.push(`\n## ${key} — SKIP (hole: no accepted parts for '${hole}')`);
				continue;
			}

			// 1) scalar: mosaic build vs the fused-string reference composer.
			const scalarQ = buildMetricQuery(ordered, outputStep.stepId, null);
			const refSteps: MetricDrillStep[] = dag.steps.map((s) => ({
				stepId: s.stepId,
				kind: s.kind,
				sql:
					s.kind === "extract" && s.standardField
						? (sqlByField.get(s.standardField) ?? null)
						: null,
				expression: s.expression,
				value: s.value,
				dependsOn: s.dependsOn,
				outputStep: s.outputStep,
			}));
			const refQ = composeMetricNodeSql(refSteps, outputStep.stepId);
			let scalarLine = "scalar: ";
			let scalarVal: number | null = null;
			if ("fail" in scalarQ) scalarLine += `mosaic FAIL ${scalarQ.fail}`;
			else {
				const r = await run(conn, scalarQ.sql);
				scalarVal = "rows" in r ? num(r.rows[0]?.value) : null;
				scalarLine +=
					"rows" in r ? `mosaic=${String(scalarVal)}` : `mosaic ERR ${r.error}`;
			}
			if ("refusal" in refQ) scalarLine += ` | ref REFUSE ${refQ.refusal}`;
			else {
				const r = await run(conn, refQ.sql);
				const rv = "rows" in r ? num(r.rows[0]?.value) : null;
				scalarLine += ` | ref=${String(rv)}`;
				scalarLine +=
					scalarVal !== null && rv !== null && Math.abs(scalarVal - rv) < 1e-9
						? " ✓parity"
						: " ⚠️DIVERGE";
			}

			// Additivity: every op in every reachable formula ∈ {+,-}.
			const ops = ordered.flatMap((c) => (c.expr ? opsOf(c.expr) : []));
			const additive = ops.every((o) => o === "+" || o === "-");

			// Axes: union over the carriers' facts (curated + substrate).
			const facts = new Set<string>();
			for (const c of ordered) {
				if (c.parts) {
					const v = viewByName.get(c.parts.fromText);
					if (v?.factTableId) facts.add(v.factTableId);
				}
			}
			const axes = new Map<string, "curated" | "substrate">();
			for (const s of sliceRows) {
				if (s.tableId && facts.has(s.tableId) && s.columnName)
					if (!axes.has(s.columnName)) axes.set(s.columnName, "curated");
			}
			for (const v of views) {
				if (
					v.factTableId &&
					facts.has(v.factTableId) &&
					v.isGrainVerified &&
					Array.isArray(v.dimensionColumns)
				) {
					for (const c of v.dimensionColumns as unknown[]) {
						if (typeof c === "string" && !axes.has(c))
							axes.set(c, "substrate");
					}
				}
			}

			report.push(
				`\n## ${key}  (${additive ? "additive" : "non-additive"}, ${facts.size} fact(s), ${axes.size} axes) — ${scalarLine}`,
			);

			for (const [dim, origin] of axes) {
				const q = buildMetricQuery(ordered, outputStep.stepId, dim);
				if ("fail" in q) {
					report.push(`  [${origin}] ${dim}: build FAIL ${q.fail}`);
					continue;
				}
				const r = await run(conn, q.sql);
				if ("error" in r) {
					report.push(`  [${origin}] ${dim}: ERR ${r.error}`);
					continue;
				}
				const groups = r.rows.length;
				const nulls = r.rows.filter((row) => row.value === null).length;
				let check = "";
				if (additive && scalarVal !== null) {
					const sum = r.rows.reduce((s, row) => s + (num(row.value) ?? 0), 0);
					check =
						Math.abs(sum - scalarVal) < 1e-6
							? " Σ=scalar"
							: ` Σ≠scalar (Σ=${sum})`;
				}
				report.push(
					`  [${origin}] ${dim}: ok ${groups} groups${nulls ? `, ${nulls} NULL` : ""}${check}`,
				);
			}
		}
	});
	await closeLake();
	console.log(report.join("\n"));
}

await main();
