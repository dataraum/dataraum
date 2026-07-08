// REVIEW SPIKE B (delete after): drive composeNodeQuery output through a real
// in-memory DuckDB for the pathological cases.
import { DuckDBInstance } from "@duckdb/node-api";
import type { NodeStep } from "#/duckdb/parts";
import { composeNodeQuery, composeNodeTotals } from "#/duckdb/parts";

const instance = await DuckDBInstance.create(":memory:");
const conn = await instance.connect();

async function q(sql: string, params?: (string | number | boolean | null)[]) {
	const reader = params
		? await conn.runAndReadAll(sql, params)
		: await conn.runAndReadAll(sql);
	return reader.getRowObjectsJson();
}

await q(`CREATE TABLE fact ("we""ird" DATE, region VARCHAR, amt INT)`);
await q(`INSERT INTO fact VALUES
  (DATE '2025-01-15','eu',10),(DATE '2025-02-03','us',20),(NULL,'eu',30)`);

const steps: NodeStep[] = [
	{
		stepId: "rev",
		kind: "extract",
		parts: { selectExpr: "SUM(amt)", relation: "fact", where: [] },
		expression: null,
		value: null,
		dependsOn: [],
		outputStep: true,
	},
];

// 1. slice on a column whose name contains a double quote, grained
const c1 = composeNodeQuery(steps, undefined, {
	slices: [{ column: 'we"ird', grain: "1M" }],
	pins: [],
});
if ("refusal" in c1) console.log("REFUSAL c1:", c1.refusal);
else {
	console.log("SQL c1:\n", c1.sql);
	try {
		console.log("rows:", JSON.stringify(await q(c1.sql, c1.params)));
	} catch (e) {
		console.log("EXEC ERR c1:", String(e).split("\n")[0]);
	}
}

// 2. pin on the measure alias name "value" — alias capture in WHERE?
const c2 = composeNodeQuery(steps, undefined, {
	slices: [],
	pins: [{ column: "value", value: 60 }],
});
if ("refusal" in c2) console.log("REFUSAL c2:", c2.refusal);
else {
	console.log("SQL c2:\n", c2.sql);
	try {
		console.log("rows:", JSON.stringify(await q(c2.sql, c2.params)));
	} catch (e) {
		console.log("EXEC ERR c2:", String(e).split("\n")[0]);
	}
}

// 3. additive metric: pin on "_observed"?
const addSteps: NodeStep[] = [
	{
		stepId: "sales",
		kind: "extract",
		parts: { selectExpr: "SUM(amt)", relation: "fact", where: [] },
		expression: null,
		value: null,
		dependsOn: [],
		outputStep: false,
	},
	{
		stepId: "gp",
		kind: "formula",
		parts: null,
		expression: "sales - sales",
		value: null,
		dependsOn: ["sales"],
		outputStep: true,
	},
];
const c3 = composeNodeQuery(addSteps, undefined, {
	slices: [{ column: "region" }],
	pins: [{ column: "_observed", value: 1 }],
});
if ("refusal" in c3) console.log("REFUSAL c3:", c3.refusal);
else {
	console.log("SQL c3:\n", c3.sql);
	try {
		console.log("rows:", JSON.stringify(await q(c3.sql, c3.params)));
	} catch (e) {
		console.log("EXEC ERR c3:", String(e).split("\n")[0]);
	}
}

// 4. a + a double-ref additive: contributes twice?
const dblSteps: NodeStep[] = [
	{
		stepId: "sales",
		kind: "extract",
		parts: { selectExpr: "SUM(amt)", relation: "fact", where: [] },
		expression: null,
		value: null,
		dependsOn: [],
		outputStep: false,
	},
	{
		stepId: "dbl",
		kind: "formula",
		parts: null,
		expression: "sales + sales",
		value: null,
		dependsOn: ["sales"],
		outputStep: true,
	},
];
const c4 = composeNodeQuery(dblSteps, undefined, {
	slices: [{ column: "region" }],
	pins: [],
});
if ("refusal" in c4) console.log("REFUSAL c4:", c4.refusal);
else {
	try {
		console.log("dbl rows:", JSON.stringify(await q(c4.sql, c4.params)));
	} catch (e) {
		console.log("EXEC ERR c4:", String(e).split("\n")[0]);
	}
}

// 5. totals for the same — operand projection of a two-ref formula
const c5 = composeNodeTotals(dblSteps, undefined);
if ("refusal" in c5) console.log("REFUSAL c5:", c5.refusal);
else {
	console.log("SQL c5:\n", c5.sql);
	try {
		console.log("totals rows:", JSON.stringify(await q(c5.sql, c5.params)));
	} catch (e) {
		console.log("EXEC ERR c5:", String(e).split("\n")[0]);
	}
}

// 6. NULL-date bucket pin end-to-end through the composer
const c6 = composeNodeQuery(steps, undefined, {
	slices: [{ column: 'we"ird', grain: "1M" }],
	pins: [{ column: 'we"ird', value: null, grain: "1M" }],
});
if ("refusal" in c6) console.log("REFUSAL c6:", c6.refusal);
else {
	try {
		console.log("null-pin rows:", JSON.stringify(await q(c6.sql, c6.params)));
	} catch (e) {
		console.log("EXEC ERR c6:", String(e).split("\n")[0]);
	}
}
