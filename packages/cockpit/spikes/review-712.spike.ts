// REVIEW SPIKE (delete after): empirically ground the DAT-712 suspicions
// against the installed @duckdb/node-api. In-memory, no lake.
import { DuckDBInstance } from "@duckdb/node-api";

const instance = await DuckDBInstance.create(":memory:");
const conn = await instance.connect();

async function q(sql: string, params?: (string | number | boolean | null)[]) {
	const reader = params
		? await conn.runAndReadAll(sql, params)
		: await conn.runAndReadAll(sql);
	return reader.getRowObjectsJson();
}

async function tryQ(
	label: string,
	sql: string,
	params?: (string | number | boolean | null)[],
) {
	try {
		const rows = await q(sql, params);
		console.log(`OK   ${label}:`, JSON.stringify(rows));
	} catch (e) {
		console.log(`ERR  ${label}:`, String(e).split("\n")[0]);
	}
}

// --- setup: a fact table with every temporal flavor ---
await q(`CREATE TABLE t (
  d DATE, ts TIMESTAMP, tstz TIMESTAMPTZ, s VARCHAR, i INTEGER,
  amt DECIMAL(18,3), big HUGEINT
)`);
await q(`INSERT INTO t VALUES
  (DATE '2025-01-15', TIMESTAMP '2025-01-15 10:30:00', TIMESTAMPTZ '2025-01-15 10:30:00+00', 'a', 1, 10.5, 12345678901234567890),
  (DATE '2025-01-20', TIMESTAMP '2025-01-20 23:59:59', TIMESTAMPTZ '2025-01-20 23:59:59+00', 'b', 2, 20.25, 1),
  (DATE '2025-02-03', TIMESTAMP '2025-02-03 00:00:00', TIMESTAMPTZ '2025-02-03 00:00:00+00', 'a', 3, 30.125, 2),
  (NULL, NULL, NULL, 'c', 4, NULL, NULL)`);

// 1. JSON serialization shapes (the grid/pin round-trip source)
console.log("--- 1. JSON serialization of grouped bucket rows ---");
console.log(
	JSON.stringify(
		await q(
			`SELECT time_bucket(INTERVAL '1 months', "d") AS d, SUM(amt) AS value FROM t GROUP BY time_bucket(INTERVAL '1 months', "d") ORDER BY 1`,
		),
	),
);
console.log(
	JSON.stringify(
		await q(
			`SELECT time_bucket(INTERVAL '1 months', "ts") AS ts, SUM(amt) AS value FROM t GROUP BY time_bucket(INTERVAL '1 months', "ts") ORDER BY 1`,
		),
	),
);
await tryQ(
	"tstz bucket",
	`SELECT time_bucket(INTERVAL '1 months', "tstz") AS tstz, SUM(amt) AS value FROM t GROUP BY time_bucket(INTERVAL '1 months', "tstz") ORDER BY 1`,
);

// 2. Pin round-trip: bind the JSON string back as $1
console.log("--- 2. pin round-trip (DATE) ---");
await tryQ(
	"date pin '2025-01-01'",
	`SELECT SUM(amt) AS value FROM t WHERE (time_bucket(INTERVAL '1 months', "d") = $1)`,
	["2025-01-01"],
);
console.log("--- 2b. pin round-trip (TIMESTAMP) — exact JSON string ---");
const tsRows = await q(
	`SELECT time_bucket(INTERVAL '1 months', "ts") AS b FROM t WHERE ts IS NOT NULL LIMIT 1`,
);
const tsStr = String(tsRows[0]?.b);
console.log("serialized ts bucket:", JSON.stringify(tsStr));
await tryQ(
	"ts pin (serialized string)",
	`SELECT SUM(amt) AS value FROM t WHERE (time_bucket(INTERVAL '1 months', "ts") = $1)`,
	[tsStr],
);
console.log("--- 2c. pin round-trip (TIMESTAMPTZ) ---");
try {
	const tzRows = await q(
		`SELECT time_bucket(INTERVAL '1 months', "tstz") AS b FROM t WHERE tstz IS NOT NULL LIMIT 1`,
	);
	const tzStr = String(tzRows[0]?.b);
	console.log("serialized tstz bucket:", JSON.stringify(tzStr));
	await tryQ(
		"tstz pin (serialized string)",
		`SELECT SUM(amt) AS value FROM t WHERE (time_bucket(INTERVAL '1 months', "tstz") = $1)`,
		[tzStr],
	);
} catch (e) {
	console.log("tstz path failed:", String(e).split("\n")[0]);
}

// 3. comparison cast direction: does '2025-1-1' (non-canonical) match DATE?
console.log("--- 3. cast direction ---");
await tryQ("DATE = '2025-1-1' param", `SELECT DATE '2025-01-01' = $1 AS eq`, [
	"2025-1-1",
]);
await tryQ("DATE = 'garbage' param", `SELECT DATE '2025-01-01' = $1 AS eq`, [
	"garbage",
]);

// 4. interval plural + '1M' danger check
console.log("--- 4. interval spellings ---");
await tryQ("'1 months' plural", `SELECT INTERVAL '1 months' AS v`);
await tryQ(
	"time_bucket weeks Monday",
	`SELECT time_bucket(INTERVAL '1 weeks', DATE '2026-07-08') AS wk, dayname(time_bucket(INTERVAL '1 weeks', DATE '2026-07-08')) AS dow`,
);
await tryQ(
	"time_bucket 3 months quarter alignment",
	`SELECT time_bucket(INTERVAL '3 months', DATE '2025-05-15') AS b`,
);

// 5. grain on non-temporal columns
console.log("--- 5. non-temporal grain ---");
await tryQ(
	"time_bucket on VARCHAR col",
	`SELECT time_bucket(INTERVAL '1 months', "s") AS b FROM t`,
);
await tryQ(
	"time_bucket on VARCHAR col DESCRIBE",
	`DESCRIBE SELECT time_bucket(INTERVAL '1 months', "s") AS b FROM t`,
);
await tryQ(
	"time_bucket on INTEGER col",
	`SELECT time_bucket(INTERVAL '1 months', "i") AS b FROM t`,
);
await tryQ(
	"sub-day grain on DATE",
	`SELECT time_bucket(INTERVAL '2 hours', "d") AS b FROM t WHERE d IS NOT NULL LIMIT 1`,
);
await tryQ(
	"sub-day grain on DATE DESCRIBE",
	`DESCRIBE SELECT time_bucket(INTERVAL '2 hours', "d") AS b FROM t`,
);

// 6. NULL pin: IS NULL over bucket
console.log("--- 6. NULL pin ---");
await tryQ(
	"bucket IS NULL",
	`SELECT COUNT(*) AS n, SUM(i) AS si FROM t WHERE (time_bucket(INTERVAL '1 months', "d") IS NULL)`,
);

// 7. alias shadowing: WHERE + GROUP BY reference vs alias
console.log("--- 7. alias shadowing ---");
await tryQ(
	"GROUP BY name resolves raw (their claim)",
	`SELECT time_bucket(INTERVAL '1 months', "d") AS d, COUNT(*) AS n FROM t GROUP BY "d" ORDER BY 1`,
);
await tryQ(
	"WHERE name resolves raw when alias same name",
	`SELECT time_bucket(INTERVAL '1 months', "d") AS d FROM t WHERE (time_bucket(INTERVAL '1 weeks', "d") = $1) GROUP BY time_bucket(INTERVAL '1 months', "d")`,
	["2025-01-13"],
);

// 8. DESCRIBE binds params (the gate)
console.log("--- 8. DESCRIBE with params ---");
await tryQ(
	"describe with param",
	`DESCRIBE SELECT SUM(amt) AS value FROM t WHERE (time_bucket(INTERVAL '1 months', "d") = $1)`,
	["2025-01-01"],
);

// 9. DECIMAL / HUGEINT JSON shape (equation formatNumber input)
console.log("--- 9. decimal/hugeint JSON ---");
console.log(
	JSON.stringify(
		await q(`SELECT SUM(amt) AS value, SUM(big) AS bigv, 1.5 AS ratio FROM t`),
	),
);

// 10. same column sliced at 1M, pinned at frozen 1w — semantics
console.log("--- 10. mixed-grain slice+pin ---");
console.log(
	JSON.stringify(
		await q(
			`SELECT time_bucket(INTERVAL '1 months', "d") AS d, SUM(i) AS value FROM t WHERE (time_bucket(INTERVAL '1 weeks', "d") = $1) GROUP BY time_bucket(INTERVAL '1 months', "d")`,
			["2025-01-13"],
		),
	),
);

// 11. TIMESTAMPTZ serialization TZ dependence
console.log("--- 11. tstz + session timezone ---");
await q(`SET TimeZone='America/New_York'`);
const tz2 = await q(
	`SELECT time_bucket(INTERVAL '1 months', "tstz") AS b FROM t WHERE tstz IS NOT NULL GROUP BY 1 ORDER BY 1`,
);
console.log("NY buckets:", JSON.stringify(tz2));
if (tz2[0]) {
	await tryQ(
		"NY pin round-trip",
		`SELECT COUNT(*) AS n FROM t WHERE (time_bucket(INTERVAL '1 months', "tstz") = $1)`,
		[String(tz2[0].b)],
	);
}

console.log("done");
