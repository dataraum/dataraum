// TS canonicalizer: polyglot transpile(duckdb->duckdb) + strip lake.<layer>. qualifier.
import * as P from "@polyglot-sql/sdk";
import { readFileSync } from "node:fs";
await P.init();
const stripQ = (s) => s.replace(/\blake\.[A-Za-z_]\w*\./gi, "");
const canon = (sql) => {
  const t = P.transpile(sql, "duckdb", "duckdb");
  if (!t.success) return { ok: false, err: t.error };
  const out = Array.isArray(t.sql) ? t.sql.join(" ") : t.sql;
  return { ok: true, canon: stripQ(out) };
};
const corpus = JSON.parse(readFileSync(process.argv[2] ?? new URL("./corpus.json", import.meta.url)));
const result = { groups: {}, distinct: [] };
for (const g of corpus.groups) result.groups[g.id] = g.members.map(canon);
result.distinct = corpus.distinct.map(canon);
console.log(JSON.stringify(result, null, 2));
