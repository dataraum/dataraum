import { readFileSync } from "node:fs";
const ts = JSON.parse(readFileSync(process.argv[2] ?? "ts-out.json"));
const py = JSON.parse(readFileSync(process.argv[3] ?? "py-out.json"));
const C = (x) => (x.ok ? x.canon : "ERR:" + x.err);
const allEq = (arr) => arr.every((x) => x === arr[0]);
console.log("GROUP                | within-PY | within-TS | PY≡TS per-member");
console.log("---------------------|-----------|-----------|-----------------");
for (const g of Object.keys(ts.groups)) {
  const tcs = ts.groups[g].map(C), pcs = py.groups[g].map(C);
  const wpy = allEq(pcs) ? "SAME" : "DIFF";
  const wts = allEq(tcs) ? "SAME" : "DIFF";
  const cross = tcs.map((t,i)=> t===pcs[i] ? "y":"n").join("");
  console.log(g.padEnd(20), "|", wpy.padEnd(9), "|", wts.padEnd(9), "|", cross);
}
const dts = ts.distinct.map(C), dpy = py.distinct.map(C);
console.log("\nDISTINCT collide? PY:", new Set(dpy).size===dpy.length?"no(good)":"YES(bad)", "| TS:", new Set(dts).size===dts.length?"no(good)":"YES(bad)");
console.log("\n--- sample canonical forms (revenue_sale) ---");
console.log("PY[0]:", JSON.stringify(C(py.groups.revenue_sale[0])));
console.log("TS[0]:", JSON.stringify(C(ts.groups.revenue_sale[0])));
console.log("PY[2-qualified]:", JSON.stringify(C(py.groups.revenue_sale[2])));
console.log("TS[2-qualified]:", JSON.stringify(C(ts.groups.revenue_sale[2])));
console.log("\n--- commutative (the known-hard case) ---");
console.log("PY:", JSON.stringify(py.groups.commutative_where.map(C)));
console.log("TS:", JSON.stringify(ts.groups.commutative_where.map(C)));
