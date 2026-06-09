import json, os, re
import sqlglot
HERE = os.path.dirname(os.path.abspath(__file__))
def strip_q(s): return re.sub(r"\blake\.[A-Za-z_]\w*\.", "", s, flags=re.I)
def canon(sql):
    try:
        p = sqlglot.parse_one(sql, dialect="duckdb")
        return {"ok": True, "canon": strip_q(p.sql(dialect="duckdb"))}
    except Exception as e:
        return {"ok": False, "err": str(e)}
corpus = json.load(open(__import__("sys").argv[1] if len(__import__("sys").argv) > 1 else os.path.join(HERE, "corpus.json")))
out = {"groups": {}, "distinct": []}
for g in corpus["groups"]: out["groups"][g["id"]] = [canon(m) for m in g["members"]]
out["distinct"] = [canon(m) for m in corpus["distinct"]]
print(json.dumps(out, indent=2))
