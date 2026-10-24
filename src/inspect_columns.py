import json
import sqlite3

clean = sqlite3.connect("data/goszakup_clean.db")
raw = sqlite3.connect("data/goszakup_raw.db")

for t in ["plan_items", "lots", "contracts", "plan_delivery", "regions"]:
    cols = [c[1] for c in clean.execute(f'PRAGMA table_info("{t}")')]
    print(f"\n{t}:\n  {', '.join(cols)}")

print("\nplan_items.source бөлінісі:")
for row in clean.execute("SELECT source, COUNT(*) FROM plan_items GROUP BY source"):
    print("  ", row)

payload = raw.execute(
    "SELECT payload FROM raw WHERE entity='contract_unit' LIMIT 1").fetchone()[0]
print("\ncontract_unit өрістері:\n ", ", ".join(json.loads(payload).keys()))
print("\ncontract_unit саны:",
      raw.execute("SELECT COUNT(*) FROM raw WHERE entity='contract_unit'").fetchone()[0])