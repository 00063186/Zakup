import sqlite3

clean = sqlite3.connect("data/goszakup_clean.db")
tables = [t for (t,) in clean.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]

print("Таза кестелер:")
for t in tables:
    n = clean.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
    cols = [c[1] for c in clean.execute(f'PRAGMA table_info("{t}")')]
    print(f"  {t:20} {n:>8} жол | {len(cols)} баған")