import os
import sqlite3
import time

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

PG_URL = (
    f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
    f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}"
    f"/{os.environ['POSTGRES_DB']}"
)
pg = create_engine(PG_URL)
clean = sqlite3.connect("data/goszakup_clean.db")

# Индекс қойылатын бағандар: id, *_id және жиі сүзілетін кілттер
KEY_COLS = {"customer_bin", "org_bin", "bin", "enstru_code", "kato", "region_code", "year"}


def index_columns(cols):
    return [c for c in cols if c == "id" or c.endswith("_id") or c in KEY_COLS]


tables = [t for (t,) in clean.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]

for t in tables:
    start = time.time()
    df = pd.read_sql(f'SELECT * FROM "{t}"', clean)

    # Кестені толығымен қайта жазамыз, сондықтан скриптті қайта іске қосу қауіпсіз
    df.to_sql(t, pg, if_exists="replace", index=False, chunksize=5000, method="multi")

    with pg.begin() as conn:
        for c in index_columns(df.columns):
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS "ix_{t}_{c}" ON "{t}" ("{c}")'))
        n_pg = conn.execute(text(f'SELECT COUNT(*) FROM "{t}"')).scalar()

    status = "OK" if n_pg == len(df) else "СӘЙКЕС ЕМЕС"
    print(f"{t:22} {len(df):>8} → {n_pg:>8}  {status}  ({time.time() - start:.1f} с)")

print("\nДайын.")