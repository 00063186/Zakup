import os
import sqlite3
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# 1) Шикі дерекқор
raw = sqlite3.connect("data/goszakup_raw.db")
print("Шикі дерекқор:")
for entity, n in raw.execute("SELECT entity, COUNT(*) FROM raw GROUP BY entity ORDER BY 2 DESC"):
    print(f"  {entity:16} {n}")

# 2) Токен
token = os.getenv("GOSZAKUP_TOKEN")
print("\nТокен:", "бар" if token else "ЖОҚ")

# 3) PostgreSQL
url = (f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
       f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}"
       f"/{os.environ['POSTGRES_DB']}")
engine = create_engine(url)
with engine.connect() as conn:
    print("PostgreSQL:", conn.execute(text("SELECT version()")).scalar()[:40])