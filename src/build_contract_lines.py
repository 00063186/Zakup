import json
import os
import sqlite3

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
pg = create_engine(
    f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
    f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}"
    f"/{os.environ['POSTGRES_DB']}"
)
raw = sqlite3.connect("data/goszakup_raw.db")
clean = sqlite3.connect("data/goszakup_clean.db")


def to_int(s):
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def to_region(k):
    """КАТО кодынан облыс коды: 711210000 -> '710000000'."""
    if k is None or pd.isna(k) or str(k).strip() in ("", "0"):
        return None
    s = str(int(float(k)))
    return s[:2] + "0000000" if len(s) == 9 else None


# 1) Шикі тауар жолдары
fields = ["id", "root_id", "lot_id", "pln_point_id", "contract_registry_id", "trd_buy_id",
          "item_price", "item_price_wnds", "quantity", "total_sum", "fact_sum",
          "executed", "deleted"]
sel = ", ".join(f"json_extract(payload, '$.{f}') AS {f}" for f in fields)
u = pd.read_sql(f"SELECT {sel} FROM raw WHERE entity='contract_unit'", raw)
u["pln_point_id"] = to_int(u.pln_point_id)
u["lot_id"] = to_int(u.lot_id)
print("Шикі тауар жолдары:", len(u))

# 2) Тек шарттың соңғы нұсқасы және өшірілмеген жолдар
c = pd.read_sql("""SELECT contract_id, customer_bin, supplier_bin, sign_date, year,
                          fact_trade_method, is_single_source, status, is_latest, deleted
                   FROM contracts""", clean)
c = c[(c.is_latest == 1) & (c.deleted.fillna(0) == 0)].drop(columns=["is_latest", "deleted"])
u = u[u.deleted.fillna(0) == 0]
u = u.merge(c, left_on="contract_registry_id", right_on="contract_id", how="inner")
u = u.sort_values("id").drop_duplicates(["contract_id", "root_id"], keep="last")
print("Соңғы нұсқадағы жолдар:", len(u))

# 3) Жоспар тармағы: тікелей сілтеме бойынша
p = pd.read_sql("""SELECT plan_id, root_id, source AS plan_source, enstru_code, unit_code,
                          name_ru AS item_name_ru, name_kz AS item_name_kz
                   FROM plan_items""", clean)
p["plan_id"] = to_int(p.plan_id)
p["root_id"] = to_int(p.root_id)
p = p.sort_values("plan_source").drop_duplicates("plan_id", keep="first")  # 'plan' бірінші

u = u.merge(p.drop(columns="root_id"), left_on="pln_point_id", right_on="plan_id", how="left")

ATTRS = ["enstru_code", "unit_code", "item_name_ru", "item_name_kz"]


def fill_plan(mask, src, label):
    """Табылмаған жолдарды src-тағы жоспар тармағымен толтырады."""
    u.loc[mask, "plan_id"] = src.plan_id.values
    for col in ATTRS:
        u.loc[mask, col] = src[col].values
    u.loc[mask, "plan_source"] = label


# 3b) Өшірілген тармақтың жаңа нұсқасы: root_id = ескі ID
by_root = (p[p.plan_source == "plan"].dropna(subset=["root_id"])
           .sort_values("plan_id").drop_duplicates("root_id", keep="last")
           .set_index("root_id"))
m = u.plan_id.isna() & u.pln_point_id.isin(by_root.index)
fill_plan(m, by_root.loc[u.loc[m, "pln_point_id"]], "plan_root")
print("root_id арқылы қалпына келді:", m.sum())

# 3c) Лоттың point_list арқылы: тек ЕНС ТРУ мен бірлігі бірдей лоттар
lots = pd.read_sql("SELECT lot_id, region_code AS lot_region, point_list FROM lots", clean)
lots["lot_id"] = to_int(lots.lot_id)
ex = (lots[["lot_id"]].assign(pid=lots.point_list.astype(str).str.findall(r"\d{5,}"))
      .explode("pid").dropna(subset=["pid"]))
ex["pid"] = to_int(ex.pid)
li = ex.merge(p, left_on="pid", right_on="plan_id")
g = li.groupby("lot_id").agg(n_e=("enstru_code", "nunique"), n_u=("unit_code", "nunique"),
                             plan_id=("plan_id", "max"))
good = g[(g.n_e == 1) & (g.n_u == 1)]
lot_src = (good[["plan_id"]].reset_index()
           .merge(p.drop(columns="root_id"), on="plan_id").set_index("lot_id"))
m = u.plan_id.isna() & u.lot_id.isin(lot_src.index)
fill_plan(m, lot_src.loc[u.loc[m, "lot_id"]].reset_index(), "lot_points")
print("Лот арқылы қалпына келді:", m.sum())

# 4) Өңір: лот → жоспардың жалғыз жеткізу өңірі → тапсырыс берушінің мекенжайы
u = u.merge(lots[["lot_id", "lot_region"]].drop_duplicates("lot_id"), on="lot_id", how="left")
u["lot_region"] = u.lot_region.map(to_region)

d = pd.read_sql("SELECT plan_id, region_code FROM plan_delivery", clean)
d["plan_id"] = to_int(d.plan_id)
d["region_code"] = d.region_code.map(to_region)
d = d.dropna().drop_duplicates()
single = (d.groupby("plan_id").filter(lambda x: len(x) == 1)
          .rename(columns={"region_code": "plan_region"}))
u = u.merge(single, on="plan_id", how="left")

subj = pd.read_sql("SELECT obj_id, payload FROM raw WHERE entity='subject'", raw)
cust_region = {}
for oid, payload in subj.itertuples(index=False):
    s = json.loads(payload)
    kl = s.get("kato_list") or []
    if kl and kl[0]:
        cust_region[s.get("bin") or str(oid)] = to_region(kl[0])
u["cust_region"] = u.customer_bin.astype(str).map(cust_region)

u["region_code"] = u.lot_region.combine_first(u.plan_region).combine_first(u.cust_region)
u["region_source"] = np.select(
    [u.lot_region.notna(), u.plan_region.notna(), u.cust_region.notna()],
    ["lot", "plan", "customer"], default=None)
u["region_source"] = u.region_source.replace("None", None)

# regions кестесімен сәйкестендіру
reg = pd.read_sql("SELECT region_code FROM regions", clean)
unknown = set(u.region_code.dropna()) - set(reg.region_code.map(to_region))
if unknown:
    print("regions кестесінде жоқ өңір кодтары:", unknown)
if pd.api.types.is_numeric_dtype(reg.region_code):
    u["region_code"] = to_int(u.region_code)

# 5) Соңғы кесте
u["price_valid"] = (u.item_price > 0) & (u.quantity > 0)
out = u.rename(columns={"id": "line_id", "item_price": "unit_price",
                        "item_price_wnds": "unit_price_with_vat"})[[
    "line_id", "root_id", "contract_id", "trd_buy_id", "lot_id", "plan_id", "plan_source",
    "customer_bin", "supplier_bin", "year", "sign_date", "status",
    "fact_trade_method", "is_single_source",
    "enstru_code", "unit_code", "item_name_ru", "item_name_kz",
    "quantity", "unit_price", "unit_price_with_vat", "total_sum", "fact_sum", "executed",
    "region_code", "region_source", "price_valid"]]

out.to_sql("contract_lines", clean, if_exists="replace", index=False)
out.to_sql("contract_lines", pg, if_exists="replace", index=False, chunksize=5000, method="multi")
with pg.begin() as conn:
    for col in ["contract_id", "plan_id", "lot_id", "customer_bin", "enstru_code",
                "region_code", "year"]:
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS "ix_contract_lines_{col}" '
                          f'ON contract_lines ("{col}")'))

# 6) Сапа есебі
print(f"\ncontract_lines: {len(out)} жол")
print("\nЖоспар тармағының көзі:")
print(out.plan_source.fillna("табылмады").value_counts().to_string())
print("\nӨңірдің көзі:")
print(out.region_source.fillna("жоқ").value_counts().to_string())
print("\nЖылдар бойынша ЕНС ТРУ қамтуы:")
print(out.groupby("year").agg(lines=("line_id", "size"),
                              enstru=("enstru_code", lambda s: f"{s.notna().mean():.1%}")))