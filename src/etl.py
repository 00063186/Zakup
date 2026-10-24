"""
ETL: шикі деректер (data/goszakup_raw.db) → таза кестелер → PostgreSQL.

Іске қосу (жоба түбірінен):
    python src/etl.py            # SQLite + PostgreSQL
    python src/etl.py --skip-pg  # тек SQLite (PostgreSQL іске қосылмаған болса)
"""
import argparse
import json
import os
import sqlite3
import time

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

RAW_PATH = "data/goszakup_raw.db"
CLEAN_PATH = "data/goszakup_clean.db"

raw = sqlite3.connect(RAW_PATH)
clean = sqlite3.connect(CLEAN_PATH)


# ---------------------------------------------------------------- көмекшілер
def raw_df(entity):
    """Шикі дерекқордағы бір түрдің барлық JSON-ын DataFrame-ге айналдырады."""
    rows = [json.loads(p) for (p,) in raw.execute(
        "SELECT payload FROM raw WHERE entity=?", (entity,))]
    return pd.DataFrame(rows)


def ref_map(table, key="id", col="name_ru"):
    """Анықтамалықтан {код: атауы} сөздігін жасайды."""
    df = pd.read_sql(f"SELECT {key}, {col} FROM {table}", clean)
    return dict(zip(df[key].astype(str), df[col]))


def col(df, name):
    """Баған жоқ болса, бос баған қайтарады (API өрістері кейде болмайды)."""
    return df[name] if name in df.columns else pd.Series([None] * len(df), index=df.index)


def to_int(s):
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def to_region(kato):
    """КАТО кодынан облыс коды: 711210000 -> '710000000'."""
    if kato is None or (not isinstance(kato, str) and pd.isna(kato)):
        return None
    s = str(kato).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s[:2] + "0000000" if len(s) == 9 and s.isdigit() and s[:2] != "00" else None


def save(df, name):
    df.to_sql(name, clean, if_exists="replace", index=False)
    print(f"  {name:22} {len(df):>8} жол")


# ---------------------------------------------------------------- 1. анықтамалықтар
def build_refs():
    for name, payload in raw.execute(
            "SELECT obj_id, payload FROM raw WHERE entity='ref'").fetchall():
        df = pd.DataFrame(json.loads(payload))
        # SQLite тізім мен сөздікті сақтай алмайды, оларды JSON мәтінге айналдырамыз
        for c in df.columns:
            if df[c].map(lambda v: isinstance(v, (list, dict))).any():
                df[c] = df[c].map(lambda v: json.dumps(v, ensure_ascii=False)
                                  if isinstance(v, (list, dict)) else v)
        key = "id" if "id" in df.columns else ("code" if "code" in df.columns else None)
        df = df.drop_duplicates(subset=[key] if key else None)
        save(df, name)


# ---------------------------------------------------------------- 2. жоспар
def build_plan():
    rows, delivery = [], []
    for entity, payload in raw.execute(
            "SELECT entity, payload FROM raw WHERE entity IN ('plan', 'plan_hist')"):
        p = json.loads(payload)
        rows.append({
            "plan_id":      p.get("id"),
            "root_id":      p.get("rootrecord_id"),
            "source":       entity,  # 'plan' = белсенді, 'plan_hist' = ескі нұсқа
            "customer_bin": p.get("subject_biin"),
            "year":         p.get("pln_point_year") or p.get("plan_fin_year"),
            "month":        p.get("ref_months_id"),
            "enstru_code":  p.get("ref_enstru_code"),
            "unit_code":    p.get("ref_units_code"),
            "name_ru":      p.get("name_ru"),
            "name_kz":      p.get("name_kz"),
            "desc_ru":      p.get("desc_ru"),
            "quantity":     p.get("count"),
            "price":        p.get("price"),
            "amount":       p.get("amount"),
            "trade_method": p.get("ref_trade_methods_id"),
            "subject_type": p.get("ref_subject_type_id"),
            "is_deleted":   p.get("is_deleted"),
            "date_create":  p.get("date_create"),
        })
        for k in p.get("kato") or []:
            delivery.append({
                "plan_id":     p.get("id"),
                "kato_code":   k.get("ref_kato_code"),
                "region_code": to_region(k.get("ref_kato_code")),
                "quantity":    k.get("count"),
                "address_ru":  k.get("full_delivery_place_name_ru"),
            })

    plan_items = (pd.DataFrame(rows).sort_values("source")  # 'plan' бірінші
                  .drop_duplicates("plan_id", keep="first"))
    save(plan_items, "plan_items")
    save(pd.DataFrame(delivery), "plan_delivery")

    regions = pd.read_sql("""SELECT code AS region_code, name_ru, name_kz FROM ref_kato
                             WHERE code LIKE '__0000000'""", clean)
    save(regions, "regions")


# ---------------------------------------------------------------- 3. хабарландырулар
def build_announcements():
    tb = raw_df("trd_buy")
    methods = ref_map("ref_trade_methods")
    buy_stat = ref_map("ref_buy_status")
    subj_type = ref_map("ref_subject_type")

    df = pd.DataFrame({
        "trd_buy_id":      tb["id"],
        "number_anno":     col(tb, "number_anno"),
        "name_ru":         col(tb, "name_ru"),
        "name_kz":         col(tb, "name_kz"),
        "customer_bin":    col(tb, "customer_bin"),
        "org_bin":         col(tb, "org_bin"),
        "total_sum":       col(tb, "total_sum"),
        "count_lots":      col(tb, "count_lots"),
        "trade_method_id": col(tb, "ref_trade_methods_id"),
        "trade_method":    col(tb, "ref_trade_methods_id").astype(str).map(methods),
        "subject_type":    col(tb, "ref_subject_type_id").astype(str).map(subj_type),
        "status":          col(tb, "ref_buy_status_id").astype(str).map(buy_stat),
        "is_repeat":       (pd.to_numeric(col(tb, "parent_id"), errors="coerce").fillna(0) > 0).astype(int),
        "parent_id":       col(tb, "parent_id"),
        "publish_date":    col(tb, "publish_date"),
        "end_date":        col(tb, "end_date"),
        "itogi_date":      col(tb, "itogi_date_public"),
    })
    df["year"] = pd.to_numeric(df.publish_date.astype(str).str[:4], errors="coerce").astype("Int64")
    save(df, "announcements")


# ---------------------------------------------------------------- 4. лоттар
def build_lots():
    lt = raw_df("lot")
    lot_stat = ref_map("ref_lots_status")
    methods = ref_map("ref_trade_methods")

    def first_kato(lst):
        lst = [k for k in (lst or []) if k and str(k) != "0"]
        return str(lst[0]) if lst else None

    df = pd.DataFrame({
        "lot_id":         lt["id"],
        "lot_number":     col(lt, "lot_number"),
        "trd_buy_id":     col(lt, "trd_buy_id"),
        "customer_bin":   col(lt, "customer_bin"),
        "name_ru":        col(lt, "name_ru"),
        "name_kz":        col(lt, "name_kz"),
        "description_ru": col(lt, "description_ru"),
        "quantity":       col(lt, "count"),
        "amount":         col(lt, "amount"),
        "is_union":       col(lt, "union_lots"),
        "status":         col(lt, "ref_lot_status_id").astype(str).map(lot_stat),
        "trade_method":   col(lt, "ref_trade_methods_id").astype(str).map(methods),
        "dumping":        col(lt, "dumping"),
        "kato_code":      col(lt, "pln_point_kato_list").map(first_kato),
        "point_list":     col(lt, "point_list").map(json.dumps),
    })
    df["region_code"] = df.kato_code.map(to_region)
    ok = (df.quantity > 0) & (df.is_union == 0)
    df.loc[ok, "unit_price"] = df.loc[ok, "amount"] / df.loc[ok, "quantity"]
    save(df, "lots")


# ---------------------------------------------------------------- 5. шарттар
def build_contracts():
    ct = raw_df("contract")
    c_stat = ref_map("ref_contract_status")
    c_type = ref_map("ref_contract_type")
    methods = ref_map("ref_trade_methods")

    number = col(ct, "contract_number_sys")
    if number.isna().all():
        number = col(ct, "contract_number")

    df = pd.DataFrame({
        "contract_id":       ct["id"],
        "root_id":           col(ct, "root_id"),
        "parent_id":         col(ct, "parent_id"),
        "contract_number":   number,
        "trd_buy_id":        col(ct, "trd_buy_id"),
        "customer_bin":      col(ct, "customer_bin"),
        "supplier_bin":      col(ct, "supplier_biin"),
        "sum_no_vat":        col(ct, "contract_sum"),
        "sum_with_vat":      col(ct, "contract_sum_wnds"),
        "fact_sum_with_vat": col(ct, "fakt_sum_wnds"),
        "sign_date":         col(ct, "sign_date"),
        "crdate":            col(ct, "crdate"),
        "fin_year":          col(ct, "fin_year"),
        "status":            col(ct, "ref_contract_status_id").astype(str).map(c_stat),
        "contract_type":     col(ct, "ref_contract_type_id").astype(str).map(c_type),
        "fact_trade_method": col(ct, "fakt_trade_methods_id").astype(str).map(methods),
        "plan_exec_date":    col(ct, "plan_exec_date"),
        "fact_exec_date":    col(ct, "fakt_exec_date"),
        "end_date":          col(ct, "contract_end_date"),
        "deleted":           col(ct, "deleted"),
    })
    df["is_single_source"] = (pd.to_numeric(df.trd_buy_id, errors="coerce").fillna(0) == 0).astype(int)
    df["year"] = pd.to_numeric(df.crdate.astype(str).str[:4], errors="coerce").astype("Int64")

    # Әр root_id бойынша ең соңғы (ID-і ең үлкен) өшірілмеген нұсқа
    live = df[df.deleted == 0]
    latest = set(live.sort_values("contract_id").groupby("root_id").tail(1).contract_id)
    df["is_latest"] = df.contract_id.isin(latest).astype(int)
    save(df, "contracts")


# ---------------------------------------------------------------- 6. шарттың тауар жолдары
def build_contract_lines():
    fields = ["id", "root_id", "lot_id", "pln_point_id", "contract_registry_id", "trd_buy_id",
              "item_price", "item_price_wnds", "quantity", "total_sum", "fact_sum",
              "executed", "deleted"]
    sel = ", ".join(f"json_extract(payload, '$.{f}') AS {f}" for f in fields)
    u = pd.read_sql(f"SELECT {sel} FROM raw WHERE entity='contract_unit'", raw)
    u["pln_point_id"] = to_int(u.pln_point_id)
    u["lot_id"] = to_int(u.lot_id)

    # Тек шарттың соңғы нұсқасы және өшірілмеген жолдар
    c = pd.read_sql("""SELECT contract_id, customer_bin, supplier_bin, sign_date, year,
                              fact_trade_method, is_single_source, status, is_latest, deleted
                       FROM contracts""", clean)
    c = c[(c.is_latest == 1) & (c.deleted.fillna(0) == 0)].drop(columns=["is_latest", "deleted"])
    u = u[u.deleted.fillna(0) == 0]
    u = u.merge(c, left_on="contract_registry_id", right_on="contract_id", how="inner")
    u = u.sort_values("id").drop_duplicates(["contract_id", "root_id"], keep="last")

    # Жоспар тармағы: тікелей сілтеме
    p = pd.read_sql("""SELECT plan_id, root_id, source AS plan_source, enstru_code, unit_code,
                              name_ru AS item_name_ru, name_kz AS item_name_kz
                       FROM plan_items""", clean)
    p["plan_id"] = to_int(p.plan_id)
    p["root_id"] = to_int(p.root_id)
    u = u.merge(p.drop(columns="root_id"), left_on="pln_point_id", right_on="plan_id", how="left")

    attrs = ["enstru_code", "unit_code", "item_name_ru", "item_name_kz"]

    def fill_plan(mask, src, label):
        u.loc[mask, "plan_id"] = src.plan_id.values
        for a in attrs:
            u.loc[mask, a] = src[a].values
        u.loc[mask, "plan_source"] = label

    # Өшірілген тармақтың жаңа нұсқасы: root_id = ескі ID
    by_root = (p[p.plan_source == "plan"].dropna(subset=["root_id"])
               .sort_values("plan_id").drop_duplicates("root_id", keep="last")
               .set_index("root_id"))
    m = u.plan_id.isna() & u.pln_point_id.isin(by_root.index)
    fill_plan(m, by_root.loc[u.loc[m, "pln_point_id"]], "plan_root")

    # Лоттың point_list арқылы: тек ЕНС ТРУ мен бірлігі бірдей лоттар
    lots = pd.read_sql("SELECT lot_id, region_code AS lot_region, point_list FROM lots", clean)
    lots["lot_id"] = to_int(lots.lot_id)
    ex = (lots[["lot_id"]].assign(pid=lots.point_list.astype(str).str.findall(r"\d+"))
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

    # Өңір: лот → жоспардың жалғыз жеткізу өңірі → тапсырыс берушінің мекенжайы
    u = u.merge(lots[["lot_id", "lot_region"]].drop_duplicates("lot_id"), on="lot_id", how="left")
    u["lot_region"] = u.lot_region.map(to_region)

    d = pd.read_sql("SELECT plan_id, region_code FROM plan_delivery", clean)
    d["plan_id"] = to_int(d.plan_id)
    d["region_code"] = d.region_code.map(to_region)
    d = d.dropna().drop_duplicates()
    single = (d.groupby("plan_id").filter(lambda x: len(x) == 1)
              .rename(columns={"region_code": "plan_region"}))
    u = u.merge(single, on="plan_id", how="left")

    cust_region = {}
    for oid, payload in raw.execute("SELECT obj_id, payload FROM raw WHERE entity='subject'"):
        s = json.loads(payload)
        kl = s.get("kato_list") or []
        if kl and kl[0]:
            cust_region[s.get("bin") or str(oid)] = to_region(kl[0])
    u["cust_region"] = u.customer_bin.astype(str).map(cust_region)

    u["region_code"] = u.lot_region.combine_first(u.plan_region).combine_first(u.cust_region)
    u["region_source"] = np.select(
        [u.lot_region.notna(), u.plan_region.notna(), u.cust_region.notna()],
        ["lot", "plan", "customer"], default="")
    u["region_source"] = u.region_source.replace("", None)

    u["plan_id"] = to_int(u.plan_id)
    u["price_valid"] = (u.item_price > 0) & (u.quantity > 0)
    out = u.rename(columns={"id": "line_id", "item_price": "unit_price",
                            "item_price_wnds": "unit_price_with_vat"})[[
        "line_id", "root_id", "contract_id", "trd_buy_id", "lot_id", "plan_id", "plan_source",
        "customer_bin", "supplier_bin", "year", "sign_date", "status",
        "fact_trade_method", "is_single_source",
        "enstru_code", "unit_code", "item_name_ru", "item_name_kz",
        "quantity", "unit_price", "unit_price_with_vat", "total_sum", "fact_sum", "executed",
        "region_code", "region_source", "price_valid"]]
    save(out, "contract_lines")
    return out


# ---------------------------------------------------------------- 7. PostgreSQL
INDEXES = {
    "plan_items":     ["plan_id", "root_id", "customer_bin", "enstru_code", "year"],
    "plan_delivery":  ["plan_id", "region_code"],
    "announcements":  ["trd_buy_id", "customer_bin", "year"],
    "lots":           ["lot_id", "trd_buy_id", "customer_bin", "region_code"],
    "contracts":      ["contract_id", "root_id", "trd_buy_id", "customer_bin", "supplier_bin", "year"],
    "contract_lines": ["contract_id", "plan_id", "lot_id", "customer_bin", "supplier_bin",
                       "enstru_code", "region_code", "year"],
}


def load_to_pg():
    load_dotenv()
    pg = create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}"
        f"/{os.environ['POSTGRES_DB']}"
    )
    tables = [t for (t,) in clean.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    for t in tables:
        df = pd.read_sql(f'SELECT * FROM "{t}"', clean)
        df.to_sql(t, pg, if_exists="replace", index=False, chunksize=5000, method="multi")
        with pg.begin() as conn:
            for c in INDEXES.get(t, []):
                if c in df.columns:
                    conn.execute(text(f'CREATE INDEX IF NOT EXISTS "ix_{t}_{c}" ON "{t}" ("{c}")'))
            n = conn.execute(text(f'SELECT COUNT(*) FROM "{t}"')).scalar()
        print(f"  {t:22} {n:>8} жол  {'OK' if n == len(df) else 'СӘЙКЕС ЕМЕС'}")


# ---------------------------------------------------------------- сапа есебі
def report(out):
    print(f"\ncontract_lines: {len(out)} жол")
    print("Жоспар тармағының көзі:")
    print(out.plan_source.fillna("табылмады").value_counts().to_string())
    print("Өңірдің көзі:")
    print(out.region_source.fillna("жоқ").value_counts().to_string())
    print("Жылдар бойынша ЕНС ТРУ қамтуы:")
    print(out.groupby("year").agg(lines=("line_id", "size"),
                                  enstru=("enstru_code", lambda s: f"{s.notna().mean():.1%}")))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-pg", action="store_true", help="PostgreSQL-ге жазбау")
    args = parser.parse_args()

    start = time.time()
    steps = [("Анықтамалықтар", build_refs), ("Жоспар", build_plan),
             ("Хабарландырулар", build_announcements), ("Лоттар", build_lots),
             ("Шарттар", build_contracts)]
    for title, fn in steps:
        print(f"\n[{title}]")
        fn()
        clean.commit()

    print("\n[Шарттың тауар жолдары]")
    out = build_contract_lines()
    clean.commit()

    if not args.skip_pg:
        print("\n[PostgreSQL]")
        load_to_pg()

    report(out)
    print(f"\nДайын: {time.time() - start:.0f} с")


if __name__ == "__main__":
    main()