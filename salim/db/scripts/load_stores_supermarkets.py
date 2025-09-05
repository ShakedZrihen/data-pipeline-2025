import os, argparse ,  json,     re
from pathlib import Path
import psycopg2

def get_chain_from_folder(name: str):
    m = re.match(r"^(.*)_(\d+)$", name)
    return m.group(1), m.group(2)

def read_json(path: Path):
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] cannot read {path}: {e}")
        return None

def pick(d: dict, *keys, default=None):
    for k in keys:
        if k in d:
            return d[k]
    return default


def iter_stores(meta: dict):
    root = meta.get("Root", {})

    subchains = root.get("SubChains", {})
    sub = subchains.get("SubChain")

    sub_list = sub if isinstance(sub, list) else [sub]

    for sc in sub_list:
        stores = sc.get("Stores", {}).get("Store")
        store_list = stores if isinstance(stores, list) else [stores]

        for st in store_list:
            store_id   = st.get("StoreId") or st.get("StoreID")
            store_name = st.get("StoreName")
            address    = (st.get("Address") or "").strip()
            city       = (st.get("City") or "").strip()

            if city.isdigit():
                city = "Missing info"

            if not address or address.lower() in {"unknown", "unk", "null"}:
                address = "Missing data"

            if store_id and store_name:
                yield str(store_id), str(store_name), address, city




def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="stores")
    ap.add_argument("--dsn", default=os.getenv("DATABASE_URL"))
    args = ap.parse_args()

    base = Path(args.dir)
    conn = None

    try:
        conn = psycopg2.connect(args.dsn)
        conn.autocommit = False

        for chain_dir in sorted([p for p in base.iterdir() if p.is_dir()]):
            chain_name, chain_id = get_chain_from_folder(chain_dir.name)

            stores_rows = []
            for jf in sorted(chain_dir.glob("*.json")):
                data = read_json(jf)
                for row in iter_stores(data):
                    stores_rows.append(row)

            if chain_id and chain_name:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO supermarkets (chain_id, chain_name)
                        VALUES (%s, %s)
                        ON CONFLICT (chain_id) DO UPDATE SET chain_name = EXCLUDED.chain_name
                    """, (chain_id, chain_name))

                with conn.cursor() as cur:
                    for store_id, store_name, address, city in stores_rows:
                        cur.execute("""
                            INSERT INTO stores (store_id, store_name, address, city)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (store_id) DO UPDATE
                               SET store_name = EXCLUDED.store_name,
                                   address    = EXCLUDED.address,
                                   city       = EXCLUDED.city
                        """, (store_id, store_name, address, city))

                conn.commit()
                print(f"[OK] {chain_dir.name} -> {chain_id} / {chain_name} | {len(stores_rows)} stores")

    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
