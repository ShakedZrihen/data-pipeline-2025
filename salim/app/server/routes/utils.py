from sqlalchemy import text
from sqlalchemy.orm import Session
import json
from pathlib import Path


def load_store_mapping() -> dict:

    for p in ["extractor/stores/stores_mapping.json", "../extractor/stores/stores_mapping.json"]:
        fp = Path(__file__).resolve().parent.parent.parent / p
        if fp.exists():
            try:
                return json.loads(fp.read_text(encoding="utf-8"))
            except Exception:
                pass
    return {}

PROVIDER_WEBSITE = {
    "Yohananof": "https://www.yochananof.co.il/",
    "RamiLevi": "https://www.rami-levy.co.il/",
}

def seed_supermarkets_if_empty(db: Session):
    n = db.execute(text("SELECT count(*) FROM supermarket")).scalar_one()
    if n and n > 0:
        return

    stores_map = load_store_mapping()

    rows = db.execute(text("""
      WITH s AS (
        SELECT provider, branch_code FROM price_item
        UNION
        SELECT provider, branch_code FROM promo_item
      )
      SELECT DISTINCT provider, branch_code
      FROM s
      ORDER BY provider, branch_code
    """)).mappings().all()

    for r in rows:
        provider = r["provider"]
        branch   = r["branch_code"]
        display  = stores_map.get(provider, {}).get(branch, None)
        db.execute(text("""
          INSERT INTO supermarket (provider, branch_code, name, branch_name, website)
          VALUES (:p, :b, :name, :branch, :web)
          ON CONFLICT (provider, branch_code) DO NOTHING
        """), dict(
            p=provider,
            b=branch,
            name=provider,
            branch=display,
            web=PROVIDER_WEBSITE.get(provider)
        ))
    db.commit()