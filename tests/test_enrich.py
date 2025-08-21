from ingest_consumer.enrich import enrich_item
from datetime import datetime, timezone

def test_enrich_basic():
    row = enrich_item("yohananof","תל אביב -   יפו","pricesFull","2025-08-06T18:00:00Z",
                      {"product":"  חלב תנובה 3% ","price":5.9,"unit":"liter"})
    assert row["branch"] == "תל אביב - יפו"
    assert row["ts"].tzinfo is not None and row["ts"].tzinfo == timezone.utc
    assert row["price"] == row["price"].quantize(row["price"])  # Decimal
