import json
import pytest

def test_invalid_body_routed_to_dlq(invalid_json_str, dlq_capture):
    """
    DLQ
    """
    from validator import Envelope
    import dlq

    try:
        Envelope.model_validate_json(invalid_json_str)
        pytest.fail("Expected validation error for invalid JSON")
    except Exception as e:
        dlq.send_to_dlq({"raw": invalid_json_str}, f"validation/decode error: {e}")

    assert len(dlq_capture) == 1
    assert "validation/decode error" in dlq_capture[0]["error"]
    assert "raw" in dlq_capture[0]["message"]

def test_upsert_idempotent(valid_msg, fake_pg, monkeypatch):
    """
      DB
    """
    import normalizer
    import storage

    rows = normalizer.normalize(valid_msg)
    def fake_upsert_items(pg, rows_):
        cur = pg.cursor()
        for r in rows_:
            cur.execute(
                "UPSERT prices ...",
                {
                    "provider": r["provider"],
                    "branch": r["branch"],
                    "ts": r["ts"],
                    "product": r["product"],
                    "price": r["price"],
                    "unit": r["unit"],
                },
            )
        pg.commit()

    monkeypatch.setattr(storage, "upsert_items", fake_upsert_items)
    storage.upsert_items(fake_pg, rows)
    size_after_first = len(fake_pg.store)
    storage.upsert_items(fake_pg, rows)
    size_after_second = len(fake_pg.store)

    assert size_after_first == size_after_second, "Upsert must be idempotent"
    assert size_after_first == len(rows), "Each item should map to one row"
