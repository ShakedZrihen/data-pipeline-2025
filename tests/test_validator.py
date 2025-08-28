import pytest
from ingest_consumer.validator import Envelope

def test_valid_envelope():
    e = Envelope(**{
        "provider":"p","branch":"b","type":"pricesFull","timestamp":"2025-01-01T00:00:00Z",
        "items":[{"product":"x","price":1.23,"unit":"ea"}]
    })
    assert e.items[0].price == 1.23

def test_invalid_price():
    with pytest.raises(Exception):
        Envelope(**{"provider":"p","branch":"b","type":"t","timestamp":"2025-01-01T00:00:00Z","items":[{"product":"x","price":0}]})
