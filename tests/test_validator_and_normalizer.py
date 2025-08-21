import json
import pytest

def test_validator_accepts_valid(valid_msg):
    from validator import Envelope

    env1 = Envelope.model_validate(valid_msg)
    env2 = Envelope.model_validate_json(json.dumps(valid_msg, ensure_ascii=False))

    assert env1.provider == valid_msg["provider"]
    assert env2.branch == valid_msg["branch"]
    assert len(env1.items) == 2

def test_normalizer_builds_rows(valid_msg):
    import normalizer

    rows = normalizer.normalize(valid_msg)
    assert isinstance(rows, list) and len(rows) == len(valid_msg["items"])
    sample = rows[0]
    for field in ("provider", "branch", "ts", "product", "price", "unit"):
        assert field in sample
