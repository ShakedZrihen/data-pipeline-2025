import json
import types
import pytest

@pytest.fixture
def valid_msg():
    return {
        "provider": "yohananof",
        "branch": "תל אביב - יפו",
        "type": "pricesFull",
        "timestamp": "2025-08-06T18:00:00Z",
        "items": [
            {"product": "חלב תנובה 3%", "price": 5.9, "unit": "liter"},
            {"product": "ביצים L 12", "price": 12.5, "unit": "pack"},
        ],
    }

@pytest.fixture
def invalid_json_str():
    return "{provider:yohananof,branch:tlv,type:pricesFull,timestamp:2025-08-06T18:00:00Z,items:[]}"

class _FakeCursor:
    def __init__(self, store):
        self.store = store
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if isinstance(params, dict):
            key = (
                params.get("provider"),
                params.get("branch"),
                params.get("ts"),
                params.get("product"),
            )
            if key not in self.store:
                self.store[key] = dict(params)

    def close(self):
        pass

class _FakePG:
    def __init__(self, store):
        self.store = store
        self._cursor = _FakeCursor(store)
        self.commits = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def close(self):
        pass

@pytest.fixture
def fake_pg():
    return _FakePG(store={})

@pytest.fixture
def dlq_capture(monkeypatch):
    sent = []

    def _fake_send(message_body: dict, error: str):
        sent.append({"error": error, "message": message_body})

    try:
        import dlq
        monkeypatch.setattr(dlq, "send_to_dlq", _fake_send, raising=False)
    except Exception:
        pass

    return sent
