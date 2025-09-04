import time, json, logging

_counters = {}

def incr(name: str, delta: int = 1, **labels):
    total = _counters.get(name, 0) + delta
    _counters[name] = total
    logging.info(json.dumps({"metric": name, "delta": delta, "total": total, **labels}))

class timer:
    def __init__(self, metric: str, **labels):
        self.metric = metric
        self.labels = labels
        self._t0 = None

    def __enter__(self):
        self._t0 = time.time()
        return self

    def __exit__(self, exc_type, exc, tb):
        dur_ms = int((time.time() - self._t0) * 1000)
        incr(self.metric, dur_ms, **self.labels)
