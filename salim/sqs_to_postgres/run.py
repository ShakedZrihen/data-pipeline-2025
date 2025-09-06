import os
import signal
import threading
import time
import traceback

_stop = threading.Event()

def handle_signal(sig, _frame):
    try:
        name = signal.Signals(sig).name
    except Exception:
        name = str(sig)
    print(f"[RUN] received {name} → shutting down…", flush=True)
    _stop.set()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def run_once():
    try:
        from enricher import enricher
    except Exception as e:
        print("[RUN] failed importing enricher:", e, flush=True)
        traceback.print_exc()
        raise

    try:
        enricher()
    except Exception as e:
        print("[RUN] enricher crashed:", e, flush=True)
        traceback.print_exc()


def main():
    idle_pause = float(os.getenv("SQS_EMPTY_PAUSE", "2"))
    print("[RUN] sqs-to-postgres runner starting…", flush=True)
    print(f"[RUN] SQS_ENDPOINT_URL={os.getenv('SQS_ENDPOINT_URL')}, "
          f"WAIT_TIME={os.getenv('SQS_WAIT_TIME')}s, "
          f"MAX_PER_POLL={os.getenv('SQS_MAX_PER_POLL')}", flush=True)

    while not _stop.is_set():
        run_once()
        # Short pause so we don't spin tight when the queue is empty
        if _stop.wait(idle_pause):
            break

    print("[RUN] stopped.", flush=True)


if __name__ == "__main__":
    main()
