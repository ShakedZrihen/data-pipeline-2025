import importlib.util, sys, time
from apscheduler.schedulers.blocking import BlockingScheduler
import os
from shared.emitter import PricesNDJSONEmitter

def load_user_module(path="src/run_crawler.py"):
    spec = importlib.util.spec_from_file_location("user_run_crawler", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod

from shared.emitter import PricesNDJSONEmitter

def run_once():
    print("[user-crawler] invoking user run_crawler.main()")
    mod = load_user_module()
    if hasattr(mod, "main"):
        with PricesNDJSONEmitter(prefix=os.getenv("S3_PREFIX", "prices/")) as E:
            # Expose emit_product(record) to the user module
            setattr(mod, "EMITTER", E)
            def emit_product(record: dict):
                try:
                    E.write(record)
                except Exception as ex:
                    print("emit_product error:", ex)
            setattr(mod, "emit_product", emit_product)
            mod.main()
    else:
        print("run_crawler.py has no main()")

if __name__ == "__main__":
    # run immediately, then hourly
    run_once()
    sched = BlockingScheduler(timezone="UTC")
    sched.add_job(run_once, "cron", minute=0)
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass
