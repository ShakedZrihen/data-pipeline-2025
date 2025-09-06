
import os, sys

try:
    _pkg = os.path.join(os.path.dirname(__file__), "package")
    if _pkg not in sys.path:
        sys.path.append(_pkg)  
except Exception:
    pass

try:
    from consumer_lambda import lambda_handler as _impl
except Exception as e:
    raise ImportError("Failed to import lambda_handler from consumer_lambda: " + str(e))

def lambda_handler(event, context):
    return _impl(event, context)
