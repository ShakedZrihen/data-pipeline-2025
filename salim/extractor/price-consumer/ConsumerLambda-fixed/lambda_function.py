# Auto-generated shim so you can set the handler to "lambda_function.lambda_handler"
# It forwards to consumer_lambda.lambda_handler
import os, sys
# Ensure vendored libs are importable before loading consumer module
try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "package"))
except Exception:
    pass

try:
    from consumer_lambda import lambda_handler as _impl
except Exception as e:
    raise ImportError("Failed to import lambda_handler from consumer_lambda: " + str(e))

def lambda_handler(event, context):
    return _impl(event, context)
