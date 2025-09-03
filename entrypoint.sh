#!/bin/bash
set -e

chmod +x /super-compare/salim/extractor/lambda_init.sh || true
[ -x /super-compare/salim/extractor/lambda_init.sh ] && /super-compare/salim/extractor/lambda_init.sh || true

cd /super-compare/salim
exec python -m extractor.mq.consumer
