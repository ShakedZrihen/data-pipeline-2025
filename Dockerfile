FROM python:3.12-slim

# OS deps useful for tooling/scripts
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash curl zip unzip less groff jq ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install awscli

WORKDIR /super-compare
COPY requirements.txt .
COPY salim/ ./salim/

# Install Python deps
RUN pip install  -r requirements.txt

# Add worker entrypoint (runs init scripts + consumer)
COPY entrypoint.sh /super-compare/salim/entrypoint.sh
RUN chmod +x /super-compare/salim/entrypoint.sh && chmod +x /super-compare/salim/extractor/lambda_init.sh || true
ENTRYPOINT ["/super-compare/salim/entrypoint.sh"]
