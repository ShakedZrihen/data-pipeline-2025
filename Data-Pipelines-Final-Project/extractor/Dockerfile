FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

COPY extractor/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY extractor/extractor.py /app/extractor.py
COPY extractor/io_utils.py    /app/io_utils.py
COPY extractor/parsers/parser_prices.py /app/parser_prices.py
COPY extractor//parsers/parser_promos.py /app/parser_promos.py


ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "extractor.py"]
