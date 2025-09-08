
FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl zip unzip ca-certificates postgresql-client \
    build-essential gcc libpq-dev \
 && rm -rf /var/lib/apt/lists/*

 RUN pip install boto3 python-dotenv psycopg2-binary "fastapi[standard]"

# Install AWS CLI v2
RUN curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip \
 && unzip -q awscliv2.zip && ./aws/install \
 && rm -rf awscliv2.zip aws

COPY runExtractor.sh /app/runExtractor.sh
COPY sqs_consumer.py /app/sqs_consumer.py
COPY db_handler.py /app/db_handler.py
COPY Enricher.py /app/Enricher.py
COPY listFiles.py /app/listFiles.py
COPY extractor/lambda_extractor /app/lambda_extractor
COPY SuperSapirBranchs.json /app/SuperSapirBranchs.json
COPY YohananofBranchs.json /app/YohananofBranchs.json
COPY ZolVeBegadolBranchs.json /app/ZolVeBegadolBranchs.json


RUN chmod +x runExtractor.sh
ENTRYPOINT []
CMD ["./runExtractor.sh"]
