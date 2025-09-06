FROM python:3.11-slim

WORKDIR /app/sqs_to_postgres

# Install dependencies
RUN pip install --no-cache-dir boto3 botocore psycopg2-binary python-dotenv

# Copy enricher function
COPY . .

# Make top-level /app a module root so we can import "postgresql", "utils", etc.
ENV PYTHONPATH=/app

# Run the extractor
CMD ["python", "run.py"]