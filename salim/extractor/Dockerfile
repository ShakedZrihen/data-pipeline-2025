FROM python:3.11-slim

WORKDIR /app/extractor

# Install dependencies
RUN pip install --no-cache-dir boto3 botocore

# Copy extractor files
COPY . .

# Run the extractor
CMD ["python", "run.py"]