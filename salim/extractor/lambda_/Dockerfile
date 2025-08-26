FROM python:3.11-slim

WORKDIR /app

# Copy requirements from parent directory
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy lambda function, utils, and shared modules
COPY extractor/ ./extractor/
COPY __init__.py .

# Set PYTHONPATH to include the app directory
ENV PYTHONPATH=/app

# Run the polling script
CMD ["python", "extractor/polling_s3_lambda.py"]