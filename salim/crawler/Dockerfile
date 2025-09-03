FROM python:3.11-slim

WORKDIR /app/crawler

# Install dependencies
RUN pip install --no-cache-dir boto3 botocore beautifulsoup4 selenium requests

# Copy crawler function
COPY . .

# Run the crawler
CMD ["python", "run.py"]