FROM python:3.11-slim
WORKDIR /app

# Install Chromium/Chromedriver for Selenium
RUN apt-get update && apt-get install -y --no-install-recommends \
      chromium \
      chromium-driver \
      fonts-liberation \
      libnss3 \
      libatk-bridge2.0-0 \
      libgtk-3-0 \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY ./crawler/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the new modular crawler architecture
COPY ./crawler /app/crawler
COPY ./shared /app/shared

# Set environment variables for MinIO (can be overridden via env)
ENV AWS_ACCESS_KEY_ID=minioadmin
ENV AWS_SECRET_ACCESS_KEY=minioadmin
ENV AWS_DEFAULT_REGION=us-east-1
ENV AWS_ENDPOINT_URL=http://minio:9000
ENV AWS_ENDPOINT_URL_S3=http://minio:9000

# Use the new main crawler runner
CMD ["python", "crawler/run_crawler.py"]
