FROM python:3.11-slim
WORKDIR /app

# (אופציונלי עבור Selenium) התקנת Chromium/Chromedriver
RUN apt-get update && apt-get install -y --no-install-recommends \
      chromium \
      chromium-driver \
      fonts-liberation \
      libnss3 \
      libatk-bridge2.0-0 \
      libgtk-3-0 \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# דרישות של הקרולר
COPY ./crawler/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# קוד הקרולר + ה-runner
RUN mkdir -p /app/src
COPY ./crawler/run_crawler.py ./src/run_crawler.py
COPY ./crawler/runner.py ./runner.py

# קוד משותף (S3/MQ/config/emitter)
COPY ./shared /app/shared

# סביבת ברירת מחדל ל-MinIO (אפשר להחליף ל-AWS דרך env)
ENV AWS_ACCESS_KEY_ID=minioadmin
ENV AWS_SECRET_ACCESS_KEY=minioadmin
ENV AWS_DEFAULT_REGION=us-east-1
ENV AWS_ENDPOINT_URL=http://minio:9000
ENV AWS_ENDPOINT_URL_S3=http://minio:9000

CMD ["python", "runner.py"]
