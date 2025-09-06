FROM python:3.11-slim

# Install Chrome and dependencies (without deprecated apt-key)
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        wget \
        gnupg \
        ca-certificates \
        unzip \
        curl; \
    mkdir -p /usr/share/keyrings; \
    curl -fsSL https://dl-ssl.google.com/linux/linux_signing_key.pub \
      | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg; \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
      > /etc/apt/sources.list.d/google-chrome.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends google-chrome-stable; \
    rm -rf /var/lib/apt/lists/*

# ChromeDriver is managed by Selenium Manager in Selenium 4.x; no manual install

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy crawler code
COPY crawler.py .
COPY config.json .

## Cron helpers removed. This image runs the crawler on demand only.

# Create downloads directory
RUN mkdir -p downloads

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the crawler once per container invocation
ENTRYPOINT ["python", "crawler.py"]
