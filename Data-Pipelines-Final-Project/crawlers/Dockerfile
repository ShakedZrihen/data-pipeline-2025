FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium chromium-driver \
    ca-certificates curl unzip xdg-utils fonts-liberation \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 libcups2 libdbus-1-3 \
    libdrm2 libgbm1 libgtk-3-0 libnss3 libxcomposite1 libxdamage1 \
    libxfixes3 libxkbcommon0 libxrandr2 libxshmfence1 libx11-xcb1 \
  && rm -rf /var/lib/apt/lists/*

ENV CHROME_BINARY=/usr/bin/chromium
ENV CHROMEDRIVER=/usr/bin/chromedriver

WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
