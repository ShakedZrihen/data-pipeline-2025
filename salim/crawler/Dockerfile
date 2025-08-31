FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    ca-certificates \
    curl \
    unzip \
    gnupg \
    libglib2.0-0 \
    libnss3 \
    libgdk-pixbuf-2.0-0 \
    libgtk-3-0 \
    libatk-bridge2.0-0 \
    libasound2 \
    libdrm2 \
    libgbm1 \
    libxkbcommon0 \
    libxdamage1 \
    libxcomposite1 \
    libxrandr2 \
    libxshmfence1 \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir \
    selenium==4.23.1 \
    boto3 \
    webdriver-manager
CMD ["sh", "-c", "python3 goodpharm.py && python3 citymarket.py && python3 yohananof.py"]

# CMD ["python3","goodpharm.py","&&","python3","citymarket.py","&&","python3","yohananof.py"]
