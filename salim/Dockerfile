# using python 3.11
FROM python:3.11-slim

# Install Chromium and dependencies
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    fonts-liberation \
    wget \
    unzip \
    gnupg \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Environment variables (optional, just to be explicit)
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_BIN=/usr/bin/chromedriver

# Set working directory inside the container
WORKDIR /salim

# Copy only salim directory
COPY . /salim

# Install Python requirements
RUN pip install --no-cache-dir -r requirements.txt

# API dependencies
RUN pip install --no-cache-dir fastapi uvicorn pydantic

# Default to a shell; crawler command will be set per service in docker-compose
CMD ["bash"]