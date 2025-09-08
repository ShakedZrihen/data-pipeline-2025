FROM python:3.13-slim

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium chromium-driver \
    fonts-liberation fonts-noto-color-emoji \
    libnss3 libxss1 libasound2 ca-certificates curl gnupg \
  && rm -rf /var/lib/apt/lists/*


ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER=/usr/bin/chromedriver

RUN pip install -r requirements.txt

CMD ["python3", "RunAllCrawlers.py"]
